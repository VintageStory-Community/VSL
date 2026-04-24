using System.Buffers.Binary;
using System.IO.Compression;
using System.Text;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using ProtoBuf;
using VSL.Application;
using VSL.Domain;
using ZstdSharp;

namespace VSL.Infrastructure.Services;

public sealed class MapPreviewService : IMapPreviewService
{
    private const int ChunkCoordinateBits = 22;
    private const int ChunkCoordinateMask = (1 << ChunkCoordinateBits) - 1;
    private const int PositionZShift = 27;
    private const int PositionYShift = 54;
    private const int ChunkYMask = 511;
    private const int DimensionLowShift = 22;
    private const int DimensionLowMask = 31;
    private const int DimensionHighShift = 44;
    private const int DimensionHighMask = 992;
    private const int ChunkSize = 32;
    private const int ChunkVoxelCount = ChunkSize * ChunkSize * ChunkSize;
    private const int ChunkRowSize = ChunkSize * ChunkSize;
    private const ushort InvalidHeightThreshold = ushort.MaxValue - 4;
    private const int MaxPreviewSideLength = 2048;
    private const int DecodeSliceCacheCapacity = 512;

    private static readonly Dictionary<int, string> DefaultMapColorCodes = new()
    {
        [1] = "land",
        [3] = "desert",
        [7] = "land",
        [2] = "desert",
        [6] = "land",
        [5] = "forest",
        [13] = "plant",
        [4] = "forest",
        [9] = "glacier",
        [8] = "lake",
        [10] = "glacier",
        [17] = "lava"
    };

    private static readonly (string Code, int SepiaRgb, int ColorRgb)[] PaletteDefinitions =
    {
        ("ink", 0x483018, 0x3D2F23),
        ("settlement", 0x856844, 0x9D7B5A),
        ("wateredge", 0x483018, 0x5A86AF),
        ("land", 0xAC8858, 0x7FA262),
        ("desert", 0xC4A468, 0xD6BE84),
        ("forest", 0x98844C, 0x4F7F43),
        ("road", 0x805030, 0x8A7350),
        ("plant", 0x808650, 0x69A358),
        ("lake", 0xCCC890, 0x5B9CCF),
        ("lava", 0xCCC890, 0xD66C33),
        ("ocean", 0xCCC890, 0x3F73A3),
        ("glacier", 0xE0E0C0, 0xDEEAF4),
        ("devastation", 0x755C3C, 0x6E6458)
    };

    private static readonly Dictionary<string, byte> PaletteIndexByCode = PaletteDefinitions
        .Select((entry, index) => (entry, index))
        .ToDictionary(
            static x => x.entry.Code,
            static x => (byte)x.index,
            StringComparer.OrdinalIgnoreCase);

    private static readonly int[] SepiaPaletteByIndex = PaletteDefinitions.Select(static entry => entry.SepiaRgb).ToArray();
    private static readonly int[] ColorPaletteByIndex = PaletteDefinitions.Select(static entry => entry.ColorRgb).ToArray();
    private static readonly byte LandPaletteIndex = PaletteIndexByCode["land"];
    private static readonly int WaterEdgeSepiaRgb = SepiaPaletteByIndex[PaletteIndexByCode["wateredge"]];
    private static readonly int WaterEdgeColorRgb = ColorPaletteByIndex[PaletteIndexByCode["wateredge"]];

    private static readonly object RuleCacheGate = new();
    private static readonly Dictionary<string, List<BlockTypeRule>> RuleCache = new(StringComparer.OrdinalIgnoreCase);

    public async Task<OperationResult<MapPreviewData>> LoadMapPreviewAsync(
        ServerProfile profile,
        string? saveFilePath = null,
        CancellationToken cancellationToken = default)
    {
        try
        {
            var resolvedSavePath = ResolveSavePath(profile, saveFilePath);
            if (string.IsNullOrWhiteSpace(resolvedSavePath))
            {
                return OperationResult<MapPreviewData>.Failed("未提供可用的存档路径。");
            }

            if (!File.Exists(resolvedSavePath))
            {
                return OperationResult<MapPreviewData>.Failed($"存档文件不存在：{resolvedSavePath}");
            }

            var decoder = new MapChunkDecoder();
            var planResult = await BuildPreviewPlanAsync(resolvedSavePath, decoder, cancellationToken);
            if (!planResult.IsSuccess || planResult.Value is null)
            {
                return OperationResult<MapPreviewData>.Failed(planResult.Message ?? "无法构建地图预览。", planResult.Exception);
            }

            var plan = planResult.Value;
            var saveMeta = await LoadSaveMetaAsync(resolvedSavePath, cancellationToken);
            var renderResult = await RenderPreviewAsync(resolvedSavePath, profile, decoder, plan, saveMeta, cancellationToken);
            if (!renderResult.IsSuccess || renderResult.Value is null)
            {
                return OperationResult<MapPreviewData>.Failed(renderResult.Message ?? "地图渲染失败。", renderResult.Exception);
            }

            var rendered = renderResult.Value;
            return OperationResult<MapPreviewData>.Success(new MapPreviewData
            {
                SaveFilePath = resolvedSavePath,
                Width = plan.OutputWidth,
                Height = plan.OutputHeight,
                ColorPixelsBgra32 = rendered.ColorPixels,
                GrayscalePixelsBgra32 = rendered.SepiaPixels,
                MapSizeX = saveMeta.MapSizeX,
                MapSizeZ = saveMeta.MapSizeZ,
                ChunkSize = plan.ChunkSize,
                ChunkCount = plan.ChunkCount,
                Dimension = plan.TargetDimension,
                MinChunkX = plan.MinChunkX,
                MaxChunkX = plan.MaxChunkX,
                MinChunkZ = plan.MinChunkZ,
                MaxChunkZ = plan.MaxChunkZ,
                SamplingStep = plan.SamplingStep,
                MinTerrainHeight = plan.MinTerrainHeight,
                MaxTerrainHeight = plan.MaxTerrainHeight
            }, "地图预览已加载（chunk/mapchunk 同步渲染）。");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return OperationResult<MapPreviewData>.Failed("读取地图预览失败。", ex);
        }
    }

    private static async Task<OperationResult<PreviewPlan>> BuildPreviewPlanAsync(
        string savePath,
        MapChunkDecoder decoder,
        CancellationToken cancellationToken)
    {
        try
        {
            var accumulators = new Dictionary<(int Dimension, int ChunkSize), PlanAccumulator>();

            await using var connection = CreateReadOnlyConnection(savePath);
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT position, data FROM mapchunk";

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (reader.IsDBNull(1))
                {
                    continue;
                }

                var blob = reader.GetFieldValue<byte[]>(1);
                if (!decoder.TryDecodeRainHeightMap(blob, out var rainHeightMap, out var side)
                    || rainHeightMap is null
                    || side <= 0)
                {
                    continue;
                }

                var packedPosition = unchecked((ulong)reader.GetInt64(0));
                var chunkPosition = DecodeMapChunkPosition(packedPosition);
                var key = (chunkPosition.Dimension, side);
                if (!accumulators.TryGetValue(key, out var accumulator))
                {
                    accumulator = new PlanAccumulator(side, chunkPosition.Dimension);
                    accumulators[key] = accumulator;
                }

                ushort localMinHeight = ushort.MaxValue;
                ushort localMaxHeight = ushort.MinValue;
                var usableCount = 0;

                for (var i = 0; i < rainHeightMap.Length; i++)
                {
                    var value = rainHeightMap[i];
                    if (!IsUsableHeight(value))
                    {
                        continue;
                    }

                    usableCount++;
                    if (value < localMinHeight)
                    {
                        localMinHeight = value;
                    }

                    if (value > localMaxHeight)
                    {
                        localMaxHeight = value;
                    }
                }

                if (usableCount <= 0)
                {
                    continue;
                }

                accumulator.IncludeChunk(
                    chunkPosition.X,
                    chunkPosition.Z,
                    localMinHeight,
                    localMaxHeight,
                    usableCount);
            }

            var selected = SelectBestAccumulator(accumulators.Values);
            if (selected is null || selected.ChunkCount == 0)
            {
                return OperationResult<PreviewPlan>.Failed("存档中没有可用地图区块（mapchunk 为空或不可解析）。");
            }

            var sourceWidth = checked((selected.MaxChunkX - selected.MinChunkX + 1) * selected.ChunkSize);
            var sourceHeight = checked((selected.MaxChunkZ - selected.MinChunkZ + 1) * selected.ChunkSize);
            var samplingStep = Math.Max(
                1,
                (int)Math.Ceiling(Math.Max(sourceWidth, sourceHeight) / (double)MaxPreviewSideLength));
            var outputWidth = Math.Max(1, (sourceWidth + samplingStep - 1) / samplingStep);
            var outputHeight = Math.Max(1, (sourceHeight + samplingStep - 1) / samplingStep);

            return OperationResult<PreviewPlan>.Success(new PreviewPlan
            {
                ChunkCount = selected.ChunkCount,
                ChunkSize = selected.ChunkSize,
                TargetDimension = selected.Dimension,
                MinChunkX = selected.MinChunkX,
                MaxChunkX = selected.MaxChunkX,
                MinChunkZ = selected.MinChunkZ,
                MaxChunkZ = selected.MaxChunkZ,
                MinTerrainHeight = selected.MinTerrainHeight,
                MaxTerrainHeight = selected.MaxTerrainHeight,
                SamplingStep = samplingStep,
                OutputWidth = outputWidth,
                OutputHeight = outputHeight
            });
        }
        catch (Exception ex)
        {
            return OperationResult<PreviewPlan>.Failed("扫描地图区块失败。", ex);
        }
    }

        private static async Task<OperationResult<RenderedPreview>> RenderPreviewAsync(
        string savePath,
        ServerProfile profile,
        MapChunkDecoder decoder,
        PreviewPlan plan,
        SaveMeta saveMeta,
        CancellationToken cancellationToken)
    {
        try
        {
            var rainMaps = await LoadRainMapsAsync(savePath, decoder, plan, cancellationToken);
            var sliceStore = await ChunkSliceStore.LoadAsync(savePath, plan, cancellationToken);
            var chunkSlicesY = ResolveChunkSlicesY(saveMeta.MapSizeY, sliceStore.MaxChunkY + 1);
            var blockColors = BuildBlockColorTable(profile, saveMeta.BlockCodeById);

            var pixelCount = checked(plan.OutputWidth * plan.OutputHeight);
            var sepiaRgbPixels = new int[pixelCount];
            var colorRgbPixels = new int[pixelCount];
            var hasSepiaPixel = new bool[pixelCount];
            var hasColorPixel = new bool[pixelCount];

            for (var chunkZ = plan.MinChunkZ; chunkZ <= plan.MaxChunkZ; chunkZ++)
            {
                cancellationToken.ThrowIfCancellationRequested();

                for (var chunkX = plan.MinChunkX; chunkX <= plan.MaxChunkX; chunkX++)
                {
                    if (!rainMaps.TryGetValue((chunkX, chunkZ), out var rainMap)
                        || rainMap is null
                        || rainMap.Length < ChunkRowSize)
                    {
                        continue;
                    }

                    rainMaps.TryGetValue((chunkX - 1, chunkZ - 1), out var rainNorthWest);
                    rainMaps.TryGetValue((chunkX - 1, chunkZ), out var rainWest);
                    rainMaps.TryGetValue((chunkX, chunkZ - 1), out var rainNorth);

                    var chunkSepiaRgb = new int[ChunkRowSize];
                    var chunkColorRgb = new int[ChunkRowSize];
                    var chunkHasSepia = new bool[ChunkRowSize];
                    var chunkHasColor = new bool[ChunkRowSize];
                    var chunkSepiaShadow = new byte[ChunkRowSize];
                    var chunkColorShadow = new byte[ChunkRowSize];
                    Array.Fill(chunkSepiaShadow, (byte)128);
                    Array.Fill(chunkColorShadow, (byte)128);

                    for (var localZ = 0; localZ < ChunkSize; localZ++)
                    {
                        for (var localX = 0; localX < ChunkSize; localX++)
                        {
                            var localIndex = localZ * ChunkSize + localX;
                            var y = rainMap[localIndex];
                            var cy = y / ChunkSize;
                            if ((uint)cy >= (uint)chunkSlicesY)
                            {
                                continue;
                            }

                            if (!sliceStore.TryGetSlice(plan.TargetDimension, chunkX, cy, chunkZ, out var currentSlice)
                                || currentSlice is null)
                            {
                                continue;
                            }

                            var brightness = ComputeSlopeBrightness(
                                y,
                                localX,
                                localZ,
                                rainMap,
                                rainNorthWest,
                                rainWest,
                                rainNorth);

                            var yInChunk = PositiveMod(y, ChunkSize);
                            var index3d = ToIndex3d(localX, yInChunk, localZ);
                            var blockId = currentSlice.GetLayer3(index3d);
                            var baseBlock = blockColors.Get(blockId);

                            var sepiaBlock = baseBlock;
                            var sepiaCy = cy;
                            var sepiaYInChunk = yInChunk;
                            var sepiaSlice = currentSlice;
                            var sepiaY = y;

                            if (sepiaBlock.IsSnow && sepiaY > 0)
                            {
                                sepiaY--;
                                sepiaCy = sepiaY / ChunkSize;
                                if ((uint)sepiaCy >= (uint)chunkSlicesY
                                    || !sliceStore.TryGetSlice(plan.TargetDimension, chunkX, sepiaCy, chunkZ, out sepiaSlice)
                                    || sepiaSlice is null)
                                {
                                    sepiaSlice = null;
                                }
                                else
                                {
                                    sepiaYInChunk = PositiveMod(sepiaY, ChunkSize);
                                    index3d = ToIndex3d(localX, sepiaYInChunk, localZ);
                                    blockId = sepiaSlice.GetLayer3(index3d);
                                    sepiaBlock = blockColors.Get(blockId);
                                }
                            }

                            if (sepiaSlice is not null)
                            {
                                var sepiaRgb = SepiaPaletteByIndex[sepiaBlock.ColorIndex];
                                if (sepiaBlock.IsLake)
                                {
                                    if (ShouldPaintWaterEdge(
                                            sliceStore,
                                            blockColors,
                                            plan.TargetDimension,
                                            chunkX,
                                            chunkZ,
                                            sepiaCy,
                                            localX,
                                            localZ,
                                            sepiaYInChunk))
                                    {
                                        sepiaRgb = WaterEdgeSepiaRgb;
                                    }
                                }
                                else
                                {
                                    chunkSepiaShadow[localIndex] = MultiplyByte(chunkSepiaShadow[localIndex], brightness);
                                }

                                chunkSepiaRgb[localIndex] = sepiaRgb;
                                chunkHasSepia[localIndex] = true;
                            }

                            // Color map follows colorAccurate=true style: no wateredge branch, always apply shadow.
                            var colorRgb = ColorPaletteByIndex[baseBlock.ColorIndex];
                            chunkColorShadow[localIndex] = MultiplyByte(chunkColorShadow[localIndex], brightness);
                            chunkColorRgb[localIndex] = colorRgb;
                            chunkHasColor[localIndex] = true;
                        }
                    }

                    var chunkSepiaShadowOriginal = (byte[])chunkSepiaShadow.Clone();
                    var chunkColorShadowOriginal = (byte[])chunkColorShadow.Clone();
                    Blur(chunkSepiaShadow, ChunkSize, ChunkSize, radius: 2);
                    Blur(chunkColorShadow, ChunkSize, ChunkSize, radius: 2);

                    for (var i = 0; i < ChunkRowSize; i++)
                    {
                        if (chunkHasSepia[i])
                        {
                            var sepiaFactor = ComputeShadowFactor(chunkSepiaShadow[i], chunkSepiaShadowOriginal[i]);
                            chunkSepiaRgb[i] = MultiplyRgbClamped(chunkSepiaRgb[i], sepiaFactor + 1f);
                        }

                        if (chunkHasColor[i])
                        {
                            var colorFactor = ComputeShadowFactor(chunkColorShadow[i], chunkColorShadowOriginal[i]);
                            chunkColorRgb[i] = MultiplyRgbClamped(chunkColorRgb[i], colorFactor + 1f);
                        }
                    }

                    var chunkOffsetX = (chunkX - plan.MinChunkX) * plan.ChunkSize;
                    var chunkOffsetY = (chunkZ - plan.MinChunkZ) * plan.ChunkSize;

                    for (var localZ = 0; localZ < ChunkSize; localZ++)
                    {
                        var sourceY = chunkOffsetY + localZ;
                        var targetY = sourceY / plan.SamplingStep;
                        if ((uint)targetY >= (uint)plan.OutputHeight)
                        {
                            continue;
                        }

                        for (var localX = 0; localX < ChunkSize; localX++)
                        {
                            var localIndex = localZ * ChunkSize + localX;
                            var sourceX = chunkOffsetX + localX;
                            var targetX = sourceX / plan.SamplingStep;
                            if ((uint)targetX >= (uint)plan.OutputWidth)
                            {
                                continue;
                            }

                            var pixelIndex = targetY * plan.OutputWidth + targetX;
                            if (chunkHasSepia[localIndex]
                                && (plan.SamplingStep == 1 || !hasSepiaPixel[pixelIndex]))
                            {
                                sepiaRgbPixels[pixelIndex] = chunkSepiaRgb[localIndex];
                                hasSepiaPixel[pixelIndex] = true;
                            }

                            if (chunkHasColor[localIndex]
                                && (plan.SamplingStep == 1 || !hasColorPixel[pixelIndex]))
                            {
                                colorRgbPixels[pixelIndex] = chunkColorRgb[localIndex];
                                hasColorPixel[pixelIndex] = true;
                            }
                        }
                    }
                }
            }

            var sepiaPixels = ToBgra32(sepiaRgbPixels, hasSepiaPixel);
            var colorPixels = ToBgra32(colorRgbPixels, hasColorPixel);

            return OperationResult<RenderedPreview>.Success(new RenderedPreview(colorPixels, sepiaPixels));
        }
        catch (Exception ex)
        {
            return OperationResult<RenderedPreview>.Failed("绘制地图失败。", ex);
        }
    }

    private static int ResolveChunkSlicesY(int mapSizeY, int inferredChunkSlices)
    {
        if (mapSizeY > 0)
        {
            return Math.Max(1, (mapSizeY + ChunkSize - 1) / ChunkSize);
        }

        return Math.Max(1, inferredChunkSlices);
    }

    private static bool ShouldPaintWaterEdge(
        ChunkSliceStore store,
        BlockColorTable blockColors,
        int dimension,
        int chunkX,
        int chunkZ,
        int cy,
        int localX,
        int localZ,
        int yInChunk)
    {
        ChunkSlice? left = null;
        ChunkSlice? right = null;
        ChunkSlice? top = null;
        ChunkSlice? bottom = null;

        var leftX = localX - 1;
        var rightX = localX + 1;
        var topZ = localZ - 1;
        var bottomZ = localZ + 1;

        if (leftX < 0)
        {
            store.TryGetSlice(dimension, chunkX - 1, cy, chunkZ, out left);
        }
        else
        {
            store.TryGetSlice(dimension, chunkX, cy, chunkZ, out left);
        }

        if (rightX >= ChunkSize)
        {
            store.TryGetSlice(dimension, chunkX + 1, cy, chunkZ, out right);
        }
        else
        {
            store.TryGetSlice(dimension, chunkX, cy, chunkZ, out right);
        }

        if (topZ < 0)
        {
            store.TryGetSlice(dimension, chunkX, cy, chunkZ - 1, out top);
        }
        else
        {
            store.TryGetSlice(dimension, chunkX, cy, chunkZ, out top);
        }

        if (bottomZ >= ChunkSize)
        {
            store.TryGetSlice(dimension, chunkX, cy, chunkZ + 1, out bottom);
        }
        else
        {
            store.TryGetSlice(dimension, chunkX, cy, chunkZ, out bottom);
        }

        if (left is null || right is null || top is null || bottom is null)
        {
            return false;
        }

        leftX = PositiveMod(leftX, ChunkSize);
        rightX = PositiveMod(rightX, ChunkSize);
        topZ = PositiveMod(topZ, ChunkSize);
        bottomZ = PositiveMod(bottomZ, ChunkSize);

        var leftBlock = blockColors.Get(left.GetLayer3(ToIndex3d(leftX, yInChunk, localZ)));
        var rightBlock = blockColors.Get(right.GetLayer3(ToIndex3d(rightX, yInChunk, localZ)));
        var topBlock = blockColors.Get(top.GetLayer3(ToIndex3d(localX, yInChunk, topZ)));
        var bottomBlock = blockColors.Get(bottom.GetLayer3(ToIndex3d(localX, yInChunk, bottomZ)));

        return !(leftBlock.IsLake && rightBlock.IsLake && topBlock.IsLake && bottomBlock.IsLake);
    }

    private static float ComputeSlopeBrightness(
        int height,
        int localX,
        int localZ,
        ushort[] rain,
        ushort[]? rainNorthWest,
        ushort[]? rainWest,
        ushort[]? rainNorth)
    {
        var rainLeftTop = rain;
        var rainRightTop = rain;
        var rainLeftBottom = rain;

        var leftX = localX - 1;
        var bottomX = localX;
        var topZ = localZ - 1;
        var rightZ = localZ;

        if (leftX < 0 && topZ < 0)
        {
            rainLeftTop = rainNorthWest;
            rainRightTop = rainWest;
            rainLeftBottom = rainNorth;
        }
        else
        {
            if (leftX < 0)
            {
                rainLeftTop = rainWest;
                rainRightTop = rainWest;
            }

            if (topZ < 0)
            {
                rainLeftTop = rainNorth;
                rainLeftBottom = rainNorth;
            }
        }

        leftX = PositiveMod(leftX, ChunkSize);
        topZ = PositiveMod(topZ, ChunkSize);

        var deltaLeftTop = rainLeftTop is null ? 0 : height - rainLeftTop[topZ * ChunkSize + leftX];
        var deltaRightTop = rainRightTop is null ? 0 : height - rainRightTop[rightZ * ChunkSize + leftX];
        var deltaLeftBottom = rainLeftBottom is null ? 0 : height - rainLeftBottom[topZ * ChunkSize + bottomX];

        var slopeSign = Math.Sign(deltaLeftTop) + Math.Sign(deltaRightTop) + Math.Sign(deltaLeftBottom);
        var steepness = Math.Max(
            Math.Max(Math.Abs(deltaLeftTop), Math.Abs(deltaRightTop)),
            Math.Abs(deltaLeftBottom));

        var brightness = 1f;
        if (slopeSign > 0f)
        {
            brightness = 1.08f + Math.Min(0.5f, steepness / 10f) / 1.25f;
        }
        else if (slopeSign < 0f)
        {
            brightness = 0.92f - Math.Min(0.5f, steepness / 10f) / 1.25f;
        }

        return brightness;
    }

    private static float ComputeShadowFactor(byte blurred, byte original)
    {
        var baseFactor = (int)(((blurred / 128f - 1f) * 5f)) / 5f;
        var detailFactor = (((original / 128f - 1f) * 5f) % 1f) / 5f;
        return baseFactor + detailFactor;
    }

    private static byte MultiplyByte(byte value, float factor)
    {
        return (byte)Math.Clamp((int)Math.Round(value * factor), 0, 255);
    }

    private static int MultiplyRgbClamped(int rgb, float factor)
    {
        var r = ClampByte((int)Math.Round(((rgb >> 16) & 0xFF) * factor));
        var g = ClampByte((int)Math.Round(((rgb >> 8) & 0xFF) * factor));
        var b = ClampByte((int)Math.Round((rgb & 0xFF) * factor));
        return (r << 16) | (g << 8) | b;
    }

    private static byte[] ToBgra32(int[] rgb, bool[] hasPixel)
    {
        var output = new byte[rgb.Length * 4];
        for (var i = 0; i < rgb.Length; i++)
        {
            if (!hasPixel[i])
            {
                continue;
            }

            var c = rgb[i];
            var offset = i * 4;
            output[offset] = (byte)(c & 0xFF);
            output[offset + 1] = (byte)((c >> 8) & 0xFF);
            output[offset + 2] = (byte)((c >> 16) & 0xFF);
            output[offset + 3] = 255;
        }

        return output;
    }

    private static void Blur(byte[] data, int width, int height, int radius)
    {
        if (radius <= 0 || data.Length == 0 || width <= 0 || height <= 0)
        {
            return;
        }

        var window = radius * 2 + 1;
        var temp = new byte[data.Length];

        for (var y = 0; y < height; y++)
        {
            var rowOffset = y * width;
            var sum = 0;
            for (var x = -radius; x <= radius; x++)
            {
                var sx = Math.Clamp(x, 0, width - 1);
                sum += data[rowOffset + sx];
            }

            for (var x = 0; x < width; x++)
            {
                temp[rowOffset + x] = (byte)(sum / window);
                var removeX = Math.Clamp(x - radius, 0, width - 1);
                var addX = Math.Clamp(x + radius + 1, 0, width - 1);
                sum += data[rowOffset + addX] - data[rowOffset + removeX];
            }
        }

        for (var x = 0; x < width; x++)
        {
            var sum = 0;
            for (var y = -radius; y <= radius; y++)
            {
                var sy = Math.Clamp(y, 0, height - 1);
                sum += temp[sy * width + x];
            }

            for (var y = 0; y < height; y++)
            {
                data[y * width + x] = (byte)(sum / window);
                var removeY = Math.Clamp(y - radius, 0, height - 1);
                var addY = Math.Clamp(y + radius + 1, 0, height - 1);
                sum += temp[addY * width + x] - temp[removeY * width + x];
            }
        }
    }

    private static BlockColorTable BuildBlockColorTable(ServerProfile profile, Dictionary<int, string> blockCodeById)
    {
        var maxId = Math.Max(0, blockCodeById.Count == 0 ? 0 : blockCodeById.Keys.Max());
        var colorIndexById = new byte[maxId + 1];
        var materialById = new int[maxId + 1];
        var isLakeById = new bool[maxId + 1];
        var isSnowById = new bool[maxId + 1];

        var resolver = BuildBlockMetadataResolver(profile);

        for (var blockId = 0; blockId <= maxId; blockId++)
        {
            var hasCode = blockCodeById.TryGetValue(blockId, out var rawCode);
            var normalizedCode = hasCode ? NormalizeCode(rawCode!) : string.Empty;

            var resolved = hasCode && resolver is not null
                ? resolver.Resolve(normalizedCode)
                : default(ResolvedBlockMetadata);

            var path = ExtractPath(normalizedCode);
            var material = resolved.Material ?? GuessMaterial(path);
            var mapColorCode = ResolveMapColorCode(resolved.MapColorCode, material);
            if (!PaletteIndexByCode.TryGetValue(mapColorCode, out var colorIndex))
            {
                colorIndex = LandPaletteIndex;
            }

            var isGlacierIce = path.Equals("glacierice", StringComparison.OrdinalIgnoreCase);
            var isLake = material == 8 || (material == 10 && !isGlacierIce);
            var isSnow = material == 9;

            colorIndexById[blockId] = colorIndex;
            materialById[blockId] = material;
            isLakeById[blockId] = isLake;
            isSnowById[blockId] = isSnow;
        }

        return new BlockColorTable(colorIndexById, materialById, isLakeById, isSnowById);
    }

    private static string ResolveMapColorCode(string? mapColorCode, int material)
    {
        if (!string.IsNullOrWhiteSpace(mapColorCode))
        {
            return mapColorCode.Trim();
        }

        if (DefaultMapColorCodes.TryGetValue(material, out var fallback))
        {
            return fallback;
        }

        return "land";
    }

    private static int GuessMaterial(string path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return 1;
        }

        var p = path.ToLowerInvariant();
        if (p.Contains("lava", StringComparison.Ordinal)) return 17;
        if (p.Contains("saltwater", StringComparison.Ordinal) || p.Contains("ocean", StringComparison.Ordinal) || p.Contains("water", StringComparison.Ordinal) || p.Contains("lake", StringComparison.Ordinal)) return 8;
        if (p.Contains("snow", StringComparison.Ordinal)) return 9;
        if (p.Contains("ice", StringComparison.Ordinal)) return 10;
        if (p.Contains("leaf", StringComparison.Ordinal) || p.Contains("foliage", StringComparison.Ordinal)) return 5;
        if (p.Contains("sand", StringComparison.Ordinal)) return 3;
        if (p.Contains("gravel", StringComparison.Ordinal)) return 2;
        if (p.Contains("ore", StringComparison.Ordinal)) return 7;
        if (p.Contains("wood", StringComparison.Ordinal) || p.Contains("log", StringComparison.Ordinal) || p.Contains("plank", StringComparison.Ordinal)) return 4;
        if (p.Contains("plant", StringComparison.Ordinal) || p.Contains("grass", StringComparison.Ordinal) || p.Contains("crop", StringComparison.Ordinal)) return 13;
        if (p.Contains("stone", StringComparison.Ordinal) || p.Contains("rock", StringComparison.Ordinal) || p.Contains("cobble", StringComparison.Ordinal) || p.Contains("brick", StringComparison.Ordinal)) return 6;
        if (p.Contains("soil", StringComparison.Ordinal) || p.Contains("dirt", StringComparison.Ordinal) || p.Contains("clay", StringComparison.Ordinal) || p.Contains("mud", StringComparison.Ordinal)) return 1;
        return 1;
    }

    private static BlockMetadataResolver? BuildBlockMetadataResolver(ServerProfile profile)
    {
        try
        {
            var installPath = WorkspaceLayout.GetServerInstallPath(profile.Version);
            var cacheKey = BuildRuleCacheKey(installPath, profile.DataPath);

            lock (RuleCacheGate)
            {
                if (RuleCache.TryGetValue(cacheKey, out var cached))
                {
                    return new BlockMetadataResolver(cached);
                }
            }

            var rules = LoadRules(installPath, profile.DataPath);
            lock (RuleCacheGate)
            {
                RuleCache[cacheKey] = rules;
            }

            return new BlockMetadataResolver(rules);
        }
        catch
        {
            return null;
        }
    }

    private static string BuildRuleCacheKey(string installPath, string profileDataPath)
    {
        var builder = new StringBuilder();
        builder.Append(Path.GetFullPath(installPath));
        builder.Append('|');
        builder.Append(Path.GetFullPath(profileDataPath));
        return builder.ToString();
    }

    private static List<BlockTypeRule> LoadRules(string installPath, string profileDataPath)
    {
        var rules = new List<BlockTypeRule>();
        var order = 0;

        void ParseAssetRoot(string assetRoot)
        {
            if (!Directory.Exists(assetRoot))
            {
                return;
            }

            foreach (var domainDir in Directory.EnumerateDirectories(assetRoot))
            {
                var domain = Path.GetFileName(domainDir).ToLowerInvariant();
                var blockTypesRoot = Path.Combine(domainDir, "blocktypes");
                if (!Directory.Exists(blockTypesRoot))
                {
                    continue;
                }

                foreach (var file in Directory.EnumerateFiles(blockTypesRoot, "*.json", SearchOption.AllDirectories))
                {
                    string text;
                    try
                    {
                        text = File.ReadAllText(file);
                    }
                    catch
                    {
                        continue;
                    }

                    if (TryParseRule(text, domain, order++, out var rule) && rule is not null)
                    {
                        rules.Add(rule);
                    }
                }
            }
        }

        void ParseModsRoot(string modsRoot)
        {
            if (!Directory.Exists(modsRoot))
            {
                return;
            }

            foreach (var modDir in Directory.EnumerateDirectories(modsRoot))
            {
                ParseAssetRoot(Path.Combine(modDir, "assets"));
            }

            foreach (var zipPath in Directory.EnumerateFiles(modsRoot, "*.zip", SearchOption.TopDirectoryOnly))
            {
                try
                {
                    using var archive = ZipFile.OpenRead(zipPath);
                    foreach (var entry in archive.Entries)
                    {
                        if (!entry.FullName.EndsWith(".json", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        var full = entry.FullName.Replace('\\', '/');
                        if (!full.StartsWith("assets/", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        var parts = full.Split('/', StringSplitOptions.RemoveEmptyEntries);
                        if (parts.Length < 4)
                        {
                            continue;
                        }

                        if (!parts[2].Equals("blocktypes", StringComparison.OrdinalIgnoreCase))
                        {
                            continue;
                        }

                        string text;
                        using (var stream = entry.Open())
                        using (var reader = new StreamReader(stream))
                        {
                            text = reader.ReadToEnd();
                        }

                        var domain = parts[1].ToLowerInvariant();
                        if (TryParseRule(text, domain, order++, out var rule) && rule is not null)
                        {
                            rules.Add(rule);
                        }
                    }
                }
                catch
                {
                    // ignore broken archives
                }
            }
        }

        ParseAssetRoot(Path.Combine(installPath, "assets"));
        ParseModsRoot(Path.Combine(installPath, "Mods"));
        ParseModsRoot(Path.Combine(profileDataPath, "Mods"));

        return rules;
    }

    private static bool TryParseRule(string source, string defaultDomain, int order, out BlockTypeRule? rule)
    {
        rule = null;

        var code = MatchStringProperty(source, "code");
        if (string.IsNullOrWhiteSpace(code))
        {
            return false;
        }

        var domain = defaultDomain;
        var pathTemplate = code.Trim();
        var colonIndex = pathTemplate.IndexOf(':');
        if (colonIndex > 0)
        {
            domain = pathTemplate[..colonIndex].Trim().ToLowerInvariant();
            pathTemplate = pathTemplate[(colonIndex + 1)..];
        }

        var codePattern = NormalizePattern(pathTemplate);
        if (string.IsNullOrWhiteSpace(codePattern))
        {
            return false;
        }

        var mapColorCode = MatchStringProperty(source, "mapColorCode");
        Dictionary<string, string>? mapColorByType = null;
        Dictionary<string, string>? materialByType = null;

        if (TryExtractObject(source, "attributes", out var attributesBody))
        {
            var attrColorCode = MatchStringProperty(attributesBody, "mapColorCode");
            if (!string.IsNullOrWhiteSpace(attrColorCode))
            {
                mapColorCode = attrColorCode;
            }

            mapColorByType = ParseStringMap(attributesBody, "mapColorCodeByType");
        }

        if (mapColorByType is null || mapColorByType.Count == 0)
        {
            mapColorByType = ParseStringMap(source, "mapColorCodeByType");
        }

        var materialValue = MatchStringProperty(source, "blockmaterial");
        var material = ParseMaterial(materialValue);
        materialByType = ParseStringMap(source, "blockmaterialByType")
            ?.ToDictionary(static pair => pair.Key, static pair => pair.Value, StringComparer.OrdinalIgnoreCase);

        var literalScore = CountLiteralCharacters(codePattern);
        var normalizedMapColorByType = mapColorByType?
            .Where(static pair => !string.IsNullOrWhiteSpace(pair.Value))
            .Select(static pair => new PatternStringValue(NormalizePattern(pair.Key), pair.Value.Trim()))
            .Where(static pair => !string.IsNullOrWhiteSpace(pair.Pattern))
            .ToArray() ?? Array.Empty<PatternStringValue>();

        var normalizedMaterialByType = materialByType?
            .Select(static pair => new { Pattern = NormalizePattern(pair.Key), Material = ParseMaterial(pair.Value) })
            .Where(static pair => !string.IsNullOrWhiteSpace(pair.Pattern) && pair.Material is not null)
            .Select(static pair => new PatternIntValue(pair.Pattern, pair.Material!.Value))
            .ToArray() ?? Array.Empty<PatternIntValue>();

        rule = new BlockTypeRule(
            domain,
            codePattern,
            literalScore,
            order,
            string.IsNullOrWhiteSpace(mapColorCode) ? null : mapColorCode.Trim(),
            material,
            normalizedMapColorByType,
            normalizedMaterialByType);

        return true;
    }

    private static string? MatchStringProperty(string source, string propertyName)
    {
        var pattern = $@"(?m)^\s*{Regex.Escape(propertyName)}\s*:\s*""([^""]+)""";
        var match = Regex.Match(source, pattern, RegexOptions.CultureInvariant);
        return match.Success ? match.Groups[1].Value : null;
    }

    private static Dictionary<string, string>? ParseStringMap(string source, string propertyName)
    {
        if (!TryExtractObject(source, propertyName, out var body))
        {
            return null;
        }

        var result = new Dictionary<string, string>(StringComparer.OrdinalIgnoreCase);
        var matches = Regex.Matches(
            body,
            "\"([^\"]+)\"\\s*:\\s*(\"([^\"]*)\"|([A-Za-z0-9_\\-]+)|null)",
            RegexOptions.CultureInvariant);

        foreach (Match match in matches)
        {
            if (!match.Success)
            {
                continue;
            }

            var key = match.Groups[1].Value;
            if (string.IsNullOrWhiteSpace(key))
            {
                continue;
            }

            if (match.Value.EndsWith("null", StringComparison.OrdinalIgnoreCase))
            {
                continue;
            }

            var value = match.Groups[3].Success
                ? match.Groups[3].Value
                : match.Groups[4].Success
                    ? match.Groups[4].Value
                    : string.Empty;
            if (string.IsNullOrWhiteSpace(value))
            {
                continue;
            }

            result[key] = value;
        }

        return result;
    }

    private static bool TryExtractObject(string source, string propertyName, out string body)
    {
        body = string.Empty;
        var matcher = new Regex($@"\b{Regex.Escape(propertyName)}\b\s*:\s*\{{", RegexOptions.CultureInvariant);
        var match = matcher.Match(source);
        if (!match.Success)
        {
            return false;
        }

        var openBraceIndex = match.Index + match.Length - 1;
        var closeBraceIndex = FindMatchingBrace(source, openBraceIndex);
        if (closeBraceIndex <= openBraceIndex)
        {
            return false;
        }

        body = source.Substring(openBraceIndex + 1, closeBraceIndex - openBraceIndex - 1);
        return true;
    }

    private static int FindMatchingBrace(string text, int openBraceIndex)
    {
        var depth = 0;
        var inString = false;
        var escape = false;

        for (var i = openBraceIndex; i < text.Length; i++)
        {
            var ch = text[i];
            if (inString)
            {
                if (escape)
                {
                    escape = false;
                    continue;
                }

                if (ch == '\\')
                {
                    escape = true;
                    continue;
                }

                if (ch == '"')
                {
                    inString = false;
                }

                continue;
            }

            if (ch == '"')
            {
                inString = true;
                continue;
            }

            if (ch == '{')
            {
                depth++;
                continue;
            }

            if (ch == '}')
            {
                depth--;
                if (depth == 0)
                {
                    return i;
                }
            }
        }

        return -1;
    }

    private static int CountLiteralCharacters(string pattern)
    {
        var count = 0;
        for (var i = 0; i < pattern.Length; i++)
        {
            if (pattern[i] != '*')
            {
                count++;
            }
        }

        return count;
    }

    private static string NormalizePattern(string pattern)
    {
        if (string.IsNullOrWhiteSpace(pattern))
        {
            return string.Empty;
        }

        var value = pattern.Trim();
        value = Regex.Replace(value, "\\{[^}]+\\}", "*");
        value = value.ToLowerInvariant();
        return value;
    }

    private static int? ParseMaterial(string? materialText)
    {
        if (string.IsNullOrWhiteSpace(materialText))
        {
            return null;
        }

        return materialText.Trim().ToLowerInvariant() switch
        {
            "air" => 0,
            "soil" => 1,
            "gravel" => 2,
            "sand" => 3,
            "wood" => 4,
            "leaves" => 5,
            "stone" => 6,
            "ore" => 7,
            "liquid" => 8,
            "snow" => 9,
            "ice" => 10,
            "metal" => 11,
            "plant" => 13,
            "lava" => 17,
            _ => null
        };
    }

    private static async Task<Dictionary<(int X, int Z), ushort[]>> LoadRainMapsAsync(
        string savePath,
        MapChunkDecoder decoder,
        PreviewPlan plan,
        CancellationToken cancellationToken)
    {
        var result = new Dictionary<(int X, int Z), ushort[]>();

        await using var connection = CreateReadOnlyConnection(savePath);
        await connection.OpenAsync(cancellationToken);
        await using var command = connection.CreateCommand();
        command.CommandText = "SELECT position, data FROM mapchunk";

        await using var reader = await command.ExecuteReaderAsync(cancellationToken);
        while (await reader.ReadAsync(cancellationToken))
        {
            cancellationToken.ThrowIfCancellationRequested();

            if (reader.IsDBNull(1))
            {
                continue;
            }

            var packedPosition = unchecked((ulong)reader.GetInt64(0));
            var pos = DecodeMapChunkPosition(packedPosition);
            if (pos.Dimension != plan.TargetDimension)
            {
                continue;
            }

            if (pos.X < plan.MinChunkX - 1
                || pos.X > plan.MaxChunkX + 1
                || pos.Z < plan.MinChunkZ - 1
                || pos.Z > plan.MaxChunkZ + 1)
            {
                continue;
            }

            var blob = reader.GetFieldValue<byte[]>(1);
            if (!decoder.TryDecodeRainHeightMap(blob, out var rainMap, out var side)
                || rainMap is null
                || side != ChunkSize)
            {
                continue;
            }

            result[(pos.X, pos.Z)] = rainMap;
        }

        return result;
    }

    private static async Task<SaveMeta> LoadSaveMetaAsync(string savePath, CancellationToken cancellationToken)
    {
        var meta = new SaveMeta
        {
            MapSizeX = 0,
            MapSizeY = 0,
            MapSizeZ = 0,
            BlockCodeById = new Dictionary<int, string>()
        };

        try
        {
            await using var connection = CreateReadOnlyConnection(savePath);
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT data FROM gamedata LIMIT 1";
            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            if (!await reader.ReadAsync(cancellationToken) || reader.IsDBNull(0))
            {
                return meta;
            }

            var blob = reader.GetFieldValue<byte[]>(0);
            if (blob.Length == 0)
            {
                return meta;
            }

            using var saveStream = new MemoryStream(blob, writable: false);
            var save = Serializer.Deserialize<StoredSaveGame>(saveStream);
            if (save is null)
            {
                return meta;
            }

            meta.MapSizeX = Math.Max(0, save.MapSizeX);
            meta.MapSizeY = Math.Max(0, save.MapSizeY);
            meta.MapSizeZ = Math.Max(0, save.MapSizeZ);

            if (save.ModData is null
                || !save.ModData.TryGetValue("BlockIDs", out var blockIdsBlob)
                || blockIdsBlob is null
                || blockIdsBlob.Length == 0)
            {
                return meta;
            }

            using var blockMapStream = new MemoryStream(blockIdsBlob, writable: false);
            var blockMap = Serializer.Deserialize<Dictionary<int, string>>(blockMapStream);
            if (blockMap is not null)
            {
                meta.BlockCodeById = blockMap;
            }
        }
        catch
        {
            // keep best effort metadata
        }

        return meta;
    }

    private static string NormalizeCode(string code)
    {
        return (code ?? string.Empty).Trim().ToLowerInvariant();
    }

    private static string ExtractPath(string normalizedCode)
    {
        if (string.IsNullOrWhiteSpace(normalizedCode))
        {
            return string.Empty;
        }

        var index = normalizedCode.IndexOf(':');
        return index >= 0 ? normalizedCode[(index + 1)..] : normalizedCode;
    }

    private static int ClampByte(int value)
    {
        return value switch
        {
            < 0 => 0,
            > 255 => 255,
            _ => value
        };
    }

    private static int ToIndex3d(int x, int y, int z)
    {
        return (y * ChunkSize + z) * ChunkSize + x;
    }

    private static bool IsUsableHeight(ushort value)
    {
        return value < InvalidHeightThreshold;
    }

    private static int PositiveMod(int value, int modulus)
    {
        var rem = value % modulus;
        return rem < 0 ? rem + modulus : rem;
    }

    private static PlanAccumulator? SelectBestAccumulator(IEnumerable<PlanAccumulator> accumulators)
    {
        PlanAccumulator? selected = null;
        foreach (var candidate in accumulators)
        {
            if (candidate.ChunkCount <= 0)
            {
                continue;
            }

            if (selected is null)
            {
                selected = candidate;
                continue;
            }

            var candidatePrimary = candidate.Dimension == 0;
            var selectedPrimary = selected.Dimension == 0;
            if (candidatePrimary != selectedPrimary)
            {
                if (candidatePrimary)
                {
                    selected = candidate;
                }

                continue;
            }

            if (candidate.ChunkCount > selected.ChunkCount)
            {
                selected = candidate;
                continue;
            }

            if (candidate.ChunkCount == selected.ChunkCount && candidate.UsableHeightSamples > selected.UsableHeightSamples)
            {
                selected = candidate;
            }
        }

        return selected;
    }

    private static DecodedMapChunkPosition DecodeMapChunkPosition(ulong packedPosition)
    {
        var x = (int)(packedPosition & ChunkCoordinateMask);
        var z = (int)((packedPosition >> PositionZShift) & ChunkCoordinateMask);
        var dimension =
            (int)(((packedPosition >> DimensionLowShift) & DimensionLowMask)
                  + ((packedPosition >> DimensionHighShift) & DimensionHighMask));

        return new DecodedMapChunkPosition(x, z, dimension);
    }

    private static DecodedChunkPosition DecodeChunkPosition(ulong packedPosition)
    {
        var x = (int)(packedPosition & ChunkCoordinateMask);
        var y = (int)((packedPosition >> PositionYShift) & ChunkYMask);
        var z = (int)((packedPosition >> PositionZShift) & ChunkCoordinateMask);
        var dimension =
            (int)(((packedPosition >> DimensionLowShift) & DimensionLowMask)
                  + ((packedPosition >> DimensionHighShift) & DimensionHighMask));

        return new DecodedChunkPosition(x, y, z, dimension);
    }

    private static string ResolveSavePath(ServerProfile profile, string? saveFilePath)
    {
        if (!string.IsNullOrWhiteSpace(saveFilePath))
        {
            return Path.GetFullPath(saveFilePath);
        }

        if (!string.IsNullOrWhiteSpace(profile.ActiveSaveFile))
        {
            return Path.GetFullPath(profile.ActiveSaveFile);
        }

        return string.Empty;
    }

    private static SqliteConnection CreateReadOnlyConnection(string savePath)
    {
        var builder = new SqliteConnectionStringBuilder
        {
            DataSource = savePath,
            Mode = SqliteOpenMode.ReadOnly,
            Cache = SqliteCacheMode.Private
        };

        return new SqliteConnection(builder.ToString());
    }

    private sealed class MapChunkDecoder
    {
        public bool TryDecodeRainHeightMap(byte[] mapChunkBlob, out ushort[]? rainHeightMap, out int side)
        {
            rainHeightMap = null;
            side = 0;

            try
            {
                using var stream = new MemoryStream(mapChunkBlob, writable: false);
                var chunk = Serializer.Deserialize<StoredMapChunk>(stream);
                if (chunk?.RainHeightMap is null || chunk.RainHeightMap.Length == 0)
                {
                    return false;
                }

                var resolvedSide = (int)Math.Sqrt(chunk.RainHeightMap.Length);
                if (resolvedSide <= 0 || resolvedSide * resolvedSide != chunk.RainHeightMap.Length)
                {
                    return false;
                }

                rainHeightMap = chunk.RainHeightMap;
                side = resolvedSide;
                return true;
            }
            catch
            {
                return false;
            }
        }

        [ProtoContract]
        private sealed class StoredMapChunk
        {
            [ProtoMember(3)]
            public ushort[]? RainHeightMap { get; set; }
        }
    }

    [ProtoContract]
    private sealed class StoredSaveGame
    {
        [ProtoMember(1)]
        public int MapSizeX { get; set; }

        [ProtoMember(2)]
        public int MapSizeY { get; set; }

        [ProtoMember(3)]
        public int MapSizeZ { get; set; }

        [ProtoMember(11)]
        public Dictionary<string, byte[]>? ModData { get; set; }
    }

    private sealed class ChunkSliceStore
    {
        private readonly Dictionary<ChunkSliceKey, ChunkSliceRow> _rows;
        private readonly Dictionary<ChunkSliceKey, LinkedListNode<CacheEntry>> _cacheNodes = new();
        private readonly LinkedList<CacheEntry> _lru = new();
        private readonly int _cacheCapacity;

        private ChunkSliceStore(Dictionary<ChunkSliceKey, ChunkSliceRow> rows, int maxChunkY, int cacheCapacity)
        {
            _rows = rows;
            MaxChunkY = maxChunkY;
            _cacheCapacity = Math.Max(32, cacheCapacity);
        }

        public int MaxChunkY { get; }

        public static async Task<ChunkSliceStore> LoadAsync(string savePath, PreviewPlan plan, CancellationToken cancellationToken)
        {
            var rows = new Dictionary<ChunkSliceKey, ChunkSliceRow>();
            var maxChunkY = 0;

            await using var connection = CreateReadOnlyConnection(savePath);
            await connection.OpenAsync(cancellationToken);
            await using var command = connection.CreateCommand();
            command.CommandText = "SELECT position, data FROM chunk";

            await using var reader = await command.ExecuteReaderAsync(cancellationToken);
            while (await reader.ReadAsync(cancellationToken))
            {
                cancellationToken.ThrowIfCancellationRequested();

                if (reader.IsDBNull(1))
                {
                    continue;
                }

                var packedPosition = unchecked((ulong)reader.GetInt64(0));
                var pos = DecodeChunkPosition(packedPosition);
                if (pos.Dimension != plan.TargetDimension)
                {
                    continue;
                }

                if (pos.X < plan.MinChunkX - 1
                    || pos.X > plan.MaxChunkX + 1
                    || pos.Z < plan.MinChunkZ - 1
                    || pos.Z > plan.MaxChunkZ + 1)
                {
                    continue;
                }

                var blob = reader.GetFieldValue<byte[]>(1);
                var key = new ChunkSliceKey(pos.Dimension, pos.X, pos.Y, pos.Z);
                rows[key] = new ChunkSliceRow(blob);
                if (pos.Y > maxChunkY)
                {
                    maxChunkY = pos.Y;
                }
            }

            return new ChunkSliceStore(rows, maxChunkY, DecodeSliceCacheCapacity);
        }

        public bool TryGetSlice(int dimension, int chunkX, int chunkY, int chunkZ, out ChunkSlice? slice)
        {
            var key = new ChunkSliceKey(dimension, chunkX, chunkY, chunkZ);
            if (_cacheNodes.TryGetValue(key, out var node))
            {
                _lru.Remove(node);
                _lru.AddFirst(node);
                slice = node.Value.Slice;
                return true;
            }

            if (!_rows.TryGetValue(key, out var row))
            {
                slice = null;
                return false;
            }

            if (!row.TryDecode(out var decoded) || decoded is null)
            {
                slice = null;
                return false;
            }

            var entry = new CacheEntry(key, decoded);
            var newNode = new LinkedListNode<CacheEntry>(entry);
            _lru.AddFirst(newNode);
            _cacheNodes[key] = newNode;

            while (_cacheNodes.Count > _cacheCapacity)
            {
                var last = _lru.Last;
                if (last is null)
                {
                    break;
                }

                _lru.RemoveLast();
                _cacheNodes.Remove(last.Value.Key);
            }

            slice = decoded;
            return true;
        }

        private sealed class ChunkSliceRow
        {
            private byte[] _blob;
            private StoredChunkSlice? _parsed;

            public ChunkSliceRow(byte[] blob)
            {
                _blob = blob;
            }

            public bool TryDecode(out ChunkSlice? slice)
            {
                slice = null;

                try
                {
                    _parsed ??= Deserialize(_blob);
                    _blob = Array.Empty<byte>();

                    if (_parsed is null)
                    {
                        return false;
                    }

                    slice = ChunkCompressionDecoder.Decode(_parsed);
                    return slice is not null;
                }
                catch
                {
                    return false;
                }
            }

            private static StoredChunkSlice? Deserialize(byte[] blob)
            {
                using var stream = new MemoryStream(blob, writable: false);
                return Serializer.Deserialize<StoredChunkSlice>(stream);
            }
        }

        private readonly record struct CacheEntry(ChunkSliceKey Key, ChunkSlice Slice);
    }

    private static class ChunkCompressionDecoder
    {
        private static readonly Decompressor ZstdDecompressor = new();

        public static ChunkSlice? Decode(StoredChunkSlice row)
        {
            var solids = new int[ChunkVoxelCount];
            if (row.BlocksCompressed is null || row.BlocksCompressed.Length == 0)
            {
                return null;
            }

            UnpackBlocksTo(solids, row.BlocksCompressed, row.LightSatCompressed, row.SavedCompressionVersion);

            int[]? fluids = null;
            if (row.FluidsCompressed is { Length: > 0 })
            {
                fluids = new int[ChunkVoxelCount];
                UnpackBlocksTo(fluids, row.FluidsCompressed, row.LightSatCompressed, row.SavedCompressionVersion);
            }

            return new ChunkSlice(solids, fluids);
        }

        private static void UnpackBlocksTo(int[] output, byte[] compressed, byte[]? lightSatCompressed, int chunkDataVersion)
        {
            if (chunkDataVersion == 0)
            {
                UnpackBlocksLegacy(output, compressed, lightSatCompressed);
                return;
            }

            if (compressed.Length < 4)
            {
                Array.Clear(output);
                return;
            }

            var headerLengthRaw = BinaryPrimitives.ReadInt32LittleEndian(compressed.AsSpan(0, 4));
            if (headerLengthRaw == 0)
            {
                Array.Clear(output);
                return;
            }

            int[] palette;
            var headerLength = Math.Abs(headerLengthRaw);

            if (headerLengthRaw < 0)
            {
                if (compressed.Length < 4 + headerLength)
                {
                    Array.Clear(output);
                    return;
                }

                var paletteCount = headerLength / 4;
                if (paletteCount <= 1)
                {
                    Array.Clear(output);
                    return;
                }

                palette = new int[RoundUpPowerOfTwo(paletteCount)];
                ByteToInt(compressed.AsSpan(4, headerLength), palette, paletteCount);
            }
            else
            {
                if (compressed.Length < 4 + headerLength)
                {
                    Array.Clear(output);
                    return;
                }

                var decompressedPalette = DecompressZstd(compressed.AsSpan(4, headerLength));
                if (decompressedPalette.Length < 8)
                {
                    Array.Clear(output);
                    return;
                }

                var paletteCount = decompressedPalette.Length / 4;
                if (paletteCount <= 1)
                {
                    Array.Clear(output);
                    return;
                }

                palette = new int[RoundUpPowerOfTwo(paletteCount)];
                ByteToInt(decompressedPalette, palette, paletteCount);
            }

            var bitPlaneCount = Log2(palette.Length);
            var bitPlaneOffset = 4 + headerLength;
            if (bitPlaneOffset >= compressed.Length)
            {
                Array.Clear(output);
                return;
            }

            var decompressedBitPlanes = DecompressZstd(compressed.AsSpan(bitPlaneOffset));
            if (decompressedBitPlanes.Length < bitPlaneCount * 1024 * 4)
            {
                throw new InvalidDataException("chunk bitplane size mismatch");
            }

            var bitPlanes = new int[bitPlaneCount][];
            for (var plane = 0; plane < bitPlaneCount; plane++)
            {
                var planeArray = new int[1024];
                ByteToInt(
                    decompressedBitPlanes.AsSpan(plane * 1024 * 4, 1024 * 4),
                    planeArray,
                    1024);
                bitPlanes[plane] = planeArray;
            }

            for (var baseIndex = 0; baseIndex < output.Length; baseIndex += 32)
            {
                var row = baseIndex / 32;
                for (var bit = 0; bit < 32; bit++)
                {
                    var paletteIndex = 0;
                    var multiplier = 1;
                    for (var plane = 0; plane < bitPlaneCount; plane++)
                    {
                        paletteIndex += ((bitPlanes[plane][row] >> bit) & 1) * multiplier;
                        multiplier <<= 1;
                    }

                    output[baseIndex + bit] = paletteIndex < palette.Length ? palette[paletteIndex] : 0;
                }
            }
        }

        private static void UnpackBlocksLegacy(int[] output, byte[] blocksCompressed, byte[]? lightSatCompressed)
        {
            var blockBytes = DecompressDeflate(blocksCompressed);
            if (blockBytes.Length < output.Length * 2)
            {
                Array.Clear(output);
                return;
            }

            byte[]? lightSat = null;
            if (lightSatCompressed is { Length: > 0 })
            {
                lightSat = DecompressDeflate(lightSatCompressed);
            }

            for (var i = 0; i < output.Length; i++)
            {
                var block = BinaryPrimitives.ReadUInt16LittleEndian(blockBytes.AsSpan(i * 2, 2));
                var sat = lightSat is { Length: > 0 }
                    ? lightSat[Math.Min(i, lightSat.Length - 1)]
                    : (byte)0;
                output[i] = block | ((sat & 0xF8) << 13);
            }
        }

        private static byte[] DecompressDeflate(byte[] data)
        {
            using var input = new MemoryStream(data);
            using var deflate = new DeflateStream(input, CompressionMode.Decompress);
            using var output = new MemoryStream();
            deflate.CopyTo(output);
            return output.ToArray();
        }

        private static byte[] DecompressZstd(ReadOnlySpan<byte> data)
        {
            return ZstdDecompressor.Unwrap(data).ToArray();
        }

        private static void ByteToInt(ReadOnlySpan<byte> bytes, int[] destination, int count)
        {
            for (var i = 0; i < count; i++)
            {
                destination[i] = BinaryPrimitives.ReadInt32LittleEndian(bytes.Slice(i * 4, 4));
            }
        }

        private static int RoundUpPowerOfTwo(int value)
        {
            if (value <= 1)
            {
                return 1;
            }

            var power = 1;
            while (power < value)
            {
                power <<= 1;
            }

            return power;
        }

        private static int Log2(int value)
        {
            var result = 0;
            var v = value;
            while ((v >>= 1) > 0)
            {
                result++;
            }

            return result;
        }
    }

    [ProtoContract]
    private sealed class StoredChunkSlice
    {
        [ProtoMember(1)]
        public byte[]? BlocksCompressed { get; set; }

        [ProtoMember(3)]
        public byte[]? LightSatCompressed { get; set; }

        [ProtoMember(15)]
        public int SavedCompressionVersion { get; set; }

        [ProtoMember(16)]
        public byte[]? FluidsCompressed { get; set; }
    }

    private sealed class ChunkSlice
    {
        private readonly int[] _solids;
        private readonly int[]? _fluids;

        public ChunkSlice(int[] solids, int[]? fluids)
        {
            _solids = solids;
            _fluids = fluids;
        }

        public int GetLayer3(int index)
        {
            if (_fluids is { Length: > 0 })
            {
                var fluid = _fluids[index];
                if (fluid != 0)
                {
                    return fluid;
                }
            }

            return _solids[index];
        }
    }

    private sealed class BlockColorTable
    {
        private readonly byte[] _colorIndexById;
        private readonly int[] _materialById;
        private readonly bool[] _isLakeById;
        private readonly bool[] _isSnowById;

        public BlockColorTable(byte[] colorIndexById, int[] materialById, bool[] isLakeById, bool[] isSnowById)
        {
            _colorIndexById = colorIndexById;
            _materialById = materialById;
            _isLakeById = isLakeById;
            _isSnowById = isSnowById;
        }

        public BlockRenderInfo Get(int blockId)
        {
            if ((uint)blockId >= (uint)_colorIndexById.Length)
            {
                return new BlockRenderInfo(LandPaletteIndex, 1, false, false);
            }

            return new BlockRenderInfo(
                _colorIndexById[blockId],
                _materialById[blockId],
                _isLakeById[blockId],
                _isSnowById[blockId]);
        }
    }

    private sealed class BlockMetadataResolver
    {
        private readonly List<BlockTypeRule> _rules;

        public BlockMetadataResolver(List<BlockTypeRule> rules)
        {
            _rules = rules;
        }

        public ResolvedBlockMetadata Resolve(string normalizedCode)
        {
            if (string.IsNullOrWhiteSpace(normalizedCode))
            {
                return default;
            }

            var domain = "game";
            var path = normalizedCode;
            var colonIndex = normalizedCode.IndexOf(':');
            if (colonIndex > 0)
            {
                domain = normalizedCode[..colonIndex];
                path = normalizedCode[(colonIndex + 1)..];
            }

            BlockTypeRule? best = null;
            foreach (var rule in _rules)
            {
                if (!rule.Domain.Equals(domain, StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                if (!WildcardMatch(rule.CodePattern, path))
                {
                    continue;
                }

                if (best is null
                    || rule.LiteralScore > best.LiteralScore
                    || (rule.LiteralScore == best.LiteralScore && rule.Order > best.Order))
                {
                    best = rule;
                }
            }

            if (best is null && !domain.Equals("game", StringComparison.OrdinalIgnoreCase))
            {
                foreach (var rule in _rules)
                {
                    if (!rule.Domain.Equals("game", StringComparison.OrdinalIgnoreCase))
                    {
                        continue;
                    }

                    if (!WildcardMatch(rule.CodePattern, path))
                    {
                        continue;
                    }

                    if (best is null
                        || rule.LiteralScore > best.LiteralScore
                        || (rule.LiteralScore == best.LiteralScore && rule.Order > best.Order))
                    {
                        best = rule;
                    }
                }
            }

            if (best is null)
            {
                return default;
            }

            var mapColorCode = best.MapColorCode;
            foreach (var pattern in best.MapColorCodeByType)
            {
                if (!string.IsNullOrWhiteSpace(pattern.Pattern) && WildcardMatch(pattern.Pattern, path))
                {
                    mapColorCode = pattern.Value;
                    break;
                }
            }

            var material = best.Material;
            foreach (var pattern in best.MaterialByType)
            {
                if (!string.IsNullOrWhiteSpace(pattern.Pattern) && WildcardMatch(pattern.Pattern, path))
                {
                    material = pattern.Value;
                    break;
                }
            }

            return new ResolvedBlockMetadata(mapColorCode, material);
        }

        private static bool WildcardMatch(string pattern, string value)
        {
            var p = 0;
            var v = 0;
            var star = -1;
            var match = 0;

            while (v < value.Length)
            {
                if (p < pattern.Length && (pattern[p] == value[v]))
                {
                    p++;
                    v++;
                    continue;
                }

                if (p < pattern.Length && pattern[p] == '*')
                {
                    star = p++;
                    match = v;
                    continue;
                }

                if (star != -1)
                {
                    p = star + 1;
                    v = ++match;
                    continue;
                }

                return false;
            }

            while (p < pattern.Length && pattern[p] == '*')
            {
                p++;
            }

            return p == pattern.Length;
        }
    }

    private readonly record struct PatternStringValue(string Pattern, string Value);

    private readonly record struct PatternIntValue(string Pattern, int Value);

    private sealed record BlockTypeRule(
        string Domain,
        string CodePattern,
        int LiteralScore,
        int Order,
        string? MapColorCode,
        int? Material,
        PatternStringValue[] MapColorCodeByType,
        PatternIntValue[] MaterialByType);

    private readonly record struct ResolvedBlockMetadata(string? MapColorCode, int? Material);

    private readonly record struct BlockRenderInfo(byte ColorIndex, int Material, bool IsLake, bool IsSnow);

    private sealed class SaveMeta
    {
        public required int MapSizeX { get; set; }

        public required int MapSizeY { get; set; }

        public required int MapSizeZ { get; set; }

        public required Dictionary<int, string> BlockCodeById { get; set; }
    }

    private sealed record RenderedPreview(byte[] ColorPixels, byte[] SepiaPixels);

    private sealed record DecodedMapChunkPosition(int X, int Z, int Dimension);

    private sealed record DecodedChunkPosition(int X, int Y, int Z, int Dimension);

    private readonly record struct ChunkSliceKey(int Dimension, int X, int Y, int Z);

    private sealed class PlanAccumulator
    {
        public PlanAccumulator(int chunkSize, int dimension)
        {
            ChunkSize = chunkSize;
            Dimension = dimension;
        }

        public int ChunkSize { get; }

        public int Dimension { get; }

        public int ChunkCount { get; private set; }

        public int MinChunkX { get; private set; } = int.MaxValue;

        public int MaxChunkX { get; private set; } = int.MinValue;

        public int MinChunkZ { get; private set; } = int.MaxValue;

        public int MaxChunkZ { get; private set; } = int.MinValue;

        public ushort MinTerrainHeight { get; private set; } = ushort.MaxValue;

        public ushort MaxTerrainHeight { get; private set; } = ushort.MinValue;

        public int UsableHeightSamples { get; private set; }

        public void IncludeChunk(int chunkX, int chunkZ, ushort minHeight, ushort maxHeight, int sampleCount)
        {
            ChunkCount++;
            MinChunkX = Math.Min(MinChunkX, chunkX);
            MaxChunkX = Math.Max(MaxChunkX, chunkX);
            MinChunkZ = Math.Min(MinChunkZ, chunkZ);
            MaxChunkZ = Math.Max(MaxChunkZ, chunkZ);
            MinTerrainHeight = Math.Min(MinTerrainHeight, minHeight);
            MaxTerrainHeight = Math.Max(MaxTerrainHeight, maxHeight);
            UsableHeightSamples += sampleCount;
        }
    }

    private sealed class PreviewPlan
    {
        public required int ChunkCount { get; init; }

        public required int ChunkSize { get; init; }

        public required int TargetDimension { get; init; }

        public required int MinChunkX { get; init; }

        public required int MaxChunkX { get; init; }

        public required int MinChunkZ { get; init; }

        public required int MaxChunkZ { get; init; }

        public required ushort MinTerrainHeight { get; init; }

        public required ushort MaxTerrainHeight { get; init; }

        public required int SamplingStep { get; init; }

        public required int OutputWidth { get; init; }

        public required int OutputHeight { get; init; }
    }
}

