namespace VSL.Domain;

public sealed class MapPreviewData
{
    public required string SaveFilePath { get; init; }

    public required int Width { get; init; }

    public required int Height { get; init; }

    public required byte[] ColorPixelsBgra32 { get; init; }

    public required byte[] GrayscalePixelsBgra32 { get; init; }

    public required int MapSizeX { get; init; }

    public required int MapSizeZ { get; init; }

    public required int ChunkSize { get; init; }

    public required int ChunkCount { get; init; }

    public required int Dimension { get; init; }

    public required int MinChunkX { get; init; }

    public required int MaxChunkX { get; init; }

    public required int MinChunkZ { get; init; }

    public required int MaxChunkZ { get; init; }

    public required int SamplingStep { get; init; }

    public required ushort MinTerrainHeight { get; init; }

    public required ushort MaxTerrainHeight { get; init; }
}
