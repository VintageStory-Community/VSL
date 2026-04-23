using System.Text.Json;
using System.Text.Json.Nodes;
using VSL.Application;
using VSL.Domain;

namespace VSL.Infrastructure.Services;

public sealed class ServerConfigService : IServerConfigService
{
    private static readonly JsonSerializerOptions JsonWriteOptions = new() { WriteIndented = true };

    public async Task<OperationResult<ServerCommonSettings>> LoadServerSettingsAsync(ServerProfile profile, CancellationToken cancellationToken = default)
    {
        var loadResult = await LoadRootAsync(profile, cancellationToken);
        if (!loadResult.IsSuccess || loadResult.Value is null)
        {
            return OperationResult<ServerCommonSettings>.Failed(loadResult.Message ?? "读取配置失败。", loadResult.Exception);
        }

        var root = loadResult.Value;
        return OperationResult<ServerCommonSettings>.Success(new ServerCommonSettings
        {
            ServerName = ReadString(root["ServerName"], "Vintage Story Server"),
            Ip = ReadNullableString(root["Ip"]),
            Port = ReadInt(root["Port"], 42420),
            MaxClients = ReadInt(root["MaxClients"], 16),
            Password = ReadNullableString(root["Password"]),
            AdvertiseServer = ReadBool(root["AdvertiseServer"], false),
            WhitelistMode = ReadInt(root["WhitelistMode"], 0),
            AllowPvP = ReadBool(root["AllowPvP"], true),
            AllowFireSpread = ReadBool(root["AllowFireSpread"], true),
            AllowFallingBlocks = ReadBool(root["AllowFallingBlocks"], true)
        });
    }

    public async Task<OperationResult<WorldSettings>> LoadWorldSettingsAsync(ServerProfile profile, CancellationToken cancellationToken = default)
    {
        var loadResult = await LoadRootAsync(profile, cancellationToken);
        if (!loadResult.IsSuccess || loadResult.Value is null)
        {
            return OperationResult<WorldSettings>.Failed(loadResult.Message ?? "读取配置失败。", loadResult.Exception);
        }

        var root = loadResult.Value;
        var worldConfig = GetOrCreateObject(root, "WorldConfig");
        var worldRules = GetOrCreateObject(worldConfig, "WorldConfiguration");

        var mapSizeY = ReadNullableInt(worldConfig["MapSizeY"]);
        if (mapSizeY is null)
        {
            mapSizeY = ReadNullableInt(worldRules["worldHeight"]);
        }

        return OperationResult<WorldSettings>.Success(new WorldSettings
        {
            Seed = ReadString(worldConfig["Seed"], "123456789"),
            WorldName = ReadString(worldConfig["WorldName"], "A new world"),
            SaveFileLocation = ReadString(worldConfig["SaveFileLocation"], WorkspaceLayout.GetDefaultSaveFile(profile)),
            PlayStyle = ReadString(worldConfig["PlayStyle"], "surviveandbuild"),
            WorldType = ReadString(worldConfig["WorldType"], "standard"),
            WorldHeight = mapSizeY ?? 256
        });
    }

    public async Task<OperationResult<IReadOnlyList<WorldRuleValue>>> LoadWorldRulesAsync(ServerProfile profile, CancellationToken cancellationToken = default)
    {
        var loadResult = await LoadRootAsync(profile, cancellationToken);
        if (!loadResult.IsSuccess || loadResult.Value is null)
        {
            return OperationResult<IReadOnlyList<WorldRuleValue>>.Failed(loadResult.Message ?? "读取配置失败。", loadResult.Exception);
        }

        var worldConfig = GetOrCreateObject(loadResult.Value, "WorldConfig");
        var worldRules = GetOrCreateObject(worldConfig, "WorldConfiguration");
        var root = loadResult.Value;

        var values = WorldRuleCatalog.DefaultRules
            .Select(def => new WorldRuleValue
            {
                Definition = def,
                Value = ReadFlexibleString(worldRules[def.Key])
                    ?? ReadRuleFallbackValue(def.Key, root, worldConfig)
                    ?? def.DefaultValue
            })
            .ToList();

        return OperationResult<IReadOnlyList<WorldRuleValue>>.Success(values);
    }

    public async Task<OperationResult> SaveCommonSettingsAsync(
        ServerProfile profile,
        ServerCommonSettings serverSettings,
        WorldSettings worldSettings,
        IReadOnlyList<WorldRuleValue> rules,
        CancellationToken cancellationToken = default)
    {
        var loadResult = await LoadRootAsync(profile, cancellationToken);
        if (!loadResult.IsSuccess || loadResult.Value is null)
        {
            return OperationResult.Failed(loadResult.Message ?? "读取配置失败。", loadResult.Exception);
        }

        try
        {
            var root = loadResult.Value;
            root["ServerName"] = serverSettings.ServerName;
            root["Ip"] = string.IsNullOrWhiteSpace(serverSettings.Ip) ? null : serverSettings.Ip;
            root["Port"] = serverSettings.Port;
            root["MaxClients"] = serverSettings.MaxClients;
            root["Password"] = string.IsNullOrWhiteSpace(serverSettings.Password) ? null : serverSettings.Password;
            root["AdvertiseServer"] = serverSettings.AdvertiseServer;
            root["WhitelistMode"] = serverSettings.WhitelistMode;
            root["AllowPvP"] = serverSettings.AllowPvP;
            root["AllowFireSpread"] = serverSettings.AllowFireSpread;
            root["AllowFallingBlocks"] = serverSettings.AllowFallingBlocks;

            var worldConfig = GetOrCreateObject(root, "WorldConfig");
            worldConfig["Seed"] = worldSettings.Seed;
            worldConfig["WorldName"] = worldSettings.WorldName;
            worldConfig["SaveFileLocation"] = worldSettings.SaveFileLocation;
            worldConfig["PlayStyle"] = worldSettings.PlayStyle;
            worldConfig["WorldType"] = worldSettings.WorldType;
            worldConfig["MapSizeY"] = worldSettings.WorldHeight;

            var worldRules = GetOrCreateObject(worldConfig, "WorldConfiguration");
            if (worldSettings.WorldHeight.HasValue)
            {
                worldRules["worldHeight"] = worldSettings.WorldHeight.Value;
            }

            foreach (var rule in rules)
            {
                var normalizedValue = rule.Value?.Trim();
                if (string.IsNullOrWhiteSpace(normalizedValue))
                {
                    continue;
                }

                if (string.Equals(rule.Definition.Key, "worldWidth", StringComparison.OrdinalIgnoreCase)
                    && int.TryParse(normalizedValue, out var worldWidth))
                {
                    root["MapSizeX"] = worldWidth;
                    worldRules[rule.Definition.Key] = worldWidth;
                    continue;
                }

                if (string.Equals(rule.Definition.Key, "worldLength", StringComparison.OrdinalIgnoreCase)
                    && int.TryParse(normalizedValue, out var worldLength))
                {
                    root["MapSizeZ"] = worldLength;
                    worldRules[rule.Definition.Key] = worldLength;
                    continue;
                }

                if (rule.Definition.Type == WorldRuleType.Boolean && bool.TryParse(normalizedValue, out var boolValue))
                {
                    worldRules[rule.Definition.Key] = boolValue;
                }
                else if (rule.Definition.Type == WorldRuleType.Number && int.TryParse(normalizedValue, out var intValue))
                {
                    worldRules[rule.Definition.Key] = intValue;
                }
                else
                {
                    worldRules[rule.Definition.Key] = normalizedValue;
                }
            }

            await SaveRootAsync(profile, root, cancellationToken);
            return OperationResult.Success("配置保存成功。");
        }
        catch (Exception ex)
        {
            return OperationResult.Failed("保存配置失败。", ex);
        }
    }

    public async Task<OperationResult<string>> LoadRawJsonAsync(ServerProfile profile, CancellationToken cancellationToken = default)
    {
        try
        {
            var configPath = WorkspaceLayout.GetServerConfigPath(profile.DataPath);
            if (!File.Exists(configPath))
            {
                return OperationResult<string>.Failed("未找到 serverconfig.json。");
            }

            var json = await File.ReadAllTextAsync(configPath, cancellationToken);
            return OperationResult<string>.Success(json);
        }
        catch (Exception ex)
        {
            return OperationResult<string>.Failed("读取原始 JSON 失败。", ex);
        }
    }

    public async Task<OperationResult> SaveRawJsonAsync(ServerProfile profile, string json, CancellationToken cancellationToken = default)
    {
        try
        {
            if (string.IsNullOrWhiteSpace(json))
            {
                return OperationResult.Failed("JSON 内容为空。");
            }

            var node = JsonNode.Parse(json);
            if (node is not JsonObject root)
            {
                return OperationResult.Failed("配置文件根节点必须是 JSON 对象。");
            }

            if (root["WorldConfig"] is not JsonObject)
            {
                return OperationResult.Failed("配置必须包含 WorldConfig 对象。");
            }

            await SaveRootAsync(profile, root, cancellationToken);
            return OperationResult.Success("高级 JSON 保存成功。");
        }
        catch (JsonException ex)
        {
            return OperationResult.Failed($"JSON 语法错误: {ex.Message}", ex);
        }
        catch (Exception ex)
        {
            return OperationResult.Failed("保存原始 JSON 失败。", ex);
        }
    }

    private static async Task<OperationResult<JsonObject>> LoadRootAsync(ServerProfile profile, CancellationToken cancellationToken)
    {
        try
        {
            var configPath = WorkspaceLayout.GetServerConfigPath(profile.DataPath);
            if (!File.Exists(configPath))
            {
                return OperationResult<JsonObject>.Failed("未找到 serverconfig.json，请先创建档案。");
            }

            await using var stream = new FileStream(configPath, FileMode.Open, FileAccess.Read, FileShare.ReadWrite);
            var node = await JsonNode.ParseAsync(stream, cancellationToken: cancellationToken);
            if (node is not JsonObject root)
            {
                return OperationResult<JsonObject>.Failed("serverconfig.json 格式错误。");
            }

            return OperationResult<JsonObject>.Success(root);
        }
        catch (Exception ex)
        {
            return OperationResult<JsonObject>.Failed("读取配置文件失败。", ex);
        }
    }

    private static async Task SaveRootAsync(ServerProfile profile, JsonObject root, CancellationToken cancellationToken)
    {
        var configPath = WorkspaceLayout.GetServerConfigPath(profile.DataPath);
        var directory = Path.GetDirectoryName(configPath);
        if (!string.IsNullOrWhiteSpace(directory))
        {
            Directory.CreateDirectory(directory);
        }

        await File.WriteAllTextAsync(configPath, root.ToJsonString(JsonWriteOptions), cancellationToken);
    }

    private static JsonObject GetOrCreateObject(JsonObject root, string propertyName)
    {
        if (root[propertyName] is JsonObject obj)
        {
            return obj;
        }

        var created = new JsonObject();
        root[propertyName] = created;
        return created;
    }

    private static string ReadString(JsonNode? node, string defaultValue)
    {
        return node?.GetValue<string>() ?? defaultValue;
    }

    private static string? ReadNullableString(JsonNode? node)
    {
        return node is null ? null : node.GetValue<string?>();
    }

    private static int ReadInt(JsonNode? node, int defaultValue)
    {
        if (node is null)
        {
            return defaultValue;
        }

        if (node.GetValueKind() == JsonValueKind.Number && node is JsonValue numericValue && numericValue.TryGetValue<int>(out var value))
        {
            return value;
        }

        if (node.GetValueKind() == JsonValueKind.String && int.TryParse(node.GetValue<string>(), out value))
        {
            return value;
        }

        return defaultValue;
    }

    private static int? ReadNullableInt(JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        if (node.GetValueKind() == JsonValueKind.Number && node is JsonValue numericValue && numericValue.TryGetValue<int>(out var numeric))
        {
            return numeric;
        }

        if (node.GetValueKind() == JsonValueKind.String && int.TryParse(node.GetValue<string>(), out numeric))
        {
            return numeric;
        }

        return null;
    }

    private static bool ReadBool(JsonNode? node, bool defaultValue)
    {
        if (node is null)
        {
            return defaultValue;
        }

        if (node.GetValueKind() == JsonValueKind.True || node.GetValueKind() == JsonValueKind.False)
        {
            return node.GetValue<bool>();
        }

        if (node.GetValueKind() == JsonValueKind.String && bool.TryParse(node.GetValue<string>(), out var parsed))
        {
            return parsed;
        }

        return defaultValue;
    }

    private static string? ReadFlexibleString(JsonNode? node)
    {
        if (node is null)
        {
            return null;
        }

        return node.GetValueKind() switch
        {
            JsonValueKind.String => node.GetValue<string>(),
            JsonValueKind.True => bool.TrueString.ToLowerInvariant(),
            JsonValueKind.False => bool.FalseString.ToLowerInvariant(),
            JsonValueKind.Number => node.ToString(),
            _ => node.ToJsonString()
        };
    }

    private static string? ReadRuleFallbackValue(string key, JsonObject root, JsonObject worldConfig)
    {
        return key switch
        {
            "worldWidth" => ReadFlexibleString(root["MapSizeX"]) ?? ReadFlexibleString(worldConfig["MapSizeX"]),
            "worldLength" => ReadFlexibleString(root["MapSizeZ"]) ?? ReadFlexibleString(worldConfig["MapSizeZ"]),
            _ => null
        };
    }
}
