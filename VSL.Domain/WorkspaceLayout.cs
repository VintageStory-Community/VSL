using System.Text.RegularExpressions;

namespace VSL.Domain;

public static partial class WorkspaceLayout
{
    private static readonly string[] PortableMarkerFileNames =
    {
        "portable.mode",
        "VSL.portable",
        ".portable"
    };

    private static readonly object StorageSettingsGate = new();
    private static string? _dataRootOverride;
    private static string? _savesRootOverride;

    public static bool IsPortableMode { get; } = DetectPortableMode();

    public static string WorkspaceRoot { get; } = ResolveWorkspaceRoot();

    public static string DefaultDataRoot => Path.Combine(WorkspaceRoot, "data");

    public static string DefaultSavesRoot => Path.Combine(WorkspaceRoot, "saves");

    public static string DataRoot => ResolveConfiguredRoot(_dataRootOverride, DefaultDataRoot);

    public static string SavesRoot => ResolveConfiguredRoot(_savesRootOverride, DefaultSavesRoot);

    public static string VersionsCachePath => Path.Combine(WorkspaceRoot, "versions-cache.json");

    public static string ProfilesPath => Path.Combine(WorkspaceRoot, "profiles.json");

    public static string LauncherSettingsPath => Path.Combine(WorkspaceRoot, "launcher-settings.json");

    public static string TempPath => Path.Combine(WorkspaceRoot, ".tmp");

    public static string PackagesPath => Path.Combine(WorkspaceRoot, "packages");

    public static string ServersRoot => Path.Combine(WorkspaceRoot, "servers", "windows");

    public static string ProfilesRoot => Path.Combine(WorkspaceRoot, "profiles");

    public static string GetServerInstallPath(string version) => Path.Combine(ServersRoot, version);

    public static string GetProfileRoot(string profileId) => Path.Combine(ProfilesRoot, profileId);

    public static string GetProfileDataPath(string profileId) => Path.Combine(DataRoot, profileId);

    public static string GetProfileSavesPath(string profileId) => Path.Combine(SavesRoot, profileId);

    public static string GetServerConfigPath(string dataPath) => Path.Combine(dataPath, "serverconfig.json");

    public static string GetSavesPath(string dataPath) => Path.Combine(dataPath, "Saves");

    public static string GetModsPath(string dataPath) => Path.Combine(dataPath, "Mods");

    public static string GetLogsPath(string dataPath) => Path.Combine(dataPath, "Logs");

    public static string GetServerMainLogPath(string dataPath) => Path.Combine(GetLogsPath(dataPath), "server-main.log");

    public static string GetDefaultSaveFile(ServerProfile profile) => GetProfileDefaultSaveFile(profile.Id);

    public static string GetProfileDefaultSaveFile(string profileId) => Path.Combine(GetProfileSavesPath(profileId), "default.vcdbs");

    public static string GetDefaultSaveFile(string dataPath) => Path.Combine(GetSavesPath(dataPath), "default.vcdbs");

    public static void ApplyStorageSettings(string? dataRoot, string? savesRoot)
    {
        lock (StorageSettingsGate)
        {
            _dataRootOverride = NormalizeDirectoryOrNull(dataRoot);
            _savesRootOverride = NormalizeDirectoryOrNull(savesRoot);
        }

        EnsureStorageRootsExist();
    }

    public static void EnsureWorkspaceExists()
    {
        Directory.CreateDirectory(WorkspaceRoot);
        Directory.CreateDirectory(TempPath);
        Directory.CreateDirectory(PackagesPath);
        Directory.CreateDirectory(ServersRoot);
        Directory.CreateDirectory(ProfilesRoot);
        EnsureStorageRootsExist();
    }

    public static string SanitizeFileName(string name)
    {
        var invalidChars = Path.GetInvalidFileNameChars();
        var sanitized = string.Join('_', name.Split(invalidChars, StringSplitOptions.RemoveEmptyEntries));
        return string.IsNullOrWhiteSpace(sanitized) ? "unnamed" : sanitized;
    }

    public static string TryExtractVersionFromServerFileName(string fileName)
    {
        // Example: vs_server_win-x64_1.21.7.zip
        var match = VersionPattern().Match(fileName);
        return match.Success ? match.Groups["version"].Value : "local-" + DateTimeOffset.UtcNow.ToUnixTimeSeconds();
    }

    private static string ResolveWorkspaceRoot()
    {
        var localAppDataWorkspace = Path.Combine(
            Environment.GetFolderPath(Environment.SpecialFolder.LocalApplicationData),
            "VSL",
            "workspace");
        var portableWorkspace = Path.Combine(AppContext.BaseDirectory, "workspace");
        var candidateRoots = IsPortableMode
            ? new[] { portableWorkspace, localAppDataWorkspace }
            : new[] { localAppDataWorkspace, portableWorkspace };

        foreach (var candidate in candidateRoots)
        {
            try
            {
                Directory.CreateDirectory(candidate);
                return candidate;
            }
            catch
            {
                // Try next candidate.
            }
        }

        // Last resort.
        return Path.Combine(Path.GetTempPath(), "VSL", "workspace");
    }

    private static bool DetectPortableMode()
    {
        var envValue = Environment.GetEnvironmentVariable("VSL_PORTABLE");
        if (IsEnabledFlag(envValue))
        {
            return true;
        }

        foreach (var markerFileName in PortableMarkerFileNames)
        {
            var markerPath = Path.Combine(AppContext.BaseDirectory, markerFileName);
            if (File.Exists(markerPath))
            {
                return true;
            }
        }

        return false;
    }

    private static bool IsEnabledFlag(string? value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return false;
        }

        return value.Trim().ToLowerInvariant() is "1" or "true" or "yes" or "y" or "on";
    }

    private static void EnsureStorageRootsExist()
    {
        Directory.CreateDirectory(DataRoot);
        Directory.CreateDirectory(SavesRoot);
    }

    private static string ResolveConfiguredRoot(string? configuredRoot, string fallbackRoot)
    {
        var normalized = NormalizeDirectoryOrNull(configuredRoot);
        return string.IsNullOrWhiteSpace(normalized) ? fallbackRoot : normalized;
    }

    private static string? NormalizeDirectoryOrNull(string? path)
    {
        if (string.IsNullOrWhiteSpace(path))
        {
            return null;
        }

        try
        {
            return Path.GetFullPath(path.Trim());
        }
        catch
        {
            return null;
        }
    }

    [GeneratedRegex(@"vs_server_win-x64_(?<version>[^\.]+\.\d+\.\d+(?:-[^\.]+(?:\.\d+)?)?)\.zip", RegexOptions.IgnoreCase)]
    private static partial Regex VersionPattern();
}
