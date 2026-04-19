using System.Collections.ObjectModel;
using System.IO;
using System.Text;
using System.Windows;
using System.Windows.Input;
using Microsoft.Win32;
using VSL.Application;
using VSL.Domain;
using Wpf.Ui;
using Wpf.Ui.Extensions;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class MainViewModel : ObservableObject, IAsyncDisposable
{
    private readonly IVersionCatalogService _versionCatalogService;
    private readonly IPackageService _packageService;
    private readonly IProfileService _profileService;
    private readonly IServerConfigService _serverConfigService;
    private readonly ISaveService _saveService;
    private readonly IModService _modService;
    private readonly IServerProcessService _serverProcessService;
    private readonly ILogTailService _logTailService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _toggleSelectedModCommand;
    private readonly AsyncRelayCommand _installSelectedReleaseCommand;
    private readonly AsyncRelayCommand _createProfileCommand;
    private readonly AsyncRelayCommand _deleteProfileCommand;
    private readonly AsyncRelayCommand _startServerCommand;
    private readonly AsyncRelayCommand _stopServerCommand;
    private readonly AsyncRelayCommand _sendConsoleCommand;
    private readonly AsyncRelayCommand _setActiveSaveCommand;
    private readonly AsyncRelayCommand _saveCommonConfigCommand;
    private readonly AsyncRelayCommand _saveRawJsonCommand;
    private readonly AsyncRelayCommand _backupSaveCommand;

    private bool _includeUnstable;
    private bool _isBusy;
    private string _busyText = string.Empty;
    private string _lastMessage = string.Empty;
    private double _installProgress;
    private ServerRelease? _selectedRelease;
    private ServerProfile? _selectedProfile;
    private SaveFileEntry? _selectedSave;
    private ModEntry? _selectedMod;
    private string _newProfileName = "我的服务器";
    private string _newSaveName = "default";
    private string _rawJsonText = string.Empty;
    private string _consoleInput = string.Empty;
    private ServerRuntimeStatus _runtimeStatus = new();
    private string _serverName = "Vintage Story Server";
    private string? _ip;
    private int _port = 42420;
    private int _maxClients = 16;
    private string? _password;
    private bool _advertiseServer;
    private int _whitelistMode;
    private bool _allowPvP = true;
    private bool _allowFireSpread = true;
    private bool _allowFallingBlocks = true;
    private string _seed = "123456789";
    private string _worldName = "A new world";
    private string _saveFileLocation = string.Empty;
    private string _playStyle = "surviveandbuild";
    private string _worldType = "standard";
    private int? _worldHeight = 256;
    private string _uiLanguage = "zh-CN";
    private string _selectedNavKey = "versionprofile";
    private bool _isConsoleAutoFollow = true;

    public MainViewModel(
        IVersionCatalogService versionCatalogService,
        IPackageService packageService,
        IProfileService profileService,
        IServerConfigService serverConfigService,
        ISaveService saveService,
        IModService modService,
        IServerProcessService serverProcessService,
        ILogTailService logTailService,
        ISnackbarService snackbarService)
    {
        _versionCatalogService = versionCatalogService;
        _packageService = packageService;
        _profileService = profileService;
        _serverConfigService = serverConfigService;
        _saveService = saveService;
        _modService = modService;
        _serverProcessService = serverProcessService;
        _logTailService = logTailService;
        _snackbarService = snackbarService;

        RefreshReleasesCommand = new AsyncRelayCommand(RefreshReleasesAsync);
        ImportLocalServerZipCommand = new AsyncRelayCommand(ImportLocalServerZipAsync);
        _installSelectedReleaseCommand = new AsyncRelayCommand(InstallSelectedReleaseAsync, () => SelectedRelease is not null);
        InstallSelectedReleaseCommand = _installSelectedReleaseCommand;

        RefreshProfilesCommand = new AsyncRelayCommand(RefreshProfilesAsync);
        _createProfileCommand = new AsyncRelayCommand(CreateProfileAsync, () => SelectedRelease is not null && !string.IsNullOrWhiteSpace(NewProfileName));
        CreateProfileCommand = _createProfileCommand;
        _deleteProfileCommand = new AsyncRelayCommand(DeleteSelectedProfileAsync, () => SelectedProfile is not null);
        DeleteSelectedProfileCommand = _deleteProfileCommand;

        _saveCommonConfigCommand = new AsyncRelayCommand(SaveCommonConfigAsync, () => SelectedProfile is not null);
        SaveCommonConfigCommand = _saveCommonConfigCommand;
        LoadRawJsonCommand = new AsyncRelayCommand(LoadRawJsonAsync, () => SelectedProfile is not null);
        _saveRawJsonCommand = new AsyncRelayCommand(SaveRawJsonAsync, () => SelectedProfile is not null && !string.IsNullOrWhiteSpace(RawJsonText));
        SaveRawJsonCommand = _saveRawJsonCommand;

        RefreshSavesCommand = new AsyncRelayCommand(RefreshSavesAsync, () => SelectedProfile is not null);
        CreateSaveCommand = new AsyncRelayCommand(CreateSaveAsync, () => SelectedProfile is not null && !string.IsNullOrWhiteSpace(NewSaveName));
        _setActiveSaveCommand = new AsyncRelayCommand(SetSelectedSaveActiveAsync, () => SelectedProfile is not null && SelectedSave is not null);
        SetActiveSaveCommand = _setActiveSaveCommand;
        _backupSaveCommand = new AsyncRelayCommand(BackupActiveSaveAsync, () => SelectedProfile is not null);
        BackupSaveCommand = _backupSaveCommand;

        RefreshModsCommand = new AsyncRelayCommand(RefreshModsAsync, () => SelectedProfile is not null);
        ImportModZipCommand = new AsyncRelayCommand(ImportModZipAsync, () => SelectedProfile is not null);
        _toggleSelectedModCommand = new AsyncRelayCommand(ToggleSelectedModAsync, () => SelectedProfile is not null && SelectedMod is not null);
        ToggleSelectedModCommand = _toggleSelectedModCommand;

        _startServerCommand = new AsyncRelayCommand(StartServerAsync, () => !RuntimeStatus.IsRunning && !IsBusy);
        StartServerCommand = _startServerCommand;
        _stopServerCommand = new AsyncRelayCommand(StopServerAsync, () => RuntimeStatus.IsRunning && !IsBusy);
        StopServerCommand = _stopServerCommand;
        _sendConsoleCommand = new AsyncRelayCommand(SendConsoleCommandAsync, () => RuntimeStatus.IsRunning && !string.IsNullOrWhiteSpace(ConsoleInput) && !IsBusy);
        SendConsoleCommandCommand = _sendConsoleCommand;
        ClearConsoleCommand = new RelayCommand(() =>
        {
            ConsoleLines.Clear();
            RaiseConsoleCommandStates();
        });
        DownloadConsoleLogCommand = new AsyncRelayCommand(DownloadConsoleLogAsync, () => ConsoleLines.Count > 0);

        _serverProcessService.OutputReceived += OnProcessOutputReceived;
        _serverProcessService.StatusChanged += OnProcessStatusChanged;
        _logTailService.LogLineReceived += OnLogTailLineReceived;
    }

    public ObservableCollection<ServerRelease> Releases { get; } = [];

    public ObservableCollection<ServerProfile> Profiles { get; } = [];

    public ObservableCollection<SaveFileEntry> Saves { get; } = [];

    public ObservableCollection<ModEntry> Mods { get; } = [];

    public ObservableCollection<WorldRuleItemViewModel> WorldRules { get; } = [];

    public ObservableCollection<string> ConsoleLines { get; } = [];

    public ICommand RefreshReleasesCommand { get; }

    public ICommand ImportLocalServerZipCommand { get; }

    public ICommand InstallSelectedReleaseCommand { get; }

    public ICommand RefreshProfilesCommand { get; }

    public ICommand CreateProfileCommand { get; }

    public ICommand DeleteSelectedProfileCommand { get; }

    public ICommand SaveCommonConfigCommand { get; }

    public ICommand LoadRawJsonCommand { get; }

    public ICommand SaveRawJsonCommand { get; }

    public ICommand RefreshSavesCommand { get; }

    public ICommand CreateSaveCommand { get; }

    public ICommand SetActiveSaveCommand { get; }

    public ICommand BackupSaveCommand { get; }

    public ICommand RefreshModsCommand { get; }

    public ICommand ImportModZipCommand { get; }

    public ICommand ToggleSelectedModCommand { get; }

    public ICommand StartServerCommand { get; }

    public ICommand StopServerCommand { get; }

    public ICommand SendConsoleCommandCommand { get; }

    public ICommand ClearConsoleCommand { get; }

    public ICommand DownloadConsoleLogCommand { get; }

    public bool IncludeUnstable
    {
        get => _includeUnstable;
        set
        {
            if (SetProperty(ref _includeUnstable, value))
            {
                _ = RefreshReleasesAsync();
            }
        }
    }

    public bool IsBusy
    {
        get => _isBusy;
        private set => SetProperty(ref _isBusy, value);
    }

    public string BusyText
    {
        get => _busyText;
        private set => SetProperty(ref _busyText, value);
    }

    public string LastMessage
    {
        get => _lastMessage;
        private set => SetProperty(ref _lastMessage, value);
    }

    public double InstallProgress
    {
        get => _installProgress;
        private set => SetProperty(ref _installProgress, value);
    }

    public ServerRelease? SelectedRelease
    {
        get => _selectedRelease;
        set
        {
            if (SetProperty(ref _selectedRelease, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public ServerProfile? SelectedProfile
    {
        get => _selectedProfile;
        set
        {
            if (SetProperty(ref _selectedProfile, value))
            {
                UpdateCommandStates();
                _ = LoadSelectedProfileAsync();
            }
        }
    }

    public SaveFileEntry? SelectedSave
    {
        get => _selectedSave;
        set
        {
            if (SetProperty(ref _selectedSave, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public ModEntry? SelectedMod
    {
        get => _selectedMod;
        set
        {
            if (SetProperty(ref _selectedMod, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public string NewProfileName
    {
        get => _newProfileName;
        set
        {
            if (SetProperty(ref _newProfileName, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public string NewSaveName
    {
        get => _newSaveName;
        set
        {
            if (SetProperty(ref _newSaveName, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public string RawJsonText
    {
        get => _rawJsonText;
        set
        {
            if (SetProperty(ref _rawJsonText, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public string ConsoleInput
    {
        get => _consoleInput;
        set
        {
            if (SetProperty(ref _consoleInput, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public ServerRuntimeStatus RuntimeStatus
    {
        get => _runtimeStatus;
        private set
        {
            if (SetProperty(ref _runtimeStatus, value))
            {
                OnPropertyChanged(nameof(RuntimeStateText));
                UpdateCommandStates();
            }
        }
    }

    public string RuntimeStateText =>
        RuntimeStatus.IsRunning
            ? $"运行中 (PID: {RuntimeStatus.ProcessId}, 启动时间: {RuntimeStatus.StartedAtUtc?.ToLocalTime():yyyy-MM-dd HH:mm:ss})"
            : "未运行";

    public string UiLanguage
    {
        get => _uiLanguage;
        set => SetProperty(ref _uiLanguage, value);
    }

    public string SelectedNavKey
    {
        get => _selectedNavKey;
        set => SetProperty(ref _selectedNavKey, value);
    }

    public bool IsConsoleAutoFollow
    {
        get => _isConsoleAutoFollow;
        set => SetProperty(ref _isConsoleAutoFollow, value);
    }

    public string ServerName { get => _serverName; set => SetProperty(ref _serverName, value); }
    public string? Ip { get => _ip; set => SetProperty(ref _ip, value); }
    public int Port { get => _port; set => SetProperty(ref _port, value); }
    public int MaxClients { get => _maxClients; set => SetProperty(ref _maxClients, value); }
    public string? Password { get => _password; set => SetProperty(ref _password, value); }
    public bool AdvertiseServer { get => _advertiseServer; set => SetProperty(ref _advertiseServer, value); }
    public int WhitelistMode { get => _whitelistMode; set => SetProperty(ref _whitelistMode, value); }
    public bool AllowPvP { get => _allowPvP; set => SetProperty(ref _allowPvP, value); }
    public bool AllowFireSpread { get => _allowFireSpread; set => SetProperty(ref _allowFireSpread, value); }
    public bool AllowFallingBlocks { get => _allowFallingBlocks; set => SetProperty(ref _allowFallingBlocks, value); }
    public string Seed { get => _seed; set => SetProperty(ref _seed, value); }
    public string WorldName { get => _worldName; set => SetProperty(ref _worldName, value); }
    public string SaveFileLocation { get => _saveFileLocation; set => SetProperty(ref _saveFileLocation, value); }
    public string PlayStyle { get => _playStyle; set => SetProperty(ref _playStyle, value); }
    public string WorldType { get => _worldType; set => SetProperty(ref _worldType, value); }
    public int? WorldHeight { get => _worldHeight; set => SetProperty(ref _worldHeight, value); }

    public async Task InitializeAsync()
    {
        RuntimeStatus = _serverProcessService.CurrentStatus;
        await RefreshReleasesAsync();
        await RefreshProfilesAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _serverProcessService.OutputReceived -= OnProcessOutputReceived;
        _serverProcessService.StatusChanged -= OnProcessStatusChanged;
        _logTailService.LogLineReceived -= OnLogTailLineReceived;
        await _logTailService.StopAsync();
        _logTailService.Dispose();
    }

    private async Task RefreshReleasesAsync()
    {
        await RunBusyAsync("正在刷新版本列表...", async () =>
        {
            var official = await _versionCatalogService.GetOfficialReleasesAsync();
            var locals = await _versionCatalogService.GetLocalReleasesAsync();
            var filteredOfficial = official.Where(x => IncludeUnstable || x.Channel == ReleaseChannel.Stable);
            var merged = filteredOfficial.Concat(locals).ToList();

            UpdateCollection(Releases, merged);
            if (SelectedRelease is null && Releases.Count > 0)
            {
                SelectedRelease = Releases[0];
            }

            SetMessage($"已加载版本：{Releases.Count} 个。");
        });
    }

    private async Task ImportLocalServerZipAsync()
    {
        var dialog = new OpenFileDialog
        {
            Title = "导入本地服务端 ZIP",
            Filter = "ZIP 文件 (*.zip)|*.zip|所有文件 (*.*)|*.*"
        };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        await RunBusyAsync("正在导入本地版本包...", async () =>
        {
            var progress = new Progress<double>(v => InstallProgress = v * 100.0);
            var result = await _packageService.ImportLocalZipAsync(dialog.FileName, progress);
            if (!result.IsSuccess)
            {
                SetMessage(result.Message ?? "导入失败。");
                return;
            }

            SetMessage($"已导入本地版本：{result.Value!.Version}");
            await RefreshReleasesAsync();
            SelectedRelease = Releases.FirstOrDefault(x => x.Version == result.Value!.Version && x.Source == ReleaseSource.LocalImport) ?? SelectedRelease;
        });
    }

    private async Task InstallSelectedReleaseAsync()
    {
        if (SelectedRelease is null)
        {
            return;
        }

        await RunBusyAsync($"正在安装版本 {SelectedRelease.Version}...", async () =>
        {
            InstallProgress = 0;
            var progress = new Progress<double>(v => InstallProgress = v * 100.0);
            var result = await _packageService.InstallReleaseAsync(SelectedRelease, progress);
            SetMessage(result.IsSuccess
                ? $"版本安装完成: {SelectedRelease.Version}"
                : result.Message ?? "安装失败。");
            await RefreshReleasesAsync();
        });
    }

    private async Task RefreshProfilesAsync()
    {
        ServerProfile? profileToSelect = null;

        await RunBusyAsync("正在加载档案...", async () =>
        {
            var profiles = await _profileService.GetProfilesAsync();
            UpdateCollection(Profiles, profiles);

            if (Profiles.Count == 0)
            {
                profileToSelect = null;
            }
            else if (SelectedProfile is not null)
            {
                profileToSelect = Profiles.FirstOrDefault(x => x.Id == SelectedProfile.Id) ?? Profiles[0];
            }
            else
            {
                profileToSelect = Profiles[0];
            }

            SetMessage($"档案数量：{Profiles.Count}");
        });

        if (Profiles.Count == 0)
        {
            SelectedProfile = null;
            return;
        }

        if (profileToSelect is not null && (SelectedProfile is null || !string.Equals(SelectedProfile.Id, profileToSelect.Id, StringComparison.Ordinal)))
        {
            SelectedProfile = profileToSelect;
        }
    }

    private async Task CreateProfileAsync()
    {
        if (SelectedRelease is null)
        {
            return;
        }

        await RunBusyAsync("正在创建档案...", async () =>
        {
            var result = await _profileService.CreateProfileAsync(NewProfileName, SelectedRelease.Version);
            if (!result.IsSuccess || result.Value is null)
            {
                SetMessage(result.Message ?? "创建档案失败。");
                return;
            }

            await RefreshProfilesAsync();
            SelectedProfile = Profiles.FirstOrDefault(x => x.Id == result.Value.Id);
            SetMessage($"档案创建成功：{result.Value.Name}");
        });
    }

    private async Task DeleteSelectedProfileAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        if (MessageBox.Show($"确认删除档案“{SelectedProfile.Name}”？此操作会删除档案数据目录。", "确认删除", MessageBoxButton.YesNo, MessageBoxImage.Warning) != MessageBoxResult.Yes)
        {
            return;
        }

        await RunBusyAsync("正在删除档案...", async () =>
        {
            var result = await _profileService.DeleteProfileAsync(SelectedProfile.Id);
            SetMessage(result.IsSuccess ? "档案已删除。" : result.Message ?? "删除失败。");
            SelectedProfile = null;
            await RefreshProfilesAsync();
        });
    }

    private async Task LoadSelectedProfileAsync()
    {
        if (SelectedProfile is null)
        {
            UpdateCollection(Saves, []);
            UpdateCollection(Mods, []);
            UpdateCollection(WorldRules, []);
            RawJsonText = string.Empty;
            return;
        }

        var ownsBusyState = false;
        try
        {
            if (!IsBusy)
            {
                ownsBusyState = true;
                IsBusy = true;
                BusyText = "正在加载档案配置...";
                UpdateCommandStates();
            }

            var serverResult = await _serverConfigService.LoadServerSettingsAsync(SelectedProfile);
            var worldResult = await _serverConfigService.LoadWorldSettingsAsync(SelectedProfile);
            var rulesResult = await _serverConfigService.LoadWorldRulesAsync(SelectedProfile);
            var rawResult = await _serverConfigService.LoadRawJsonAsync(SelectedProfile);

            if (serverResult.IsSuccess && serverResult.Value is not null)
            {
                var s = serverResult.Value;
                ServerName = s.ServerName;
                Ip = s.Ip;
                Port = s.Port;
                MaxClients = s.MaxClients;
                Password = s.Password;
                AdvertiseServer = s.AdvertiseServer;
                WhitelistMode = s.WhitelistMode;
                AllowPvP = s.AllowPvP;
                AllowFireSpread = s.AllowFireSpread;
                AllowFallingBlocks = s.AllowFallingBlocks;
            }

            if (worldResult.IsSuccess && worldResult.Value is not null)
            {
                var w = worldResult.Value;
                Seed = w.Seed;
                WorldName = w.WorldName;
                SaveFileLocation = w.SaveFileLocation;
                PlayStyle = w.PlayStyle;
                WorldType = w.WorldType;
                WorldHeight = w.WorldHeight;

                SelectedProfile.ActiveSaveFile = w.SaveFileLocation;
                SelectedProfile.LastUpdatedUtc = DateTimeOffset.UtcNow;
                await _profileService.UpdateProfileAsync(SelectedProfile);
            }

            if (rulesResult.IsSuccess && rulesResult.Value is not null)
            {
                var uiRules = rulesResult.Value.Select(rule => new WorldRuleItemViewModel
                {
                    Definition = rule.Definition,
                    Value = rule.Value
                }).ToList();
                UpdateCollection(WorldRules, uiRules);
            }

            if (rawResult.IsSuccess && rawResult.Value is not null)
            {
                RawJsonText = rawResult.Value;
            }

            await RefreshSavesAsync();
            await RefreshModsAsync();
            SetMessage($"档案已加载：{SelectedProfile.Name}");
        }
        catch (Exception ex)
        {
            SetMessage($"加载档案失败: {ex.Message}");
        }
        finally
        {
            if (ownsBusyState)
            {
                IsBusy = false;
                BusyText = string.Empty;
                UpdateCommandStates();
            }
        }
    }

    private async Task SaveCommonConfigAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        await RunBusyAsync("正在保存配置...", async () =>
        {
            var serverSettings = new ServerCommonSettings
            {
                ServerName = ServerName,
                Ip = Ip,
                Port = Port,
                MaxClients = MaxClients,
                Password = Password,
                AdvertiseServer = AdvertiseServer,
                WhitelistMode = WhitelistMode,
                AllowPvP = AllowPvP,
                AllowFireSpread = AllowFireSpread,
                AllowFallingBlocks = AllowFallingBlocks
            };

            var worldSettings = new WorldSettings
            {
                Seed = Seed,
                WorldName = WorldName,
                SaveFileLocation = SaveFileLocation,
                PlayStyle = PlayStyle,
                WorldType = WorldType,
                WorldHeight = WorldHeight
            };

            var rules = WorldRules.Select(static x => x.ToDomainValue()).ToList();
            var result = await _serverConfigService.SaveCommonSettingsAsync(SelectedProfile, serverSettings, worldSettings, rules);
            if (!result.IsSuccess)
            {
                SetMessage(result.Message ?? "保存配置失败。");
                return;
            }

            SelectedProfile.ActiveSaveFile = SaveFileLocation;
            SelectedProfile.LastUpdatedUtc = DateTimeOffset.UtcNow;
            await _profileService.UpdateProfileAsync(SelectedProfile);

            await LoadRawJsonAsync();
            await RefreshSavesAsync();
            SetMessage("配置保存成功。");
        });
    }

    private async Task LoadRawJsonAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        var result = await _serverConfigService.LoadRawJsonAsync(SelectedProfile);
        SetMessage(result.IsSuccess ? "已刷新高级 JSON。" : result.Message ?? "加载高级 JSON 失败。");
        if (result.IsSuccess && result.Value is not null)
        {
            RawJsonText = result.Value;
        }
    }

    private async Task SaveRawJsonAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        await RunBusyAsync("正在保存高级 JSON...", async () =>
        {
            var result = await _serverConfigService.SaveRawJsonAsync(SelectedProfile, RawJsonText);
            SetMessage(result.IsSuccess ? "高级 JSON 保存成功。" : result.Message ?? "保存失败。");
            if (result.IsSuccess)
            {
                await LoadSelectedProfileAsync();
            }
        });
    }

    private async Task RefreshSavesAsync()
    {
        if (SelectedProfile is null)
        {
            UpdateCollection(Saves, []);
            return;
        }

        var saves = await _saveService.GetSavesAsync(SelectedProfile);
        UpdateCollection(Saves, saves);
        if (Saves.Count > 0)
        {
            SelectedSave = Saves.FirstOrDefault(x => x.FullPath.Equals(SelectedProfile.ActiveSaveFile, StringComparison.OrdinalIgnoreCase))
                           ?? Saves[0];
        }
    }

    private async Task CreateSaveAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        await RunBusyAsync("正在创建存档...", async () =>
        {
            var result = await _saveService.CreateSaveAsync(SelectedProfile, NewSaveName);
            SetMessage(result.IsSuccess ? $"存档已创建：{result.Value}" : result.Message ?? "创建存档失败。");

            if (result.IsSuccess && result.Value is not null)
            {
                SaveFileLocation = result.Value;
                SelectedProfile.ActiveSaveFile = result.Value;
                SelectedProfile.LastUpdatedUtc = DateTimeOffset.UtcNow;
                await _profileService.UpdateProfileAsync(SelectedProfile);
            }

            await RefreshSavesAsync();
            await LoadRawJsonAsync();
        });
    }

    private async Task SetSelectedSaveActiveAsync()
    {
        if (SelectedProfile is null || SelectedSave is null)
        {
            return;
        }

        await RunBusyAsync("正在切换存档...", async () =>
        {
            var result = await _saveService.SetActiveSaveAsync(SelectedProfile, SelectedSave.FullPath);
            SetMessage(result.IsSuccess ? "已切换当前存档。" : result.Message ?? "切换存档失败。");
            if (result.IsSuccess)
            {
                SaveFileLocation = SelectedSave.FullPath;
                SelectedProfile.ActiveSaveFile = SelectedSave.FullPath;
                SelectedProfile.LastUpdatedUtc = DateTimeOffset.UtcNow;
                await _profileService.UpdateProfileAsync(SelectedProfile);
                await LoadRawJsonAsync();
            }
        });
    }

    private async Task BackupActiveSaveAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        await RunBusyAsync("正在备份存档...", async () =>
        {
            var result = await _saveService.BackupActiveSaveAsync(SelectedProfile);
            SetMessage(result.IsSuccess ? $"备份完成：{result.Value}" : result.Message ?? "备份失败。");
        });
    }

    private async Task RefreshModsAsync()
    {
        if (SelectedProfile is null)
        {
            UpdateCollection(Mods, []);
            return;
        }

        var mods = await _modService.GetModsAsync(SelectedProfile);
        UpdateCollection(Mods, mods);
        SelectedMod = Mods.Count > 0 ? Mods[0] : null;
    }

    private async Task ImportModZipAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        var dialog = new OpenFileDialog
        {
            Title = "导入 Mod ZIP",
            Filter = "ZIP 文件 (*.zip)|*.zip|所有文件 (*.*)|*.*"
        };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        await RunBusyAsync("正在导入 Mod...", async () =>
        {
            var result = await _modService.ImportModZipAsync(SelectedProfile, dialog.FileName);
            SetMessage(result.IsSuccess ? $"已导入 Mod: {result.Value!.ModId}" : result.Message ?? "导入 Mod 失败。");
            await RefreshModsAsync();
            await LoadRawJsonAsync();
        });
    }

    private async Task ToggleSelectedModAsync()
    {
        if (SelectedProfile is null || SelectedMod is null)
        {
            return;
        }

        var targetEnabled = SelectedMod.IsDisabled;
        await RunBusyAsync(targetEnabled ? "正在启用 Mod..." : "正在禁用 Mod...", async () =>
        {
            var result = await _modService.SetModEnabledAsync(SelectedProfile, SelectedMod.ModId, SelectedMod.Version, targetEnabled);
            SetMessage(result.IsSuccess
                ? $"{SelectedMod.ModId} 已{(targetEnabled ? "启用" : "禁用")}。"
                : result.Message ?? "更新 Mod 状态失败。");

            await RefreshModsAsync();
            await LoadRawJsonAsync();
        });
    }

    private async Task StartServerAsync()
    {
        if (SelectedProfile is null)
        {
            var hint = Profiles.Count == 0
                ? "请先到“版本与档案”页创建服务器档案并安装对应版本。"
                : "请先在“版本与档案”页选择一个服务器档案。";
            SetMessage(hint);
            return;
        }

        var installPath = _packageService.GetInstallPath(SelectedProfile.Version);
        var serverExe = Path.Combine(installPath, "VintagestoryServer.exe");
        if (!File.Exists(serverExe))
        {
            var hint = $"未找到服务端程序：{serverExe}\n请先安装档案对应版本。";
            SetMessage(hint);
            return;
        }

        await RunBusyAsync("正在启动服务器...", async () =>
        {
            var result = await _serverProcessService.StartAsync(SelectedProfile);
            if (result.IsSuccess)
            {
                SetMessage("服务器已启动。");
            }
            else
            {
                var detail = BuildErrorMessage("启动服务器失败。", result.Message, result.Exception);
                ConsoleLines.Add($"[system] {detail}");
                TrimConsole();
                SetMessage(detail);
                return;
            }

            if (result.IsSuccess)
            {
                await _logTailService.StartAsync(SelectedProfile);
            }
        });
    }

    private async Task StopServerAsync()
    {
        await RunBusyAsync("正在停止服务器...", async () =>
        {
            var result = await _serverProcessService.StopAsync(TimeSpan.FromSeconds(12));
            await _logTailService.StopAsync();
            SetMessage(result.IsSuccess ? result.Message ?? "服务器已停止。" : result.Message ?? "停止服务器失败。");
        });
    }

    private async Task SendConsoleCommandAsync()
    {
        var text = ConsoleInput;
        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        var result = await _serverProcessService.SendCommandAsync(text);
        if (!result.IsSuccess)
        {
            SetMessage(result.Message ?? "发送命令失败。");
            return;
        }

        ConsoleInput = string.Empty;
    }

    private async Task DownloadConsoleLogAsync()
    {
        if (ConsoleLines.Count == 0)
        {
            SetMessage("当前没有日志可下载。");
            return;
        }

        var dialog = new SaveFileDialog
        {
            Title = "下载日志",
            Filter = "日志文件 (*.log)|*.log|文本文件 (*.txt)|*.txt|所有文件 (*.*)|*.*",
            FileName = $"vsl-console-{DateTime.Now:yyyyMMdd-HHmmss}.log",
            AddExtension = true,
            DefaultExt = "log"
        };
        if (dialog.ShowDialog() != true)
        {
            return;
        }

        try
        {
            var lines = ConsoleLines.ToArray();
            await File.WriteAllLinesAsync(dialog.FileName, lines, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            SetMessage($"日志已下载：{dialog.FileName}");
        }
        catch (Exception ex)
        {
            SetMessage($"下载日志失败: {ex.Message}");
        }
    }

    private async Task RunBusyAsync(string busyText, Func<Task> action)
    {
        if (IsBusy)
        {
            var hint = string.IsNullOrWhiteSpace(BusyText)
                ? "当前有任务正在执行，请稍候。"
                : $"当前正在执行：{BusyText}";
            SetMessage(hint);
            return;
        }

        try
        {
            IsBusy = true;
            BusyText = busyText;
            UpdateCommandStates();
            await action();
        }
        catch (Exception ex)
        {
            SetMessage($"发生异常: {ex.Message}");
        }
        finally
        {
            IsBusy = false;
            BusyText = string.Empty;
            UpdateCommandStates();
        }
    }

    private static string BuildErrorMessage(string summary, string? message, Exception? ex)
    {
        var details = string.IsNullOrWhiteSpace(message) ? summary : $"{summary}\n{message}";
        if (ex is null)
        {
            return details;
        }

        return $"{details}\n{ex.GetType().Name}: {ex.Message}";
    }

    private void SetMessage(string message)
    {
        LastMessage = $"[{DateTime.Now:HH:mm:ss}] {message}";
        ShowToast(message);
    }

    private void ShowToast(string message)
    {
        var appearance = ResolveToastAppearance(message);
        RunOnUi(() => _snackbarService.Show("操作提示", message, appearance));
    }

    private static ControlAppearance ResolveToastAppearance(string message)
    {
        if (message.Contains("失败", StringComparison.OrdinalIgnoreCase)
            || message.Contains("异常", StringComparison.OrdinalIgnoreCase)
            || message.Contains("错误", StringComparison.OrdinalIgnoreCase)
            || message.Contains("未找到", StringComparison.OrdinalIgnoreCase)
            || message.Contains("无法", StringComparison.OrdinalIgnoreCase))
        {
            return ControlAppearance.Danger;
        }

        if (message.Contains("成功", StringComparison.OrdinalIgnoreCase)
            || message.Contains("已", StringComparison.OrdinalIgnoreCase)
            || message.Contains("完成", StringComparison.OrdinalIgnoreCase)
            || message.Contains("启动", StringComparison.OrdinalIgnoreCase))
        {
            return ControlAppearance.Success;
        }

        return ControlAppearance.Info;
    }

    private static void UpdateCollection<T>(ObservableCollection<T> target, IEnumerable<T> source)
    {
        target.Clear();
        foreach (var item in source)
        {
            target.Add(item);
        }
    }

    private void OnProcessOutputReceived(object? sender, string line)
    {
        RunOnUi(() =>
        {
            ConsoleLines.Add(line);
            TrimConsole();
            RaiseConsoleCommandStates();
        });
    }

    private void OnProcessStatusChanged(object? sender, ServerRuntimeStatus status)
    {
        RunOnUi(() =>
        {
            RuntimeStatus = status;
            if (!status.IsRunning)
            {
                _ = _logTailService.StopAsync();
            }
        });
    }

    private void OnLogTailLineReceived(object? sender, string line)
    {
        RunOnUi(() =>
        {
            ConsoleLines.Add($"[log] {line}");
            TrimConsole();
            RaiseConsoleCommandStates();
        });
    }

    private void TrimConsole()
    {
        const int maxLines = 3000;
        while (ConsoleLines.Count > maxLines)
        {
            ConsoleLines.RemoveAt(0);
        }

        RaiseConsoleCommandStates();
    }

    private static void RunOnUi(Action action)
    {
        if (System.Windows.Application.Current?.Dispatcher is not { } dispatcher || dispatcher.CheckAccess())
        {
            action();
            return;
        }

        dispatcher.Invoke(action);
    }

    private void UpdateCommandStates()
    {
        _installSelectedReleaseCommand.RaiseCanExecuteChanged();
        _createProfileCommand.RaiseCanExecuteChanged();
        _deleteProfileCommand.RaiseCanExecuteChanged();
        _saveCommonConfigCommand.RaiseCanExecuteChanged();
        if (LoadRawJsonCommand is AsyncRelayCommand loadRawJsonCommand)
        {
            loadRawJsonCommand.RaiseCanExecuteChanged();
        }
        _saveRawJsonCommand.RaiseCanExecuteChanged();
        if (RefreshSavesCommand is AsyncRelayCommand refreshSavesCommand)
        {
            refreshSavesCommand.RaiseCanExecuteChanged();
        }
        if (CreateSaveCommand is AsyncRelayCommand createSaveCommand)
        {
            createSaveCommand.RaiseCanExecuteChanged();
        }
        _setActiveSaveCommand.RaiseCanExecuteChanged();
        _backupSaveCommand.RaiseCanExecuteChanged();
        if (RefreshModsCommand is AsyncRelayCommand refreshModsCommand)
        {
            refreshModsCommand.RaiseCanExecuteChanged();
        }
        if (ImportModZipCommand is AsyncRelayCommand importModZipCommand)
        {
            importModZipCommand.RaiseCanExecuteChanged();
        }
        _toggleSelectedModCommand.RaiseCanExecuteChanged();
        _startServerCommand.RaiseCanExecuteChanged();
        _stopServerCommand.RaiseCanExecuteChanged();
        _sendConsoleCommand.RaiseCanExecuteChanged();
        RaiseConsoleCommandStates();
    }

    private void RaiseConsoleCommandStates()
    {
        if (DownloadConsoleLogCommand is AsyncRelayCommand downloadCommand)
        {
            downloadCommand.RaiseCanExecuteChanged();
        }
    }
}
