using System.Collections.ObjectModel;
using System.Globalization;
using System.IO;
using System.Text;
using System.Windows;
using System.Windows.Input;
using System.Windows.Media;
using System.Windows.Media.Imaging;
using System.Windows.Threading;
using Microsoft.Win32;
using VSL.Application;
using VSL.Domain;
using Wpf.Ui;
using Wpf.Ui.Extensions;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class MainViewModel : ObservableObject, IAsyncDisposable
{
    private const string AutoStartRegistryKeyPath = @"Software\Microsoft\Windows\CurrentVersion\Run";
    private const string AutoStartRegistryValueName = "VSL";

    private readonly IVersionCatalogService _versionCatalogService;
    private readonly ILauncherSettingsService _launcherSettingsService;
    private readonly IPackageService _packageService;
    private readonly IProfileService _profileService;
    private readonly IServerConfigService _serverConfigService;
    private readonly ISaveService _saveService;
    private readonly IMapPreviewService _mapPreviewService;
    private readonly IModService _modService;
    private readonly IServerProcessService _serverProcessService;
    private readonly IVs2QQProcessService _vs2qqProcessService;
    private readonly ILogTailService _logTailService;
    private readonly ISnackbarService _snackbarService;
    private readonly DispatcherTimer _versionListsAutoRefreshTimer;

    private readonly AsyncRelayCommand _toggleSelectedModCommand;
    private readonly AsyncRelayCommand _refreshInstalledVersionsCommand;
    private readonly AsyncRelayCommand _installSelectedReleaseCommand;
    private readonly AsyncRelayCommand _createProfileCommand;
    private readonly AsyncRelayCommand _deleteProfileCommand;
    private readonly AsyncRelayCommand _deleteSelectedInstalledVersionCommand;
    private readonly AsyncRelayCommand _startServerCommand;
    private readonly AsyncRelayCommand _stopServerCommand;
    private readonly AsyncRelayCommand _sendConsoleCommand;
    private readonly AsyncRelayCommand _setActiveSaveCommand;
    private readonly AsyncRelayCommand _saveCommonConfigCommand;
    private readonly AsyncRelayCommand _saveRawJsonCommand;
    private readonly AsyncRelayCommand _backupSaveCommand;
    private readonly AsyncRelayCommand _loadMapPreviewCommand;
    private readonly AsyncRelayCommand _saveLauncherSettingsCommand;
    private readonly AsyncRelayCommand _loadVs2QQConfigCommand;
    private readonly AsyncRelayCommand _saveVs2QQConfigCommand;
    private readonly AsyncRelayCommand _startVs2QQCommand;
    private readonly AsyncRelayCommand _stopVs2QQCommand;
    private readonly RelayCommand _chooseLauncherDataDirectoryCommand;
    private readonly RelayCommand _chooseLauncherSaveDirectoryCommand;
    private readonly RelayCommand _chooseVs2QQDatabasePathCommand;

    private bool _includeUnstable;
    private bool _isBusy;
    private bool _isInstallProgressVisible;
    private bool _isServerStartupProgressVisible;
    private bool _isAutoRefreshingVersionLists;
    private string _busyText = string.Empty;
    private string _lastMessage = string.Empty;
    private double _installProgress;
    private double _serverStartupProgress;
    private ServerRelease? _selectedRelease;
    private InstalledVersionItemViewModel? _selectedInstalledVersion;
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
    private string _launcherDataDirectory = WorkspaceLayout.DefaultDataRoot;
    private string _launcherSaveDirectory = WorkspaceLayout.DefaultSavesRoot;
    private bool _launchAtStartup;
    private string _vs2qqOneBotWsUrl = "ws://127.0.0.1:3001/";
    private string _vs2qqAccessToken = string.Empty;
    private int _vs2qqReconnectIntervalSec = 5;
    private string _vs2qqDatabasePath = Path.Combine(WorkspaceLayout.WorkspaceRoot, "vs2qq", "vs2qq.db");
    private double _vs2qqPollIntervalSec = 1.0;
    private string _vs2qqDefaultEncoding = "utf-8";
    private string _vs2qqFallbackEncoding = "gbk";
    private string _vs2qqSuperUsersText = string.Empty;
    private bool _isVs2QQConsoleAutoFollow = true;
    private Vs2QQRuntimeStatus _vs2qqRuntimeStatus = new();
    private string _mapPreviewSummary = "暂无地图预览。";
    private string _mapPreviewSourcePath = string.Empty;
    private BitmapSource? _mapPreviewColorImage;
    private BitmapSource? _mapPreviewGrayscaleImage;
    private int _mapPreviewDimension;
    private int _mapPreviewMinChunkX;
    private int _mapPreviewMinChunkZ;
    private int _mapPreviewSamplingStep = 1;
    private int _mapPreviewWidth;
    private int _mapPreviewHeight;
    private int _mapPreviewMapSizeX;
    private int _mapPreviewMapSizeZ;

    public MainViewModel(
        IVersionCatalogService versionCatalogService,
        ILauncherSettingsService launcherSettingsService,
        IPackageService packageService,
        IProfileService profileService,
        IServerConfigService serverConfigService,
        ISaveService saveService,
        IMapPreviewService mapPreviewService,
        IModService modService,
        IServerProcessService serverProcessService,
        IVs2QQProcessService vs2qqProcessService,
        ILogTailService logTailService,
        ISnackbarService snackbarService)
    {
        _versionCatalogService = versionCatalogService;
        _launcherSettingsService = launcherSettingsService;
        _packageService = packageService;
        _profileService = profileService;
        _serverConfigService = serverConfigService;
        _saveService = saveService;
        _mapPreviewService = mapPreviewService;
        _modService = modService;
        _serverProcessService = serverProcessService;
        _vs2qqProcessService = vs2qqProcessService;
        _logTailService = logTailService;
        _snackbarService = snackbarService;

        RefreshReleasesCommand = new AsyncRelayCommand(RefreshReleasesAsync);
        ImportLocalServerZipCommand = new AsyncRelayCommand(ImportLocalServerZipAsync);
        _installSelectedReleaseCommand = new AsyncRelayCommand(InstallSelectedReleaseAsync, () => SelectedRelease is not null);
        InstallSelectedReleaseCommand = _installSelectedReleaseCommand;
        _refreshInstalledVersionsCommand = new AsyncRelayCommand(RefreshInstalledVersionsWithBusyAsync, () => !IsBusy);
        RefreshInstalledVersionsCommand = _refreshInstalledVersionsCommand;
        _deleteSelectedInstalledVersionCommand = new AsyncRelayCommand(DeleteSelectedInstalledVersionAsync, () => SelectedInstalledVersion is not null && !IsBusy);
        DeleteSelectedInstalledVersionCommand = _deleteSelectedInstalledVersionCommand;

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
        _saveLauncherSettingsCommand = new AsyncRelayCommand(SaveLauncherSettingsAsync, () => !IsBusy);
        SaveLauncherSettingsCommand = _saveLauncherSettingsCommand;
        _chooseLauncherDataDirectoryCommand = new RelayCommand(ChooseLauncherDataDirectory, () => !IsBusy);
        ChooseLauncherDataDirectoryCommand = _chooseLauncherDataDirectoryCommand;
        _chooseLauncherSaveDirectoryCommand = new RelayCommand(ChooseLauncherSaveDirectory, () => !IsBusy);
        ChooseLauncherSaveDirectoryCommand = _chooseLauncherSaveDirectoryCommand;
        _loadVs2QQConfigCommand = new AsyncRelayCommand(LoadVs2QQConfigAsync, () => !IsBusy);
        LoadVs2QQConfigCommand = _loadVs2QQConfigCommand;
        _saveVs2QQConfigCommand = new AsyncRelayCommand(SaveVs2QQConfigAsync, () => !IsBusy && CanSaveVs2QQSettings());
        SaveVs2QQConfigCommand = _saveVs2QQConfigCommand;
        _startVs2QQCommand = new AsyncRelayCommand(StartVs2QQAsync, () => !Vs2QQRuntimeStatus.IsRunning && !IsBusy);
        StartVs2QQCommand = _startVs2QQCommand;
        _stopVs2QQCommand = new AsyncRelayCommand(StopVs2QQAsync, () => Vs2QQRuntimeStatus.IsRunning && !IsBusy);
        StopVs2QQCommand = _stopVs2QQCommand;
        _chooseVs2QQDatabasePathCommand = new RelayCommand(ChooseVs2QQDatabasePath, () => !IsBusy);
        ChooseVs2QQDatabasePathCommand = _chooseVs2QQDatabasePathCommand;

        RefreshSavesCommand = new AsyncRelayCommand(RefreshSavesAsync, () => SelectedProfile is not null);
        CreateSaveCommand = new AsyncRelayCommand(CreateSaveAsync, () => SelectedProfile is not null && !string.IsNullOrWhiteSpace(NewSaveName));
        _setActiveSaveCommand = new AsyncRelayCommand(SetSelectedSaveActiveAsync, () => SelectedProfile is not null && SelectedSave is not null);
        SetActiveSaveCommand = _setActiveSaveCommand;
        _backupSaveCommand = new AsyncRelayCommand(BackupActiveSaveAsync, () => SelectedProfile is not null);
        BackupSaveCommand = _backupSaveCommand;
        _loadMapPreviewCommand = new AsyncRelayCommand(LoadMapPreviewAsync, () => SelectedProfile is not null && !IsBusy);
        LoadMapPreviewCommand = _loadMapPreviewCommand;

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
        ClearVs2QQConsoleCommand = new RelayCommand(() => Vs2QQConsoleLines.Clear());

        _serverProcessService.OutputReceived += OnProcessOutputReceived;
        _serverProcessService.StatusChanged += OnProcessStatusChanged;
        _vs2qqProcessService.OutputReceived += OnVs2QQOutputReceived;
        _vs2qqProcessService.StatusChanged += OnVs2QQStatusChanged;
        _logTailService.LogLineReceived += OnLogTailLineReceived;

        _versionListsAutoRefreshTimer = new DispatcherTimer
        {
            Interval = TimeSpan.FromSeconds(5)
        };
        _versionListsAutoRefreshTimer.Tick += VersionListsAutoRefreshTimerOnTick;
        _versionListsAutoRefreshTimer.Start();
    }

    public ObservableCollection<ServerRelease> Releases { get; } = [];

    public ObservableCollection<InstalledVersionItemViewModel> InstalledVersions { get; } = [];

    public ObservableCollection<ServerProfile> Profiles { get; } = [];

    public ObservableCollection<SaveFileEntry> Saves { get; } = [];

    public ObservableCollection<ModEntry> Mods { get; } = [];

    public ObservableCollection<WorldRuleItemViewModel> WorldRules { get; } = [];

    public ObservableCollection<string> ConsoleLines { get; } = [];

    public ObservableCollection<string> Vs2QQConsoleLines { get; } = [];

    public ICommand RefreshReleasesCommand { get; }

    public ICommand ImportLocalServerZipCommand { get; }

    public ICommand InstallSelectedReleaseCommand { get; }

    public ICommand RefreshInstalledVersionsCommand { get; }

    public ICommand DeleteSelectedInstalledVersionCommand { get; }

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

    public ICommand LoadMapPreviewCommand { get; }

    public ICommand RefreshModsCommand { get; }

    public ICommand ImportModZipCommand { get; }

    public ICommand ToggleSelectedModCommand { get; }

    public ICommand StartServerCommand { get; }

    public ICommand StopServerCommand { get; }

    public ICommand SendConsoleCommandCommand { get; }

    public ICommand ClearConsoleCommand { get; }

    public ICommand DownloadConsoleLogCommand { get; }

    public ICommand SaveLauncherSettingsCommand { get; }

    public ICommand ChooseLauncherDataDirectoryCommand { get; }

    public ICommand ChooseLauncherSaveDirectoryCommand { get; }

    public ICommand LoadVs2QQConfigCommand { get; }

    public ICommand SaveVs2QQConfigCommand { get; }

    public ICommand StartVs2QQCommand { get; }

    public ICommand StopVs2QQCommand { get; }

    public ICommand ChooseVs2QQDatabasePathCommand { get; }

    public ICommand ClearVs2QQConsoleCommand { get; }

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
        private set
        {
            if (SetProperty(ref _installProgress, value))
            {
                OnPropertyChanged(nameof(InstallProgressText));
            }
        }
    }

    public bool IsInstallProgressVisible
    {
        get => _isInstallProgressVisible;
        private set
        {
            if (SetProperty(ref _isInstallProgressVisible, value))
            {
                OnPropertyChanged(nameof(InstallProgressText));
            }
        }
    }

    public string InstallProgressText =>
        IsInstallProgressVisible
            ? $"安装进度：{InstallProgress:0}%"
            : string.Empty;

    public bool IsServerStartupProgressVisible
    {
        get => _isServerStartupProgressVisible;
        private set
        {
            if (SetProperty(ref _isServerStartupProgressVisible, value))
            {
                OnPropertyChanged(nameof(ServerStartupProgressText));
            }
        }
    }

    public double ServerStartupProgress
    {
        get => _serverStartupProgress;
        private set
        {
            if (SetProperty(ref _serverStartupProgress, value))
            {
                OnPropertyChanged(nameof(ServerStartupProgressText));
            }
        }
    }

    public string ServerStartupProgressText =>
        IsServerStartupProgressVisible
            ? $"启动进度：{ServerStartupProgress:0}%"
            : string.Empty;

    public InstalledVersionItemViewModel? SelectedInstalledVersion
    {
        get => _selectedInstalledVersion;
        set
        {
            if (SetProperty(ref _selectedInstalledVersion, value))
            {
                UpdateCommandStates();
            }
        }
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
                ClearMapPreview();
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
        set
        {
            if (SetProperty(ref _selectedNavKey, value)
                && string.Equals(value, "versionprofile", StringComparison.OrdinalIgnoreCase))
            {
                _ = AutoRefreshVersionListsAsync();
            }
        }
    }

    public bool IsConsoleAutoFollow
    {
        get => _isConsoleAutoFollow;
        set => SetProperty(ref _isConsoleAutoFollow, value);
    }

    public string LauncherDataDirectory
    {
        get => _launcherDataDirectory;
        set => SetProperty(ref _launcherDataDirectory, value);
    }

    public string LauncherSaveDirectory
    {
        get => _launcherSaveDirectory;
        set => SetProperty(ref _launcherSaveDirectory, value);
    }

    public bool LaunchAtStartup
    {
        get => _launchAtStartup;
        set => SetProperty(ref _launchAtStartup, value);
    }

    public string Vs2QQOneBotWsUrl
    {
        get => _vs2qqOneBotWsUrl;
        set
        {
            if (SetProperty(ref _vs2qqOneBotWsUrl, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public string Vs2QQAccessToken
    {
        get => _vs2qqAccessToken;
        set => SetProperty(ref _vs2qqAccessToken, value);
    }

    public int Vs2QQReconnectIntervalSec
    {
        get => _vs2qqReconnectIntervalSec;
        set
        {
            if (SetProperty(ref _vs2qqReconnectIntervalSec, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public string Vs2QQDatabasePath
    {
        get => _vs2qqDatabasePath;
        set
        {
            if (SetProperty(ref _vs2qqDatabasePath, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public double Vs2QQPollIntervalSec
    {
        get => _vs2qqPollIntervalSec;
        set
        {
            if (SetProperty(ref _vs2qqPollIntervalSec, value))
            {
                UpdateCommandStates();
            }
        }
    }

    public string Vs2QQDefaultEncoding
    {
        get => _vs2qqDefaultEncoding;
        set => SetProperty(ref _vs2qqDefaultEncoding, value);
    }

    public string Vs2QQFallbackEncoding
    {
        get => _vs2qqFallbackEncoding;
        set => SetProperty(ref _vs2qqFallbackEncoding, value);
    }

    public string Vs2QQSuperUsersText
    {
        get => _vs2qqSuperUsersText;
        set => SetProperty(ref _vs2qqSuperUsersText, value);
    }

    public bool IsVs2QQConsoleAutoFollow
    {
        get => _isVs2QQConsoleAutoFollow;
        set => SetProperty(ref _isVs2QQConsoleAutoFollow, value);
    }

    public Vs2QQRuntimeStatus Vs2QQRuntimeStatus
    {
        get => _vs2qqRuntimeStatus;
        private set
        {
            if (SetProperty(ref _vs2qqRuntimeStatus, value))
            {
                OnPropertyChanged(nameof(Vs2QQRuntimeStateText));
                UpdateCommandStates();
            }
        }
    }

    public string Vs2QQRuntimeStateText =>
        Vs2QQRuntimeStatus.IsRunning
            ? $"运行中 (OneBot: {Vs2QQRuntimeStatus.OneBotWsUrl}, 启动时间: {Vs2QQRuntimeStatus.StartedAtUtc?.ToLocalTime():yyyy-MM-dd HH:mm:ss})"
            : "未运行";

    public string MapPreviewSummary
    {
        get => _mapPreviewSummary;
        private set => SetProperty(ref _mapPreviewSummary, value);
    }

    public string MapPreviewSourcePath
    {
        get => _mapPreviewSourcePath;
        private set => SetProperty(ref _mapPreviewSourcePath, value);
    }

    public BitmapSource? MapPreviewColorImage
    {
        get => _mapPreviewColorImage;
        private set
        {
            if (SetProperty(ref _mapPreviewColorImage, value))
            {
                OnPropertyChanged(nameof(HasMapPreview));
            }
        }
    }

    public BitmapSource? MapPreviewGrayscaleImage
    {
        get => _mapPreviewGrayscaleImage;
        private set
        {
            if (SetProperty(ref _mapPreviewGrayscaleImage, value))
            {
                OnPropertyChanged(nameof(HasMapPreview));
            }
        }
    }

    public bool HasMapPreview => MapPreviewColorImage is not null && MapPreviewGrayscaleImage is not null;

    public int MapPreviewDimension
    {
        get => _mapPreviewDimension;
        private set => SetProperty(ref _mapPreviewDimension, value);
    }

    public int MapPreviewMinChunkX
    {
        get => _mapPreviewMinChunkX;
        private set => SetProperty(ref _mapPreviewMinChunkX, value);
    }

    public int MapPreviewMinChunkZ
    {
        get => _mapPreviewMinChunkZ;
        private set => SetProperty(ref _mapPreviewMinChunkZ, value);
    }

    public int MapPreviewSamplingStep
    {
        get => _mapPreviewSamplingStep;
        private set => SetProperty(ref _mapPreviewSamplingStep, value);
    }

    public int MapPreviewWidth
    {
        get => _mapPreviewWidth;
        private set => SetProperty(ref _mapPreviewWidth, value);
    }

    public int MapPreviewHeight
    {
        get => _mapPreviewHeight;
        private set => SetProperty(ref _mapPreviewHeight, value);
    }

    public int MapPreviewMapSizeX
    {
        get => _mapPreviewMapSizeX;
        private set => SetProperty(ref _mapPreviewMapSizeX, value);
    }

    public int MapPreviewMapSizeZ
    {
        get => _mapPreviewMapSizeZ;
        private set => SetProperty(ref _mapPreviewMapSizeZ, value);
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
        await LoadLauncherSettingsAsync();
        RuntimeStatus = _serverProcessService.CurrentStatus;
        Vs2QQRuntimeStatus = _vs2qqProcessService.CurrentStatus;
        await RefreshReleasesAsync();
        await RefreshProfilesAsync();
        await RefreshInstalledVersionsCoreAsync(showMessage: false);
    }

    public async ValueTask DisposeAsync()
    {
        _versionListsAutoRefreshTimer.Tick -= VersionListsAutoRefreshTimerOnTick;
        _versionListsAutoRefreshTimer.Stop();

        _serverProcessService.OutputReceived -= OnProcessOutputReceived;
        _serverProcessService.StatusChanged -= OnProcessStatusChanged;
        _vs2qqProcessService.OutputReceived -= OnVs2QQOutputReceived;
        _vs2qqProcessService.StatusChanged -= OnVs2QQStatusChanged;
        _logTailService.LogLineReceived -= OnLogTailLineReceived;
        await _logTailService.StopAsync();
        await _vs2qqProcessService.StopAsync(TimeSpan.FromSeconds(4));
        _logTailService.Dispose();
    }

    private async Task RefreshReleasesAsync()
    {
        await RunBusyAsync("正在刷新版本列表...", () => RefreshReleasesCoreAsync(showMessage: true));
    }

    private async Task RefreshReleasesCoreAsync(bool showMessage)
    {
        var official = await _versionCatalogService.GetOfficialReleasesAsync();
        var locals = await _versionCatalogService.GetLocalReleasesAsync();
        var filteredOfficial = official.Where(x => IncludeUnstable || x.Channel == ReleaseChannel.Stable);
        var merged = filteredOfficial.Concat(locals).ToList();

        UpdateCollection(Releases, merged);
        await RefreshInstalledVersionsCoreAsync(showMessage: false);

        if (SelectedRelease is not null)
        {
            SelectedRelease = Releases.FirstOrDefault(x => x.Version == SelectedRelease.Version && x.Source == SelectedRelease.Source)
                ?? Releases.FirstOrDefault(x => x.Version == SelectedRelease.Version)
                ?? Releases.FirstOrDefault();
        }
        else if (Releases.Count > 0)
        {
            SelectedRelease = Releases[0];
        }

        if (showMessage)
        {
            SetMessage($"已加载版本：{Releases.Count} 个。");
        }
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

        IsInstallProgressVisible = true;
        InstallProgress = 0;
        try
        {
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
                await RefreshReleasesCoreAsync(showMessage: false);
                SelectedRelease = Releases.FirstOrDefault(x => x.Version == result.Value!.Version && x.Source == ReleaseSource.LocalImport)
                    ?? SelectedRelease;
            });
        }
        finally
        {
            IsInstallProgressVisible = false;
        }
    }

    private async Task InstallSelectedReleaseAsync()
    {
        if (SelectedRelease is null)
        {
            return;
        }

        IsInstallProgressVisible = true;
        InstallProgress = 0;
        try
        {
            await RunBusyAsync($"正在安装版本 {SelectedRelease.Version}...", async () =>
            {
                InstallProgress = 3;
                var progress = new Progress<double>(v => InstallProgress = v * 100.0);
                var result = await _packageService.InstallReleaseAsync(SelectedRelease, progress);
                if (result.IsSuccess)
                {
                    InstallProgress = 100;
                }

                SetMessage(result.IsSuccess
                    ? $"版本安装完成: {SelectedRelease.Version}"
                    : result.Message ?? "安装失败。");
                await RefreshReleasesCoreAsync(showMessage: false);
            });
        }
        finally
        {
            IsInstallProgressVisible = false;
        }
    }

    private async Task RefreshInstalledVersionsWithBusyAsync()
    {
        await RunBusyAsync("正在刷新已安装版本...", () => RefreshInstalledVersionsCoreAsync(showMessage: true));
    }

    private async Task RefreshInstalledVersionsCoreAsync(bool showMessage)
    {
        var installedVersions = await _packageService.GetInstalledVersionsAsync();
        var profileUsageLookup = Profiles
            .GroupBy(x => x.Version, StringComparer.OrdinalIgnoreCase)
            .ToDictionary(x => x.Key, x => x.Count(), StringComparer.OrdinalIgnoreCase);

        var items = installedVersions
            .Select(version =>
            {
                profileUsageLookup.TryGetValue(version, out var profileCount);
                return new InstalledVersionItemViewModel
                {
                    Version = version,
                    InstallPath = _packageService.GetInstallPath(version),
                    ProfileCount = profileCount
                };
            })
            .OrderByDescending(x => x.Version, StringComparer.OrdinalIgnoreCase)
            .ToList();

        UpdateCollection(InstalledVersions, items);

        if (SelectedInstalledVersion is not null)
        {
            SelectedInstalledVersion = InstalledVersions.FirstOrDefault(x => string.Equals(x.Version, SelectedInstalledVersion.Version, StringComparison.OrdinalIgnoreCase));
        }
        else if (InstalledVersions.Count > 0)
        {
            SelectedInstalledVersion = InstalledVersions[0];
        }

        if (showMessage)
        {
            SetMessage($"已安装版本：{InstalledVersions.Count} 个。");
        }
    }

    private async Task DeleteSelectedInstalledVersionAsync()
    {
        if (SelectedInstalledVersion is null)
        {
            return;
        }

        var version = SelectedInstalledVersion.Version;
        var profilesUsingVersion = Profiles.Where(x => string.Equals(x.Version, version, StringComparison.OrdinalIgnoreCase)).ToList();
        if (profilesUsingVersion.Count > 0)
        {
            SetMessage($"无法删除 {version}：仍有 {profilesUsingVersion.Count} 个档案在使用该版本。");
            return;
        }

        if (MessageBox.Show(
                $"确认删除已安装版本“{version}”？\n目录：{SelectedInstalledVersion.InstallPath}",
                "确认删除版本",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning) != MessageBoxResult.Yes)
        {
            return;
        }

        await RunBusyAsync($"正在删除版本 {version}...", async () =>
        {
            var result = await _packageService.DeleteInstalledVersionAsync(version);
            SetMessage(result.IsSuccess ? $"已删除版本：{version}" : result.Message ?? "删除版本失败。");
            await RefreshInstalledVersionsCoreAsync(showMessage: false);
            await RefreshReleasesCoreAsync(showMessage: false);
        });
    }

    private async Task RefreshProfilesAsync()
    {
        await RunBusyAsync("正在加载档案...", () => RefreshProfilesCoreAsync(showMessage: true));
    }

    private async Task RefreshProfilesCoreAsync(bool showMessage)
    {
        var profiles = await _profileService.GetProfilesAsync();
        var previousSelectedId = SelectedProfile?.Id;

        UpdateCollection(Profiles, profiles);

        ServerProfile? profileToSelect = null;
        if (Profiles.Count > 0)
        {
            profileToSelect = !string.IsNullOrWhiteSpace(previousSelectedId)
                ? Profiles.FirstOrDefault(x => string.Equals(x.Id, previousSelectedId, StringComparison.Ordinal))
                : Profiles[0];
            profileToSelect ??= Profiles[0];
        }

        if (Profiles.Count == 0)
        {
            SelectedProfile = null;
        }
        else if (profileToSelect is not null
                 && (SelectedProfile is null || !string.Equals(SelectedProfile.Id, profileToSelect.Id, StringComparison.Ordinal)))
        {
            SelectedProfile = profileToSelect;
        }

        await RefreshInstalledVersionsCoreAsync(showMessage: false);

        if (showMessage)
        {
            SetMessage($"档案数量：{Profiles.Count}");
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

            await RefreshProfilesCoreAsync(showMessage: false);
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
            await RefreshProfilesCoreAsync(showMessage: false);
        });
    }

    private async Task LoadSelectedProfileAsync()
    {
        if (SelectedProfile is null)
        {
            UpdateCollection(Saves, []);
            SelectedSave = null;
            UpdateCollection(Mods, []);
            UpdateCollection(WorldRules, []);
            RawJsonText = string.Empty;
            ClearMapPreview();
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
                SelectedProfile.SaveDirectory = Path.GetDirectoryName(w.SaveFileLocation);
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
            SetMessage($"档案已加载：{SelectedProfile.Name}", showToast: false);
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
            SelectedProfile.SaveDirectory = Path.GetDirectoryName(SaveFileLocation);
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

    private async Task LoadLauncherSettingsAsync()
    {
        var settings = await _launcherSettingsService.LoadAsync();
        var dataDirectory = NormalizeDirectory(settings.DataDirectory, WorkspaceLayout.DefaultDataRoot);
        var saveDirectory = NormalizeDirectory(settings.SaveDirectory, WorkspaceLayout.DefaultSavesRoot);

        WorkspaceLayout.ApplyStorageSettings(dataDirectory, saveDirectory);

        LauncherDataDirectory = dataDirectory;
        LauncherSaveDirectory = saveDirectory;
        LaunchAtStartup = IsAutoStartEnabled() || settings.AutoStartWithWindows;

        ApplyVs2QQSettings(settings);
    }

    private async Task SaveLauncherSettingsAsync()
    {
        await RunBusyAsync("正在保存启动器设置...", async () =>
        {
            var dataDirectory = NormalizeDirectory(LauncherDataDirectory, WorkspaceLayout.DefaultDataRoot);
            var saveDirectory = NormalizeDirectory(LauncherSaveDirectory, WorkspaceLayout.DefaultSavesRoot);

            Directory.CreateDirectory(dataDirectory);
            Directory.CreateDirectory(saveDirectory);

            WorkspaceLayout.ApplyStorageSettings(dataDirectory, saveDirectory);

            var persistResult = await _launcherSettingsService.SaveAsync(new LauncherSettings
            {
                DataDirectory = dataDirectory,
                SaveDirectory = saveDirectory,
                AutoStartWithWindows = LaunchAtStartup,
                Vs2QQOneBotWsUrl = NormalizeVs2QQWsUrl(Vs2QQOneBotWsUrl),
                Vs2QQAccessToken = Vs2QQAccessToken?.Trim() ?? string.Empty,
                Vs2QQReconnectIntervalSec = NormalizePositiveInt(Vs2QQReconnectIntervalSec, 5),
                Vs2QQDatabasePath = NormalizeVs2QQDatabasePath(Vs2QQDatabasePath),
                Vs2QQPollIntervalSec = NormalizePositiveDouble(Vs2QQPollIntervalSec, 1.0),
                Vs2QQDefaultEncoding = NormalizeText(Vs2QQDefaultEncoding, "utf-8"),
                Vs2QQFallbackEncoding = NormalizeText(Vs2QQFallbackEncoding, "gbk"),
                Vs2QQSuperUsers = ParseSuperUsers(Vs2QQSuperUsersText)
            });

            if (!persistResult.IsSuccess)
            {
                SetMessage(persistResult.Message ?? "保存启动器设置失败。");
                return;
            }

            LauncherDataDirectory = dataDirectory;
            LauncherSaveDirectory = saveDirectory;

            var autoStartResult = TrySetAutoStart(LaunchAtStartup);
            if (!autoStartResult.IsSuccess)
            {
                SetMessage($"{persistResult.Message ?? "设置已保存。"}\n{autoStartResult.Message}");
                return;
            }

            SetMessage("设置已保存。新目录仅影响后续新建档案。");
        });
    }

    private void ChooseLauncherDataDirectory()
    {
        var dialog = new OpenFolderDialog
        {
            Title = "选择默认数据目录",
            InitialDirectory = NormalizeDirectory(LauncherDataDirectory, WorkspaceLayout.DefaultDataRoot)
        };

        if (dialog.ShowDialog() == true && !string.IsNullOrWhiteSpace(dialog.FolderName))
        {
            LauncherDataDirectory = NormalizeDirectory(dialog.FolderName, WorkspaceLayout.DefaultDataRoot);
        }
    }

    private void ChooseLauncherSaveDirectory()
    {
        var dialog = new OpenFolderDialog
        {
            Title = "选择默认存档目录",
            InitialDirectory = NormalizeDirectory(LauncherSaveDirectory, WorkspaceLayout.DefaultSavesRoot)
        };

        if (dialog.ShowDialog() == true && !string.IsNullOrWhiteSpace(dialog.FolderName))
        {
            LauncherSaveDirectory = NormalizeDirectory(dialog.FolderName, WorkspaceLayout.DefaultSavesRoot);
        }
    }

    private void ChooseVs2QQDatabasePath()
    {
        var dialog = new OpenFileDialog
        {
            Title = "选择 VS2QQ 数据库文件",
            InitialDirectory = ResolveVs2QQDatabaseDirectory(Vs2QQDatabasePath),
            Filter = "SQLite 数据库 (*.db;*.sqlite)|*.db;*.sqlite|所有文件 (*.*)|*.*",
            CheckFileExists = false
        };

        if (dialog.ShowDialog() == true && !string.IsNullOrWhiteSpace(dialog.FileName))
        {
            Vs2QQDatabasePath = NormalizeVs2QQDatabasePath(dialog.FileName);
        }
    }

    private async Task LoadVs2QQConfigAsync()
    {
        if (IsBusy)
        {
            SetMessage("当前有任务正在执行，请稍候。");
            return;
        }

        var settings = await _launcherSettingsService.LoadAsync();
        ApplyVs2QQSettings(settings);
        SetMessage("VS2QQ 配置已加载。");
    }

    private async Task SaveVs2QQConfigAsync()
    {
        await RunBusyAsync("正在保存 VS2QQ 配置...", async () =>
        {
            var validationResult = ValidateVs2QQSettings();
            if (!validationResult.IsSuccess)
            {
                SetMessage(validationResult.Message ?? "VS2QQ 配置无效。");
                return;
            }

            var persistResult = await _launcherSettingsService.SaveAsync(BuildLauncherSettingsSnapshot());

            if (!persistResult.IsSuccess)
            {
                SetMessage(persistResult.Message ?? "保存 VS2QQ 配置失败。");
                return;
            }

            SetMessage("VS2QQ 配置已保存。");
        });
    }

    private async Task StartVs2QQAsync()
    {
        await RunBusyAsync("正在启动 VS2QQ...", async () =>
        {
            var validationResult = ValidateVs2QQSettings();
            if (!validationResult.IsSuccess)
            {
                SetMessage(validationResult.Message ?? "VS2QQ 配置无效。");
                return;
            }

            var persistResult = await _launcherSettingsService.SaveAsync(BuildLauncherSettingsSnapshot());
            if (!persistResult.IsSuccess)
            {
                SetMessage(persistResult.Message ?? "启动器设置保存失败。");
                return;
            }

            var result = await _vs2qqProcessService.StartAsync(new Vs2QQLaunchSettings
            {
                OneBotWsUrl = NormalizeVs2QQWsUrl(Vs2QQOneBotWsUrl),
                AccessToken = string.IsNullOrWhiteSpace(Vs2QQAccessToken) ? null : Vs2QQAccessToken.Trim(),
                ReconnectIntervalSec = NormalizePositiveInt(Vs2QQReconnectIntervalSec, 5),
                DatabasePath = NormalizeVs2QQDatabasePath(Vs2QQDatabasePath),
                PollIntervalSec = NormalizePositiveDouble(Vs2QQPollIntervalSec, 1.0),
                DefaultEncoding = NormalizeText(Vs2QQDefaultEncoding, "utf-8"),
                FallbackEncoding = NormalizeText(Vs2QQFallbackEncoding, "gbk"),
                SuperUsers = ParseSuperUsers(Vs2QQSuperUsersText)
            });

            if (!result.IsSuccess)
            {
                var detail = BuildErrorMessage("启动 VS2QQ 失败。", result.Message, result.Exception);
                Vs2QQConsoleLines.Add($"[system] {detail}");
                TrimVs2QQConsole();
                SetMessage(detail);
                return;
            }

            SetMessage("VS2QQ 已启动。");
        });
    }

    private async Task StopVs2QQAsync()
    {
        await RunBusyAsync("正在停止 VS2QQ...", async () =>
        {
            var result = await _vs2qqProcessService.StopAsync(TimeSpan.FromSeconds(6));
            SetMessage(result.IsSuccess ? result.Message ?? "VS2QQ 已停止。" : result.Message ?? "停止 VS2QQ 失败。");
        });
    }

    private async Task RefreshSavesAsync()
    {
        if (SelectedProfile is null)
        {
            UpdateCollection(Saves, []);
            SelectedSave = null;
            return;
        }

        var saves = await _saveService.GetSavesAsync(SelectedProfile);
        UpdateCollection(Saves, saves);
        if (Saves.Count > 0)
        {
            SelectedSave = Saves.FirstOrDefault(x => x.FullPath.Equals(SelectedProfile.ActiveSaveFile, StringComparison.OrdinalIgnoreCase))
                           ?? Saves[0];
        }
        else
        {
            SelectedSave = null;
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
                SelectedProfile.SaveDirectory = Path.GetDirectoryName(result.Value);
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
                SelectedProfile.SaveDirectory = Path.GetDirectoryName(SelectedSave.FullPath);
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

    private async Task LoadMapPreviewAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        await RunBusyAsync("正在读取地图并生成预览...", async () =>
        {
            var preferredSavePath =
                !string.IsNullOrWhiteSpace(SelectedSave?.FullPath) && File.Exists(SelectedSave.FullPath) ? SelectedSave.FullPath
                : !string.IsNullOrWhiteSpace(SaveFileLocation) ? SaveFileLocation
                : SelectedProfile.ActiveSaveFile;

            var result = await _mapPreviewService.LoadMapPreviewAsync(SelectedProfile, preferredSavePath);
            if (!result.IsSuccess || result.Value is null)
            {
                var detail = BuildErrorMessage("地图预览加载失败。", result.Message, result.Exception);
                ClearMapPreview(result.Message ?? "地图预览加载失败。");
                SetMessage(detail);
                return;
            }

            var preview = result.Value;
            var colorImage = CreateBitmapSource(preview.ColorPixelsBgra32, preview.Width, preview.Height);
            var grayscaleImage = CreateBitmapSource(preview.GrayscalePixelsBgra32, preview.Width, preview.Height);
            if (colorImage is null || grayscaleImage is null)
            {
                ClearMapPreview("地图像素数据无效。");
                SetMessage("地图像素数据无效，无法显示预览。");
                return;
            }

            MapPreviewColorImage = colorImage;
            MapPreviewGrayscaleImage = grayscaleImage;
            MapPreviewSourcePath = preview.SaveFilePath;
            MapPreviewDimension = preview.Dimension;
            MapPreviewMinChunkX = preview.MinChunkX;
            MapPreviewMinChunkZ = preview.MinChunkZ;
            MapPreviewSamplingStep = Math.Max(1, preview.SamplingStep);
            MapPreviewWidth = preview.Width;
            MapPreviewHeight = preview.Height;
            MapPreviewMapSizeX = Math.Max(0, preview.MapSizeX);
            MapPreviewMapSizeZ = Math.Max(0, preview.MapSizeZ);

            var chunkCenterOffsetX = preview.MapSizeX > 0 && preview.ChunkSize > 0 ? preview.MapSizeX / (preview.ChunkSize * 2) : 0;
            var chunkCenterOffsetZ = preview.MapSizeZ > 0 && preview.ChunkSize > 0 ? preview.MapSizeZ / (preview.ChunkSize * 2) : 0;
            var worldMinChunkX = preview.MinChunkX - chunkCenterOffsetX;
            var worldMaxChunkX = preview.MaxChunkX - chunkCenterOffsetX;
            var worldMinChunkZ = preview.MinChunkZ - chunkCenterOffsetZ;
            var worldMaxChunkZ = preview.MaxChunkZ - chunkCenterOffsetZ;
            MapPreviewSummary =
                $"维度 {preview.Dimension} | 区块 {preview.ChunkCount} | X[{worldMinChunkX},{worldMaxChunkX}] Z[{worldMinChunkZ},{worldMaxChunkZ}] | 分辨率 {preview.Width}x{preview.Height} | 高度 {preview.MinTerrainHeight}-{preview.MaxTerrainHeight} | 采样 x{preview.SamplingStep}";

            SetMessage(result.Message ?? "地图预览已加载。");
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

        IsServerStartupProgressVisible = true;
        ServerStartupProgress = 5;

        var installPath = _packageService.GetInstallPath(SelectedProfile.Version);
        var serverExe = Path.Combine(installPath, "VintagestoryServer.exe");
        if (!File.Exists(serverExe))
        {
            var hint = $"未找到服务端程序：{serverExe}\n请先安装档案对应版本。";
            SetMessage(hint);
            IsServerStartupProgressVisible = false;
            ServerStartupProgress = 0;
            return;
        }

        try
        {
            await RunBusyAsync("正在启动服务器...", async () =>
            {
                ServerStartupProgress = 25;
                var result = await _serverProcessService.StartAsync(SelectedProfile);
                if (result.IsSuccess)
                {
                    ServerStartupProgress = 75;
                    SetMessage("服务器已启动。");
                }
                else
                {
                    var detail = BuildErrorMessage("启动服务器失败。", result.Message, result.Exception);
                    ConsoleLines.Add($"[system] {detail}");
                    TrimConsole();
                    SetMessage(detail);
                    ServerStartupProgress = 0;
                    return;
                }

                if (result.IsSuccess)
                {
                    await _logTailService.StartAsync(SelectedProfile);
                    ServerStartupProgress = 100;
                }
            });
        }
        finally
        {
            IsServerStartupProgressVisible = false;
        }
    }

    private async Task StopServerAsync()
    {
        await RunBusyAsync("正在停止服务器...", async () =>
        {
            var result = await _serverProcessService.StopAsync(TimeSpan.FromSeconds(12));
            await _logTailService.StopAsync();
            SetMessage(result.IsSuccess ? result.Message ?? "服务器已停止。" : result.Message ?? "停止服务器失败。");
            IsServerStartupProgressVisible = false;
            ServerStartupProgress = 0;
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

    private async void VersionListsAutoRefreshTimerOnTick(object? sender, EventArgs e)
    {
        if (!string.Equals(SelectedNavKey, "versionprofile", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        await AutoRefreshVersionListsAsync();
    }

    private async Task AutoRefreshVersionListsAsync()
    {
        if (_isAutoRefreshingVersionLists || IsBusy)
        {
            return;
        }

        _isAutoRefreshingVersionLists = true;
        try
        {
            await RefreshProfilesCoreAsync(showMessage: false);
        }
        catch
        {
            // Automatic refresh should never interrupt user operations.
        }
        finally
        {
            _isAutoRefreshingVersionLists = false;
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

    private void ClearMapPreview(string summary = "暂无地图预览。")
    {
        MapPreviewColorImage = null;
        MapPreviewGrayscaleImage = null;
        MapPreviewSourcePath = string.Empty;
        MapPreviewDimension = 0;
        MapPreviewMinChunkX = 0;
        MapPreviewMinChunkZ = 0;
        MapPreviewSamplingStep = 1;
        MapPreviewWidth = 0;
        MapPreviewHeight = 0;
        MapPreviewMapSizeX = 0;
        MapPreviewMapSizeZ = 0;
        MapPreviewSummary = summary;
    }

    private static BitmapSource? CreateBitmapSource(byte[] pixels, int width, int height)
    {
        if (width <= 0 || height <= 0)
        {
            return null;
        }

        var expectedLength = checked(width * height * 4);
        if (pixels.Length < expectedLength)
        {
            return null;
        }

        var bitmap = BitmapSource.Create(
            width,
            height,
            96,
            96,
            PixelFormats.Bgra32,
            palette: null,
            pixels,
            width * 4);
        bitmap.Freeze();
        return bitmap;
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

    private void SetMessage(string message, bool showToast = true)
    {
        LastMessage = $"[{DateTime.Now:HH:mm:ss}] {message}";
        if (showToast)
        {
            ShowToast(message);
        }
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

    private static string NormalizeDirectory(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        try
        {
            return Path.GetFullPath(value.Trim());
        }
        catch
        {
            return fallback;
        }
    }

    private bool CanSaveVs2QQSettings()
    {
        var validation = ValidateVs2QQSettings();
        return validation.IsSuccess;
    }

    private void ApplyVs2QQSettings(LauncherSettings settings)
    {
        Vs2QQOneBotWsUrl = NormalizeVs2QQWsUrl(settings.Vs2QQOneBotWsUrl);
        Vs2QQAccessToken = settings.Vs2QQAccessToken ?? string.Empty;
        Vs2QQReconnectIntervalSec = NormalizePositiveInt(settings.Vs2QQReconnectIntervalSec, 5);
        Vs2QQDatabasePath = NormalizeVs2QQDatabasePath(settings.Vs2QQDatabasePath);
        Vs2QQPollIntervalSec = NormalizePositiveDouble(settings.Vs2QQPollIntervalSec, 1.0);
        Vs2QQDefaultEncoding = NormalizeText(settings.Vs2QQDefaultEncoding, "utf-8");
        Vs2QQFallbackEncoding = NormalizeText(settings.Vs2QQFallbackEncoding, "gbk");
        Vs2QQSuperUsersText = string.Join(Environment.NewLine, (settings.Vs2QQSuperUsers ?? []).Select(x => x.ToString(CultureInfo.InvariantCulture)));
    }

    private LauncherSettings BuildLauncherSettingsSnapshot()
    {
        return new LauncherSettings
        {
            DataDirectory = NormalizeDirectory(LauncherDataDirectory, WorkspaceLayout.DefaultDataRoot),
            SaveDirectory = NormalizeDirectory(LauncherSaveDirectory, WorkspaceLayout.DefaultSavesRoot),
            AutoStartWithWindows = LaunchAtStartup,
            Vs2QQOneBotWsUrl = NormalizeVs2QQWsUrl(Vs2QQOneBotWsUrl),
            Vs2QQAccessToken = Vs2QQAccessToken?.Trim() ?? string.Empty,
            Vs2QQReconnectIntervalSec = NormalizePositiveInt(Vs2QQReconnectIntervalSec, 5),
            Vs2QQDatabasePath = NormalizeVs2QQDatabasePath(Vs2QQDatabasePath),
            Vs2QQPollIntervalSec = NormalizePositiveDouble(Vs2QQPollIntervalSec, 1.0),
            Vs2QQDefaultEncoding = NormalizeText(Vs2QQDefaultEncoding, "utf-8"),
            Vs2QQFallbackEncoding = NormalizeText(Vs2QQFallbackEncoding, "gbk"),
            Vs2QQSuperUsers = ParseSuperUsers(Vs2QQSuperUsersText)
        };
    }

    private OperationResult ValidateVs2QQSettings()
    {
        var wsUrl = NormalizeVs2QQWsUrl(Vs2QQOneBotWsUrl);
        if (!Uri.TryCreate(wsUrl, UriKind.Absolute, out var uri)
            || (uri.Scheme != Uri.UriSchemeWs && uri.Scheme != Uri.UriSchemeWss))
        {
            return OperationResult.Failed("OneBot WS 地址无效，请使用 ws:// 或 wss://。");
        }

        var dbPath = NormalizeVs2QQDatabasePath(Vs2QQDatabasePath);
        if (string.IsNullOrWhiteSpace(dbPath))
        {
            return OperationResult.Failed("数据库路径无效。");
        }

        var reconnect = NormalizePositiveInt(Vs2QQReconnectIntervalSec, 5);
        if (reconnect < 1)
        {
            return OperationResult.Failed("重连间隔必须大于 0。");
        }

        var poll = NormalizePositiveDouble(Vs2QQPollIntervalSec, 1.0);
        if (poll <= 0)
        {
            return OperationResult.Failed("轮询间隔必须大于 0。");
        }

        return OperationResult.Success();
    }

    private static List<long> ParseSuperUsers(string? rawText)
    {
        if (string.IsNullOrWhiteSpace(rawText))
        {
            return [];
        }

        var items = rawText
            .Split(['\r', '\n', ',', ';', ' '], StringSplitOptions.RemoveEmptyEntries)
            .Select(x => x.Trim())
            .Where(x => !string.IsNullOrWhiteSpace(x))
            .Select(x => long.TryParse(x, NumberStyles.Integer, CultureInfo.InvariantCulture, out var value) ? value : 0)
            .Where(x => x > 0)
            .Distinct()
            .Order()
            .ToList();

        return items;
    }

    private static string NormalizeVs2QQWsUrl(string? value)
    {
        var normalized = NormalizeText(value, "ws://127.0.0.1:3001/");
        return normalized;
    }

    private static string NormalizeVs2QQDatabasePath(string? value)
    {
        var raw = string.IsNullOrWhiteSpace(value)
            ? Path.Combine(WorkspaceLayout.WorkspaceRoot, "vs2qq", "vs2qq.db")
            : value.Trim();
        try
        {
            if (!Path.IsPathRooted(raw))
            {
                raw = Path.Combine(WorkspaceLayout.WorkspaceRoot, raw);
            }

            return Path.GetFullPath(raw);
        }
        catch
        {
            return Path.Combine(WorkspaceLayout.WorkspaceRoot, "vs2qq", "vs2qq.db");
        }
    }

    private static string ResolveVs2QQDatabaseDirectory(string? dbPath)
    {
        var normalized = NormalizeVs2QQDatabasePath(dbPath);
        var directory = Path.GetDirectoryName(normalized);
        if (!string.IsNullOrWhiteSpace(directory) && Directory.Exists(directory))
        {
            return directory;
        }

        return WorkspaceLayout.WorkspaceRoot;
    }

    private static int NormalizePositiveInt(int value, int fallback)
    {
        return value <= 0 ? fallback : value;
    }

    private static double NormalizePositiveDouble(double value, double fallback)
    {
        if (double.IsNaN(value) || double.IsInfinity(value) || value <= 0)
        {
            return fallback;
        }

        return value;
    }

    private static string NormalizeText(string? value, string fallback)
    {
        return string.IsNullOrWhiteSpace(value) ? fallback : value.Trim();
    }

    private static bool IsAutoStartEnabled()
    {
        try
        {
            using var key = Registry.CurrentUser.OpenSubKey(AutoStartRegistryKeyPath, writable: false);
            var value = key?.GetValue(AutoStartRegistryValueName) as string;
            return !string.IsNullOrWhiteSpace(value);
        }
        catch
        {
            return false;
        }
    }

    private static OperationResult TrySetAutoStart(bool enabled)
    {
        try
        {
            using var key = Registry.CurrentUser.CreateSubKey(AutoStartRegistryKeyPath);
            if (key is null)
            {
                return OperationResult.Failed("无法打开启动项注册表。");
            }

            if (enabled)
            {
                key.SetValue(AutoStartRegistryValueName, BuildAutoStartCommand(), RegistryValueKind.String);
            }
            else
            {
                key.DeleteValue(AutoStartRegistryValueName, throwOnMissingValue: false);
            }

            return OperationResult.Success();
        }
        catch (Exception ex)
        {
            return OperationResult.Failed("设置开机自启动失败。", ex);
        }
    }

    private static string BuildAutoStartCommand()
    {
        var executablePath = Environment.ProcessPath;
        if (string.IsNullOrWhiteSpace(executablePath)
            || executablePath.EndsWith("dotnet.exe", StringComparison.OrdinalIgnoreCase))
        {
            var candidate = Path.Combine(AppContext.BaseDirectory, "VSL.UI.exe");
            if (File.Exists(candidate))
            {
                executablePath = candidate;
            }
        }

        if (string.IsNullOrWhiteSpace(executablePath))
        {
            executablePath = Path.Combine(AppContext.BaseDirectory, "VSL.UI.exe");
        }

        return $"\"{executablePath}\"";
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
            if (status.IsRunning)
            {
                ServerStartupProgress = 100;
                IsServerStartupProgressVisible = false;
            }
            if (!status.IsRunning)
            {
                _ = _logTailService.StopAsync();
                ServerStartupProgress = 0;
                IsServerStartupProgressVisible = false;
            }
        });
    }

    private void OnVs2QQOutputReceived(object? sender, string line)
    {
        RunOnUi(() =>
        {
            Vs2QQConsoleLines.Add(line);
            TrimVs2QQConsole();
        });
    }

    private void OnVs2QQStatusChanged(object? sender, Vs2QQRuntimeStatus status)
    {
        RunOnUi(() => Vs2QQRuntimeStatus = status);
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

    private void TrimVs2QQConsole()
    {
        const int maxLines = 3000;
        while (Vs2QQConsoleLines.Count > maxLines)
        {
            Vs2QQConsoleLines.RemoveAt(0);
        }
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
        _refreshInstalledVersionsCommand.RaiseCanExecuteChanged();
        _installSelectedReleaseCommand.RaiseCanExecuteChanged();
        _deleteSelectedInstalledVersionCommand.RaiseCanExecuteChanged();
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
        _loadMapPreviewCommand.RaiseCanExecuteChanged();
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
        _saveLauncherSettingsCommand.RaiseCanExecuteChanged();
        _chooseLauncherDataDirectoryCommand.RaiseCanExecuteChanged();
        _chooseLauncherSaveDirectoryCommand.RaiseCanExecuteChanged();
        _loadVs2QQConfigCommand.RaiseCanExecuteChanged();
        _saveVs2QQConfigCommand.RaiseCanExecuteChanged();
        _startVs2QQCommand.RaiseCanExecuteChanged();
        _stopVs2QQCommand.RaiseCanExecuteChanged();
        _chooseVs2QQDatabasePathCommand.RaiseCanExecuteChanged();
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
