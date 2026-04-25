using System.Windows.Input;
using Wpf.Ui;

namespace VSL.UI.ViewModels;

public sealed class MainViewModel : ObservableObjectWithMessenger, IAsyncDisposable
{
    private readonly VersionManagementViewModel _versionManagementViewModel;
    private readonly ProfileManagementViewModel _profileManagementViewModel;
    private readonly ServerControlViewModel _serverControlViewModel;
    private readonly ModManagementViewModel _modManagementViewModel;
    private readonly SaveManagementViewModel _saveManagementViewModel;
    private readonly SettingsViewModel _settingsViewModel;
    private readonly Vs2QQRunnerViewModel _vs2QQRunnerViewModel;

    private string _selectedNavKey = "versionprofile";

    public MainViewModel(
        VersionManagementViewModel versionManagementViewModel,
        ProfileManagementViewModel profileManagementViewModel,
        ServerControlViewModel serverControlViewModel,
        ModManagementViewModel modManagementViewModel,
        SaveManagementViewModel saveManagementViewModel,
        SettingsViewModel settingsViewModel,
        Vs2QQRunnerViewModel vs2QQRunnerViewModel)
    {
        _versionManagementViewModel = versionManagementViewModel;
        _profileManagementViewModel = profileManagementViewModel;
        _serverControlViewModel = serverControlViewModel;
        _modManagementViewModel = modManagementViewModel;
        _saveManagementViewModel = saveManagementViewModel;
        _settingsViewModel = settingsViewModel;
        _vs2QQRunnerViewModel = vs2QQRunnerViewModel;
    }

    public VersionManagementViewModel VersionManagement => _versionManagementViewModel;
    public ProfileManagementViewModel ProfileManagement => _profileManagementViewModel;
    public ServerControlViewModel ServerControl => _serverControlViewModel;
    public ModManagementViewModel ModManagement => _modManagementViewModel;
    public SaveManagementViewModel SaveManagement => _saveManagementViewModel;
    public SettingsViewModel Settings => _settingsViewModel;
    public Vs2QQRunnerViewModel Vs2QQRunner => _vs2QQRunnerViewModel;

    public string SelectedNavKey
    {
        get => _selectedNavKey;
        set => SetProperty(ref _selectedNavKey, value);
    }

    public async Task InitializeAsync()
    {
        _serverControlViewModel.Initialize();
        await _settingsViewModel.LoadSettingsAsync();
        await _versionManagementViewModel.InitializeAsync();
        await _profileManagementViewModel.InitializeAsync();
    }

    public async ValueTask DisposeAsync()
    {
        _versionManagementViewModel.Cleanup();
        _profileManagementViewModel.Cleanup();
        await _serverControlViewModel.DisposeAsync();
        _modManagementViewModel.Cleanup();
        _saveManagementViewModel.Cleanup();
        _settingsViewModel.Cleanup();
        _vs2QQRunnerViewModel.Cleanup();
    }
}
