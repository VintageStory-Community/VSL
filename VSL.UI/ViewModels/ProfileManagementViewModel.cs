using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Input;
using VSL.Application;
using VSL.Domain;
using VSL.UI.ViewModels.Messages;
using Wpf.Ui;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class ProfileManagementViewModel : ObservableObjectWithMessenger
{
    private readonly IProfileService _profileService;
    private readonly IServerConfigService _serverConfigService;
    private readonly IPackageService _packageService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _refreshProfilesCommand;
    private readonly AsyncRelayCommand _createProfileCommand;
    private readonly AsyncRelayCommand _deleteProfileCommand;

    private bool _isBusy;
    private ServerProfile? _selectedProfile;
    private string _newProfileName = "我的服务器";
    private string _statusMessage = string.Empty;
    private bool _includeUnstable;

    public ProfileManagementViewModel(
        IProfileService profileService,
        IServerConfigService serverConfigService,
        IPackageService packageService,
        ISnackbarService snackbarService)
    {
        _profileService = profileService;
        _serverConfigService = serverConfigService;
        _packageService = packageService;
        _snackbarService = snackbarService;

        _refreshProfilesCommand = new AsyncRelayCommand(RefreshProfilesAsync);
        _createProfileCommand = new AsyncRelayCommand(CreateProfileAsync, CanCreateProfile);
        _deleteProfileCommand = new AsyncRelayCommand(DeleteProfileAsync, () => SelectedProfile is not null && !IsBusy);

        RegisterForMessage<VersionListChangedMessage>(async _ => await RefreshProfilesAsync());
    }

    public ObservableCollection<ServerProfile> Profiles { get; } = [];
    public ObservableCollection<ServerRelease> AvailableReleases { get; } = [];

    public ICommand RefreshProfilesCommand => _refreshProfilesCommand;
    public ICommand CreateProfileCommand => _createProfileCommand;
    public ICommand DeleteProfileCommand => _deleteProfileCommand;

    public bool IsBusy
    {
        get => _isBusy;
        private set => SetProperty(ref _isBusy, value);
    }

    public ServerProfile? SelectedProfile
    {
        get => _selectedProfile;
        set
        {
            if (SetProperty(ref _selectedProfile, value))
            {
                UpdateCommandStates();
                SendMessage(new ProfileSelectedMessage(value));
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

    public string StatusMessage
    {
        get => _statusMessage;
        private set => SetProperty(ref _statusMessage, value);
    }

    public bool IncludeUnstable
    {
        get => _includeUnstable;
        set => SetProperty(ref _includeUnstable, value);
    }

    public async Task InitializeAsync()
    {
        await RefreshProfilesAsync();
    }

    public void SetAvailableReleases(IEnumerable<ServerRelease> releases)
    {
        AvailableReleases.Clear();
        var filtered = IncludeUnstable ? releases : releases.Where(x => x.Channel == ReleaseChannel.Stable);
        foreach (var release in filtered)
        {
            AvailableReleases.Add(release);
        }
    }

    public async Task RefreshProfilesAsync()
    {
        IsBusy = true;
        StatusMessage = "正在加载档案...";
        try
        {
            var profiles = await _profileService.GetProfilesAsync();
            var previousSelectedId = SelectedProfile?.Id;

            UpdateCollection(Profiles, profiles);

            if (Profiles.Count > 0)
            {
                ServerProfile? profileToSelect;
                if (!string.IsNullOrWhiteSpace(previousSelectedId))
                {
                    profileToSelect = Profiles.FirstOrDefault(x => string.Equals(x.Id, previousSelectedId, StringComparison.Ordinal));
                }
                else
                {
                    profileToSelect = Profiles[0];
                }

                SelectedProfile ??= profileToSelect;
            }
            else
            {
                SelectedProfile = null;
            }

            StatusMessage = $"档案数量：{Profiles.Count}";
        }
        catch (Exception ex)
        {
            StatusMessage = $"加载档案失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private bool CanCreateProfile()
    {
        return AvailableReleases.Count > 0 && !string.IsNullOrWhiteSpace(NewProfileName) && !IsBusy;
    }

    private async Task CreateProfileAsync()
    {
        var selectedRelease = AvailableReleases.FirstOrDefault();
        if (selectedRelease is null)
        {
            StatusMessage = "请先选择要使用的服务器版本。";
            ShowToast(StatusMessage, ControlAppearance.Secondary);
            return;
        }

        IsBusy = true;
        StatusMessage = "正在创建档案...";
        try
        {
            var result = await _profileService.CreateProfileAsync(NewProfileName, selectedRelease.Version);
            if (!result.IsSuccess || result.Value is null)
            {
                StatusMessage = result.Message ?? "创建档案失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
                return;
            }

            StatusMessage = $"档案创建成功：{result.Value.Name}";
            ShowToast(StatusMessage, ControlAppearance.Success);
            await RefreshProfilesAsync();
            SelectedProfile = Profiles.FirstOrDefault(x => x.Id == result.Value.Id);
        }
        catch (Exception ex)
        {
            StatusMessage = $"创建档案失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task DeleteProfileAsync()
    {
        if (SelectedProfile is null)
        {
            return;
        }

        if (MessageBox.Show(
                $"确认删除档案“{SelectedProfile.Name}”？此操作会删除档案数据目录。",
                "确认删除",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning) != MessageBoxResult.Yes)
        {
            return;
        }

        IsBusy = true;
        StatusMessage = "正在删除档案...";
        try
        {
            var result = await _profileService.DeleteProfileAsync(SelectedProfile.Id);
            if (result.IsSuccess)
            {
                StatusMessage = "档案已删除。";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "删除失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }

            SelectedProfile = null;
            await RefreshProfilesAsync();
        }
        catch (Exception ex)
        {
            StatusMessage = $"删除失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private void UpdateCommandStates()
    {
        _createProfileCommand.RaiseCanExecuteChanged();
        _deleteProfileCommand.RaiseCanExecuteChanged();
    }

    private void ShowToast(string message, ControlAppearance appearance)
    {
        _snackbarService.Show("操作提示", message, appearance, null, TimeSpan.FromSeconds(3));
    }

    private static void UpdateCollection<T>(ObservableCollection<T> collection, IEnumerable<T> items)
    {
        collection.Clear();
        foreach (var item in items)
        {
            collection.Add(item);
        }
    }

    public override void Cleanup()
    {
        UnregisterFromMessage<VersionListChangedMessage>();
        base.Cleanup();
    }
}
