using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Input;
using Microsoft.Win32;
using VSL.Application;
using VSL.Domain;
using VSL.UI.ViewModels.Messages;
using Wpf.Ui;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class ModManagementViewModel : ObservableObjectWithMessenger
{
    private readonly IModService _modService;
    private readonly IServerConfigService _serverConfigService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _refreshModsCommand;
    private readonly AsyncRelayCommand _importModCommand;
    private readonly AsyncRelayCommand _toggleModCommand;

    private bool _isBusy;
    private ModEntry? _selectedMod;
    private ServerProfile? _currentProfile;
    private string _statusMessage = string.Empty;

    public ModManagementViewModel(
        IModService modService,
        IServerConfigService serverConfigService,
        ISnackbarService snackbarService)
    {
        _modService = modService;
        _serverConfigService = serverConfigService;
        _snackbarService = snackbarService;

        _refreshModsCommand = new AsyncRelayCommand(RefreshModsAsync, () => _currentProfile is not null);
        _importModCommand = new AsyncRelayCommand(ImportModAsync, () => _currentProfile is not null && !IsBusy);
        _toggleModCommand = new AsyncRelayCommand(ToggleModAsync, () => _currentProfile is not null && SelectedMod is not null && !IsBusy);

        RegisterForMessage<ProfileSelectedMessage>(OnProfileSelected);
    }

    public ObservableCollection<ModEntry> Mods { get; } = [];

    public ICommand RefreshModsCommand => _refreshModsCommand;
    public ICommand ImportModCommand => _importModCommand;
    public ICommand ToggleModCommand => _toggleModCommand;

    public bool IsBusy
    {
        get => _isBusy;
        private set => SetProperty(ref _isBusy, value);
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

    public string StatusMessage
    {
        get => _statusMessage;
        private set => SetProperty(ref _statusMessage, value);
    }

    private void OnProfileSelected(ProfileSelectedMessage msg)
    {
        _currentProfile = msg.Profile;
        if (_currentProfile is null)
        {
            Mods.Clear();
            SelectedMod = null;
        }
        else
        {
            _ = RefreshModsAsync();
        }
        UpdateCommandStates();
    }

    public async Task RefreshModsAsync()
    {
        if (_currentProfile is null)
        {
            UpdateCollection(Mods, []);
            SelectedMod = null;
            return;
        }

        IsBusy = true;
        StatusMessage = "正在加载 Mod 列表...";
        try
        {
            var mods = await _modService.GetModsAsync(_currentProfile);
            UpdateCollection(Mods, mods);
            SelectedMod = Mods.Count > 0 ? Mods[0] : null;
            StatusMessage = $"已加载 {Mods.Count} 个 Mod。";
        }
        catch (Exception ex)
        {
            StatusMessage = $"加载 Mod 失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task ImportModAsync()
    {
        if (_currentProfile is null)
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

        IsBusy = true;
        StatusMessage = "正在导入 Mod...";
        try
        {
            var result = await _modService.ImportModZipAsync(_currentProfile, dialog.FileName);
            if (result.IsSuccess)
            {
                StatusMessage = $"已导入 Mod: {result.Value!.ModId}";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "导入 Mod 失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }

            await RefreshModsAsync();
        }
        catch (Exception ex)
        {
            StatusMessage = $"导入 Mod 失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task ToggleModAsync()
    {
        if (_currentProfile is null || SelectedMod is null)
        {
            return;
        }

        var targetEnabled = SelectedMod.IsDisabled;
        IsBusy = true;
        StatusMessage = targetEnabled ? "正在启用 Mod..." : "正在禁用 Mod...";

        try
        {
            var result = await _modService.SetModEnabledAsync(
                _currentProfile,
                SelectedMod.ModId,
                SelectedMod.Version,
                targetEnabled);

            if (result.IsSuccess)
            {
                StatusMessage = $"{SelectedMod.ModId} 已{(targetEnabled ? "启用" : "禁用")}。";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "更新 Mod 状态失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }

            await RefreshModsAsync();
        }
        catch (Exception ex)
        {
            StatusMessage = $"更新 Mod 状态失败: {ex.Message}";
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
        _refreshModsCommand.RaiseCanExecuteChanged();
        _importModCommand.RaiseCanExecuteChanged();
        _toggleModCommand.RaiseCanExecuteChanged();
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
        UnregisterFromMessage<ProfileSelectedMessage>();
        base.Cleanup();
    }
}
