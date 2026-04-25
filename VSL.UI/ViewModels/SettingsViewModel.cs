using System.Globalization;
using System.Windows.Input;
using Microsoft.Win32;
using VSL.Application;
using VSL.Domain;
using Wpf.Ui;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class SettingsViewModel : ObservableObjectWithMessenger
{
    private readonly ILauncherSettingsService _launcherSettingsService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _saveSettingsCommand;
    private readonly RelayCommand _chooseDataDirectoryCommand;
    private readonly RelayCommand _chooseSaveDirectoryCommand;

    private bool _isBusy;
    private string _launcherDataDirectory = WorkspaceLayout.DefaultDataRoot;
    private string _launcherSaveDirectory = WorkspaceLayout.DefaultSavesRoot;
    private bool _launchAtStartup;
    private string _statusMessage = string.Empty;

    public SettingsViewModel(
        ILauncherSettingsService launcherSettingsService,
        ISnackbarService snackbarService)
    {
        _launcherSettingsService = launcherSettingsService;
        _snackbarService = snackbarService;

        _saveSettingsCommand = new AsyncRelayCommand(SaveSettingsAsync, () => !IsBusy);
        _chooseDataDirectoryCommand = new RelayCommand(ChooseDataDirectory, () => !IsBusy);
        _chooseSaveDirectoryCommand = new RelayCommand(ChooseSaveDirectory, () => !IsBusy);
    }

    public ICommand SaveSettingsCommand => _saveSettingsCommand;
    public ICommand ChooseDataDirectoryCommand => _chooseDataDirectoryCommand;
    public ICommand ChooseSaveDirectoryCommand => _chooseSaveDirectoryCommand;

    public bool IsBusy
    {
        get => _isBusy;
        private set => SetProperty(ref _isBusy, value);
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

    public string StatusMessage
    {
        get => _statusMessage;
        private set => SetProperty(ref _statusMessage, value);
    }

    public async Task LoadSettingsAsync()
    {
        IsBusy = true;
        StatusMessage = "正在加载设置...";
        try
        {
            var settings = await _launcherSettingsService.LoadAsync();
            var dataDirectory = NormalizeDirectory(settings.DataDirectory, WorkspaceLayout.DefaultDataRoot);
            var saveDirectory = NormalizeDirectory(settings.SaveDirectory, WorkspaceLayout.DefaultSavesRoot);

            WorkspaceLayout.ApplyStorageSettings(dataDirectory, saveDirectory);

            LauncherDataDirectory = dataDirectory;
            LauncherSaveDirectory = saveDirectory;
            LaunchAtStartup = IsAutoStartEnabled() || settings.AutoStartWithWindows;

            StatusMessage = "设置已加载。";
        }
        catch (Exception ex)
        {
            StatusMessage = $"加载设置失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
        }
    }

    private async Task SaveSettingsAsync()
    {
        IsBusy = true;
        StatusMessage = "正在保存设置...";
        try
        {
            var dataDirectory = NormalizeDirectory(LauncherDataDirectory, WorkspaceLayout.DefaultDataRoot);
            var saveDirectory = NormalizeDirectory(LauncherSaveDirectory, WorkspaceLayout.DefaultSavesRoot);

            System.IO.Directory.CreateDirectory(dataDirectory);
            System.IO.Directory.CreateDirectory(saveDirectory);

            WorkspaceLayout.ApplyStorageSettings(dataDirectory, saveDirectory);

            var settings = new LauncherSettings
            {
                DataDirectory = dataDirectory,
                SaveDirectory = saveDirectory,
                AutoStartWithWindows = LaunchAtStartup
            };

            var result = await _launcherSettingsService.SaveAsync(settings);
            if (!result.IsSuccess)
            {
                StatusMessage = result.Message ?? "保存设置失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
                IsBusy = false;
                return;
            }

            LauncherDataDirectory = dataDirectory;
            LauncherSaveDirectory = saveDirectory;

            var autoStartResult = TrySetAutoStart(LaunchAtStartup);
            if (!autoStartResult.IsSuccess)
            {
                StatusMessage = $"{result.Message ?? "设置已保存。"}\n{autoStartResult.Message}";
                ShowToast(StatusMessage, ControlAppearance.Secondary);
            }
            else
            {
                StatusMessage = "设置已保存。新目录仅影响后续新建档案。";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"保存设置失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
        }
    }

    private void ChooseDataDirectory()
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

    private void ChooseSaveDirectory()
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

    private static string NormalizeDirectory(string? value, string fallback)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return fallback;
        }

        try
        {
            return System.IO.Path.GetFullPath(value.Trim());
        }
        catch
        {
            return fallback;
        }
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
            return OperationResult.Failed($"设置开机自启失败: {ex.Message}");
        }
    }

    private static string BuildAutoStartCommand()
    {
        var exePath = Environment.ProcessPath;
        if (string.IsNullOrWhiteSpace(exePath))
        {
            return string.Empty;
        }

        return $"\"{exePath}\"";
    }

    private const string AutoStartRegistryKeyPath = @"Software\Microsoft\Windows\CurrentVersion\Run";
    private const string AutoStartRegistryValueName = "VSL";

    private void ShowToast(string message, ControlAppearance appearance)
    {
        _snackbarService.Show("操作提示", message, appearance, null, TimeSpan.FromSeconds(3));
    }
}
