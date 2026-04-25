using System.Collections.ObjectModel;
using System.Windows;
using System.Windows.Input;
using Microsoft.Win32;
using VSL.Application;
using VSL.Domain;
using Wpf.Ui;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class VersionManagementViewModel : ObservableObjectWithMessenger
{
    private readonly IVersionCatalogService _versionCatalogService;
    private readonly IPackageService _packageService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _refreshReleasesCommand;
    private readonly AsyncRelayCommand _importLocalZipCommand;
    private readonly AsyncRelayCommand _installSelectedCommand;
    private readonly AsyncRelayCommand _deleteSelectedCommand;

    private bool _includeUnstable;
    private bool _isBusy;
    private bool _isInstallProgressVisible;
    private double _installProgress;
    private ServerRelease? _selectedRelease;
    private InstalledVersionItemViewModel? _selectedInstalledVersion;
    private string _statusMessage = string.Empty;

    public VersionManagementViewModel(
        IVersionCatalogService versionCatalogService,
        IPackageService packageService,
        ISnackbarService snackbarService)
    {
        _versionCatalogService = versionCatalogService;
        _packageService = packageService;
        _snackbarService = snackbarService;

        _refreshReleasesCommand = new AsyncRelayCommand(RefreshReleasesAsync);
        _importLocalZipCommand = new AsyncRelayCommand(ImportLocalZipAsync);
        _installSelectedCommand = new AsyncRelayCommand(InstallSelectedAsync, () => SelectedRelease is not null && !IsBusy);
        _deleteSelectedCommand = new AsyncRelayCommand(DeleteSelectedAsync, () => SelectedInstalledVersion is not null && !IsBusy);
    }

    public ObservableCollection<ServerRelease> Releases { get; } = [];
    public ObservableCollection<InstalledVersionItemViewModel> InstalledVersions { get; } = [];

    public ICommand RefreshReleasesCommand => _refreshReleasesCommand;
    public ICommand ImportLocalZipCommand => _importLocalZipCommand;
    public ICommand InstallSelectedCommand => _installSelectedCommand;
    public ICommand DeleteSelectedCommand => _deleteSelectedCommand;

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

    public bool IsInstallProgressVisible
    {
        get => _isInstallProgressVisible;
        private set => SetProperty(ref _isInstallProgressVisible, value);
    }

    public double InstallProgress
    {
        get => _installProgress;
        private set => SetProperty(ref _installProgress, value);
    }

    public string StatusMessage
    {
        get => _statusMessage;
        private set => SetProperty(ref _statusMessage, value);
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

    public string InstallProgressText =>
        IsInstallProgressVisible ? $"安装进度：{InstallProgress:0}%" : string.Empty;

    public async Task InitializeAsync()
    {
        await RefreshReleasesAsync();
    }

    public async Task RefreshReleasesAsync()
    {
        IsBusy = true;
        StatusMessage = "正在刷新版本列表...";
        try
        {
            var official = await _versionCatalogService.GetOfficialReleasesAsync();
            var locals = await _versionCatalogService.GetLocalReleasesAsync();
            var filteredOfficial = official.Where(x => IncludeUnstable || x.Channel == ReleaseChannel.Stable);
            var merged = filteredOfficial.Concat(locals).ToList();

            UpdateCollection(Releases, merged);
            await RefreshInstalledVersionsCoreAsync();

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

            StatusMessage = $"已加载版本：{Releases.Count} 个。";
        }
        catch (Exception ex)
        {
            StatusMessage = $"加载版本失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task RefreshInstalledVersionsCoreAsync()
    {
        var installedVersions = await _packageService.GetInstalledVersionsAsync();
        var items = installedVersions
            .Select(version => new InstalledVersionItemViewModel
            {
                Version = version,
                InstallPath = _packageService.GetInstallPath(version),
                ProfileCount = 0
            })
            .OrderByDescending(x => x.Version, StringComparer.OrdinalIgnoreCase)
            .ToList();

        UpdateCollection(InstalledVersions, items);

        if (SelectedInstalledVersion is not null)
        {
            SelectedInstalledVersion = InstalledVersions.FirstOrDefault(
                x => string.Equals(x.Version, SelectedInstalledVersion.Version, StringComparison.OrdinalIgnoreCase));
        }
        else if (InstalledVersions.Count > 0)
        {
            SelectedInstalledVersion = InstalledVersions[0];
        }
    }

    private async Task ImportLocalZipAsync()
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
        IsBusy = true;

        try
        {
            StatusMessage = "正在导入本地版本包...";
            var progress = new Progress<double>(v => InstallProgress = v * 100.0);
            var result = await _packageService.ImportLocalZipAsync(dialog.FileName, progress);

            if (!result.IsSuccess)
            {
                StatusMessage = result.Message ?? "导入失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
                return;
            }

            StatusMessage = $"已导入本地版本：{result.Value!.Version}";
            ShowToast(StatusMessage, ControlAppearance.Success);
            await RefreshReleasesAsync();
            SelectedRelease = Releases.FirstOrDefault(x => x.Version == result.Value.Version && x.Source == ReleaseSource.LocalImport)
                ?? SelectedRelease;
        }
        catch (Exception ex)
        {
            StatusMessage = $"导入失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsInstallProgressVisible = false;
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task InstallSelectedAsync()
    {
        if (SelectedRelease is null)
        {
            return;
        }

        IsInstallProgressVisible = true;
        InstallProgress = 0;
        IsBusy = true;
        StatusMessage = $"正在安装版本 {SelectedRelease.Version}...";

        try
        {
            InstallProgress = 3;
            var progress = new Progress<double>(v => InstallProgress = v * 100.0);
            var result = await _packageService.InstallReleaseAsync(SelectedRelease, progress);

            if (result.IsSuccess)
            {
                InstallProgress = 100;
                StatusMessage = $"版本安装完成: {SelectedRelease.Version}";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "安装失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }

            await RefreshReleasesAsync();
        }
        catch (Exception ex)
        {
            StatusMessage = $"安装失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsInstallProgressVisible = false;
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task DeleteSelectedAsync()
    {
        if (SelectedInstalledVersion is null)
        {
            return;
        }

        var version = SelectedInstalledVersion.Version;

        if (MessageBox.Show(
                $"确认删除已安装版本“{version}”？\n目录：{SelectedInstalledVersion.InstallPath}",
                "确认删除版本",
                MessageBoxButton.YesNo,
                MessageBoxImage.Warning) != MessageBoxResult.Yes)
        {
            return;
        }

        IsBusy = true;
        StatusMessage = $"正在删除版本 {version}...";

        try
        {
            var result = await _packageService.DeleteInstalledVersionAsync(version);
            if (result.IsSuccess)
            {
                StatusMessage = $"已删除版本：{version}";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "删除失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }

            await RefreshReleasesAsync();
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
        _installSelectedCommand.RaiseCanExecuteChanged();
        _deleteSelectedCommand.RaiseCanExecuteChanged();
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
}
