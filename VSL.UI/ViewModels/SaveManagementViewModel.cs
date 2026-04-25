using System.Collections.ObjectModel;
using System.Windows.Input;
using VSL.Application;
using VSL.Domain;
using VSL.UI.ViewModels.Messages;
using Wpf.Ui;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class SaveManagementViewModel : ObservableObjectWithMessenger
{
    private readonly ISaveService _saveService;
    private readonly IServerConfigService _serverConfigService;
    private readonly IProfileService _profileService;
    private readonly IMapPreviewService _mapPreviewService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _refreshSavesCommand;
    private readonly AsyncRelayCommand _createSaveCommand;
    private readonly AsyncRelayCommand _setActiveSaveCommand;
    private readonly AsyncRelayCommand _backupSaveCommand;
    private readonly AsyncRelayCommand _loadMapPreviewCommand;

    private bool _isBusy;
    private SaveFileEntry? _selectedSave;
    private ServerProfile? _currentProfile;
    private string _newSaveName = "default";
    private string _statusMessage = string.Empty;

    private string _mapPreviewSummary = "暂无地图预览。";
    private System.Windows.Media.Imaging.BitmapSource? _mapPreviewColorImage;
    private System.Windows.Media.Imaging.BitmapSource? _mapPreviewGrayscaleImage;
    private int _mapPreviewWidth;
    private int _mapPreviewHeight;
    private int _mapPreviewSamplingStep = 1;
    private int _mapPreviewMinChunkX;
    private int _mapPreviewMinChunkZ;
    private int _mapPreviewMapSizeX;
    private int _mapPreviewMapSizeZ;
    private int _mapPreviewDimension;

    public SaveManagementViewModel(
        ISaveService saveService,
        IServerConfigService serverConfigService,
        IProfileService profileService,
        IMapPreviewService mapPreviewService,
        ISnackbarService snackbarService)
    {
        _saveService = saveService;
        _serverConfigService = serverConfigService;
        _profileService = profileService;
        _mapPreviewService = mapPreviewService;
        _snackbarService = snackbarService;

        _refreshSavesCommand = new AsyncRelayCommand(RefreshSavesAsync, () => _currentProfile is not null);
        _createSaveCommand = new AsyncRelayCommand(CreateSaveAsync, () => _currentProfile is not null && !string.IsNullOrWhiteSpace(NewSaveName) && !IsBusy);
        _setActiveSaveCommand = new AsyncRelayCommand(SetActiveSaveAsync, () => _currentProfile is not null && SelectedSave is not null && !IsBusy);
        _backupSaveCommand = new AsyncRelayCommand(BackupSaveAsync, () => _currentProfile is not null && !IsBusy);
        _loadMapPreviewCommand = new AsyncRelayCommand(LoadMapPreviewAsync, () => _currentProfile is not null && !IsBusy);

        RegisterForMessage<ProfileSelectedMessage>(OnProfileSelected);
    }

    public ObservableCollection<SaveFileEntry> Saves { get; } = [];

    public ICommand RefreshSavesCommand => _refreshSavesCommand;
    public ICommand CreateSaveCommand => _createSaveCommand;
    public ICommand SetActiveSaveCommand => _setActiveSaveCommand;
    public ICommand BackupSaveCommand => _backupSaveCommand;
    public ICommand LoadMapPreviewCommand => _loadMapPreviewCommand;

    public bool IsBusy
    {
        get => _isBusy;
        private set => SetProperty(ref _isBusy, value);
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

    public string StatusMessage
    {
        get => _statusMessage;
        private set => SetProperty(ref _statusMessage, value);
    }

    public string MapPreviewSummary
    {
        get => _mapPreviewSummary;
        private set => SetProperty(ref _mapPreviewSummary, value);
    }

    public System.Windows.Media.Imaging.BitmapSource? MapPreviewColorImage
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

    public System.Windows.Media.Imaging.BitmapSource? MapPreviewGrayscaleImage
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

    public int MapPreviewSamplingStep
    {
        get => _mapPreviewSamplingStep;
        private set => SetProperty(ref _mapPreviewSamplingStep, value);
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

    public int MapPreviewDimension
    {
        get => _mapPreviewDimension;
        private set => SetProperty(ref _mapPreviewDimension, value);
    }

    private void OnProfileSelected(ProfileSelectedMessage msg)
    {
        _currentProfile = msg.Profile;
        if (_currentProfile is null)
        {
            Saves.Clear();
            SelectedSave = null;
            ClearMapPreview();
        }
        else
        {
            _ = RefreshSavesAsync();
        }
        UpdateCommandStates();
    }

    public async Task RefreshSavesAsync()
    {
        if (_currentProfile is null)
        {
            UpdateCollection(Saves, []);
            SelectedSave = null;
            return;
        }

        IsBusy = true;
        StatusMessage = "正在加载存档列表...";
        try
        {
            var saves = await _saveService.GetSavesAsync(_currentProfile);
            UpdateCollection(Saves, saves);

            if (Saves.Count > 0)
            {
                SelectedSave = Saves.FirstOrDefault(x => x.FullPath.Equals(_currentProfile.ActiveSaveFile, StringComparison.OrdinalIgnoreCase))
                              ?? Saves[0];
            }
            else
            {
                SelectedSave = null;
            }

            StatusMessage = $"已加载 {Saves.Count} 个存档。";
        }
        catch (Exception ex)
        {
            StatusMessage = $"加载存档失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task CreateSaveAsync()
    {
        if (_currentProfile is null)
        {
            return;
        }

        IsBusy = true;
        StatusMessage = "正在创建存档...";
        try
        {
            var result = await _saveService.CreateSaveAsync(_currentProfile, NewSaveName);
            if (result.IsSuccess)
            {
                StatusMessage = $"存档已创建：{result.Value}";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "创建存档失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }

            await RefreshSavesAsync();
        }
        catch (Exception ex)
        {
            StatusMessage = $"创建存档失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task SetActiveSaveAsync()
    {
        if (_currentProfile is null || SelectedSave is null)
        {
            return;
        }

        IsBusy = true;
        StatusMessage = "正在切换存档...";
        try
        {
            var result = await _saveService.SetActiveSaveAsync(_currentProfile, SelectedSave.FullPath);
            if (result.IsSuccess)
            {
                StatusMessage = "已切换当前存档。";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "切换存档失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }

            await RefreshSavesAsync();
        }
        catch (Exception ex)
        {
            StatusMessage = $"切换存档失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task BackupSaveAsync()
    {
        if (_currentProfile is null)
        {
            return;
        }

        IsBusy = true;
        StatusMessage = "正在备份存档...";
        try
        {
            var result = await _saveService.BackupActiveSaveAsync(_currentProfile);
            if (result.IsSuccess)
            {
                StatusMessage = $"备份完成：{result.Value}";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "备份失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"备份失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task LoadMapPreviewAsync()
    {
        if (_currentProfile is null)
        {
            return;
        }

        IsBusy = true;
        StatusMessage = "正在读取地图并生成预览...";
        try
        {
            var preferredSavePath =
                !string.IsNullOrWhiteSpace(SelectedSave?.FullPath) && System.IO.File.Exists(SelectedSave.FullPath) ? SelectedSave.FullPath
                : _currentProfile.ActiveSaveFile;

            var result = await _mapPreviewService.LoadMapPreviewAsync(_currentProfile, preferredSavePath);
            if (!result.IsSuccess || result.Value is null)
            {
                ClearMapPreview(result.Message ?? "地图预览加载失败。");
                StatusMessage = result.Message ?? "地图预览加载失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
                return;
            }

            var preview = result.Value;
            var colorImage = CreateBitmapSource(preview.ColorPixelsBgra32, preview.Width, preview.Height);
            var grayscaleImage = CreateBitmapSource(preview.GrayscalePixelsBgra32, preview.Width, preview.Height);

            if (colorImage is null || grayscaleImage is null)
            {
                ClearMapPreview("地图像素数据无效。");
                StatusMessage = "地图像素数据无效，无法显示预览。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
                return;
            }

            MapPreviewColorImage = colorImage;
            MapPreviewGrayscaleImage = grayscaleImage;
            MapPreviewWidth = preview.Width;
            MapPreviewHeight = preview.Height;
            MapPreviewSamplingStep = preview.SamplingStep;
            MapPreviewMinChunkX = preview.MinChunkX;
            MapPreviewMinChunkZ = preview.MinChunkZ;
            MapPreviewMapSizeX = preview.MapSizeX;
            MapPreviewMapSizeZ = preview.MapSizeZ;
            MapPreviewDimension = preview.Dimension;

            var chunkCenterOffsetX = preview.MapSizeX > 0 && preview.ChunkSize > 0 ? preview.MapSizeX / (preview.ChunkSize * 2) : 0;
            var chunkCenterOffsetZ = preview.MapSizeZ > 0 && preview.ChunkSize > 0 ? preview.MapSizeZ / (preview.ChunkSize * 2) : 0;
            var worldMinChunkX = preview.MinChunkX - chunkCenterOffsetX;
            var worldMaxChunkX = preview.MaxChunkX - chunkCenterOffsetX;
            var worldMinChunkZ = preview.MinChunkZ - chunkCenterOffsetZ;
            var worldMaxChunkZ = preview.MaxChunkZ - chunkCenterOffsetZ;

            MapPreviewSummary =
                $"维度 {preview.Dimension} | 区块 {preview.ChunkCount} | X[{worldMinChunkX},{worldMaxChunkX}] Z[{worldMinChunkZ},{worldMaxChunkZ}] | 分辨率 {preview.Width}x{preview.Height} | 高度 {preview.MinTerrainHeight}-{preview.MaxTerrainHeight} | 采样 x{preview.SamplingStep}";

            StatusMessage = result.Message ?? "地图预览已加载。";
            ShowToast(StatusMessage, ControlAppearance.Success);
        }
        catch (Exception ex)
        {
            StatusMessage = $"地图预览加载失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private void ClearMapPreview(string summary = "暂无地图预览。")
    {
        MapPreviewColorImage = null;
        MapPreviewGrayscaleImage = null;
        MapPreviewSummary = summary;
    }

    private static System.Windows.Media.Imaging.BitmapSource? CreateBitmapSource(byte[] pixels, int width, int height)
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

        var bitmap = System.Windows.Media.Imaging.BitmapSource.Create(
            width,
            height,
            96,
            96,
            System.Windows.Media.PixelFormats.Bgra32,
            palette: null,
            pixels,
            width * 4);
        bitmap.Freeze();
        return bitmap;
    }

    private void UpdateCommandStates()
    {
        _refreshSavesCommand.RaiseCanExecuteChanged();
        _createSaveCommand.RaiseCanExecuteChanged();
        _setActiveSaveCommand.RaiseCanExecuteChanged();
        _backupSaveCommand.RaiseCanExecuteChanged();
        _loadMapPreviewCommand.RaiseCanExecuteChanged();
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
