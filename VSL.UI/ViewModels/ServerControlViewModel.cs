using System.Collections.ObjectModel;
using System.Text;
using System.Windows.Input;
using VSL.Application;
using VSL.Domain;
using VSL.UI.ViewModels.Messages;
using Wpf.Ui;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class ServerControlViewModel : ObservableObjectWithMessenger
{
    private readonly IServerProcessService _serverProcessService;
    private readonly ILogTailService _logTailService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _startServerCommand;
    private readonly AsyncRelayCommand _stopServerCommand;
    private readonly AsyncRelayCommand _sendConsoleCommand;
    private readonly RelayCommand _clearConsoleCommand;
    private readonly AsyncRelayCommand _downloadConsoleLogCommand;

    private ServerRuntimeStatus _runtimeStatus = new();
    private string _consoleInput = string.Empty;
    private bool _isBusy;
    private bool _isServerStartupProgressVisible;
    private double _serverStartupProgress;
    private string _statusMessage = string.Empty;
    private bool _isAutoFollow = true;
    private ServerProfile? _currentProfile;

    public ServerControlViewModel(
        IServerProcessService serverProcessService,
        ILogTailService logTailService,
        ISnackbarService snackbarService)
    {
        _serverProcessService = serverProcessService;
        _logTailService = logTailService;
        _snackbarService = snackbarService;

        _startServerCommand = new AsyncRelayCommand(StartServerAsync, () => !RuntimeStatus.IsRunning && !IsBusy);
        _stopServerCommand = new AsyncRelayCommand(StopServerAsync, () => RuntimeStatus.IsRunning && !IsBusy);
        _sendConsoleCommand = new AsyncRelayCommand(SendConsoleCommandAsync, () => RuntimeStatus.IsRunning && !string.IsNullOrWhiteSpace(ConsoleInput) && !IsBusy);
        _clearConsoleCommand = new RelayCommand(() => ConsoleLines.Clear());
        _downloadConsoleLogCommand = new AsyncRelayCommand(DownloadConsoleLogAsync, () => ConsoleLines.Count > 0);

        _serverProcessService.OutputReceived += OnProcessOutputReceived;
        _serverProcessService.StatusChanged += OnProcessStatusChanged;
        _logTailService.LogLineReceived += OnLogTailLineReceived;

        RegisterForMessage<ProfileSelectedMessage>(OnProfileSelected);
    }

    public ObservableCollection<string> ConsoleLines { get; } = [];

    public ICommand StartServerCommand => _startServerCommand;
    public ICommand StopServerCommand => _stopServerCommand;
    public ICommand SendConsoleCommandCommand => _sendConsoleCommand;
    public ICommand ClearConsoleCommand => _clearConsoleCommand;
    public ICommand DownloadConsoleLogCommand => _downloadConsoleLogCommand;

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

    public bool IsBusy
    {
        get => _isBusy;
        private set => SetProperty(ref _isBusy, value);
    }

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
        IsServerStartupProgressVisible ? $"启动进度：{ServerStartupProgress:0}%" : string.Empty;

    public string StatusMessage
    {
        get => _statusMessage;
        private set => SetProperty(ref _statusMessage, value);
    }

    public bool IsAutoFollow
    {
        get => _isAutoFollow;
        set => SetProperty(ref _isAutoFollow, value);
    }

    public void Initialize()
    {
        RuntimeStatus = _serverProcessService.CurrentStatus;
    }

    private void OnProfileSelected(ProfileSelectedMessage msg)
    {
        _currentProfile = msg.Profile;
    }

    private void OnProcessOutputReceived(object? sender, string line)
    {
        AddConsoleLine(line);
    }

    private void OnProcessStatusChanged(object? sender, ServerRuntimeStatus status)
    {
        RuntimeStatus = status;
        SendMessage(new ServerStatusChangedMessage(status));
    }

    private void OnLogTailLineReceived(object? sender, string line)
    {
        AddConsoleLine(line);
    }

    private void AddConsoleLine(string line)
    {
        System.Windows.Application.Current?.Dispatcher.Invoke(() =>
        {
            ConsoleLines.Add(line);
            TrimConsole();
        });
    }

    private void TrimConsole()
    {
        while (ConsoleLines.Count > 1000)
        {
            ConsoleLines.RemoveAt(0);
        }
    }

    private async Task StartServerAsync()
    {
        if (_currentProfile is null)
        {
            StatusMessage = "请先在档案管理中选择一个服务器档案。";
            ShowToast(StatusMessage, ControlAppearance.Secondary);
            return;
        }

        var installPath = System.IO.Path.Combine(
            VSL.Domain.WorkspaceLayout.GetServerInstallPath(_currentProfile.Version),
            "VintagestoryServer.exe");
        if (!System.IO.File.Exists(installPath))
        {
            StatusMessage = $"未找到服务端程序：{installPath}\n请先安装档案对应版本。";
            ShowToast(StatusMessage, ControlAppearance.Danger);
            return;
        }

        IsServerStartupProgressVisible = true;
        ServerStartupProgress = 5;
        IsBusy = true;
        StatusMessage = "正在启动服务器...";

        try
        {
            ServerStartupProgress = 25;
            var result = await _serverProcessService.StartAsync(_currentProfile);
            if (!result.IsSuccess)
            {
                var detail = BuildErrorMessage("启动服务器失败。", result.Message, result.Exception);
                AddConsoleLine($"[system] {detail}");
                StatusMessage = detail;
                ShowToast(detail, ControlAppearance.Danger);
                ServerStartupProgress = 0;
                return;
            }

            ServerStartupProgress = 75;
            await _logTailService.StartAsync(_currentProfile);
            ServerStartupProgress = 100;
            StatusMessage = "服务器已启动。";
            ShowToast(StatusMessage, ControlAppearance.Success);
        }
        catch (Exception ex)
        {
            var detail = $"启动服务器失败: {ex.Message}";
            AddConsoleLine($"[system] {detail}");
            StatusMessage = detail;
            ShowToast(detail, ControlAppearance.Danger);
            ServerStartupProgress = 0;
        }
        finally
        {
            IsServerStartupProgressVisible = false;
            IsBusy = false;
            UpdateCommandStates();
        }
    }

    private async Task StopServerAsync()
    {
        IsBusy = true;
        StatusMessage = "正在停止服务器...";

        try
        {
            var result = await _serverProcessService.StopAsync(TimeSpan.FromSeconds(12));
            await _logTailService.StopAsync();

            if (result.IsSuccess)
            {
                StatusMessage = result.Message ?? "服务器已停止。";
                ShowToast(StatusMessage, ControlAppearance.Success);
            }
            else
            {
                StatusMessage = result.Message ?? "停止服务器失败。";
                ShowToast(StatusMessage, ControlAppearance.Danger);
            }
        }
        catch (Exception ex)
        {
            StatusMessage = $"停止服务器失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
        finally
        {
            IsBusy = false;
            ServerStartupProgress = 0;
            UpdateCommandStates();
        }
    }

    private async Task SendConsoleCommandAsync()
    {
        var text = ConsoleInput;
        if (string.IsNullOrWhiteSpace(text))
        {
            return;
        }

        var result = await _serverProcessService.SendCommandAsync(text);
        if (result.IsSuccess)
        {
            ConsoleInput = string.Empty;
        }
        else
        {
            StatusMessage = result.Message ?? "发送命令失败。";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
    }

    private async Task DownloadConsoleLogAsync()
    {
        if (ConsoleLines.Count == 0)
        {
            StatusMessage = "当前没有日志可下载。";
            return;
        }

        var dialog = new Microsoft.Win32.SaveFileDialog
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
            await System.IO.File.WriteAllLinesAsync(dialog.FileName, lines, new UTF8Encoding(encoderShouldEmitUTF8Identifier: false));
            StatusMessage = $"日志已下载：{dialog.FileName}";
            ShowToast(StatusMessage, ControlAppearance.Success);
        }
        catch (Exception ex)
        {
            StatusMessage = $"下载日志失败: {ex.Message}";
            ShowToast(StatusMessage, ControlAppearance.Danger);
        }
    }

    private void UpdateCommandStates()
    {
        _startServerCommand.RaiseCanExecuteChanged();
        _stopServerCommand.RaiseCanExecuteChanged();
        _sendConsoleCommand.RaiseCanExecuteChanged();
        _downloadConsoleLogCommand.RaiseCanExecuteChanged();
    }

    private void ShowToast(string message, ControlAppearance appearance)
    {
        _snackbarService.Show("操作提示", message, appearance, null, TimeSpan.FromSeconds(3));
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

    public async ValueTask DisposeAsync()
    {
        _serverProcessService.OutputReceived -= OnProcessOutputReceived;
        _serverProcessService.StatusChanged -= OnProcessStatusChanged;
        _logTailService.LogLineReceived -= OnLogTailLineReceived;
        await _logTailService.StopAsync();
    }

    public override void Cleanup()
    {
        UnregisterFromMessage<ProfileSelectedMessage>();
        base.Cleanup();
    }
}
