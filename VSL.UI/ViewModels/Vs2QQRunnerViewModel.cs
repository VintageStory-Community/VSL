using System.Collections.ObjectModel;
using System.Windows.Input;
using VSL.Application;
using VSL.Domain;
using VSL.UI.ViewModels.Messages;
using Wpf.Ui;
using ControlAppearance = Wpf.Ui.Controls.ControlAppearance;

namespace VSL.UI.ViewModels;

public sealed class Vs2QQRunnerViewModel : ObservableObjectWithMessenger
{
    private readonly IVs2QQProcessService _vs2QQProcessService;
    private readonly ISnackbarService _snackbarService;

    private readonly AsyncRelayCommand _startVs2QQCommand;
    private readonly AsyncRelayCommand _stopVs2QQCommand;
    private readonly RelayCommand _clearConsoleCommand;

    private string _vs2QQRuntimeStateText = "未运行";
    private bool _isVs2QQConsoleAutoFollow = true;
    private bool _isRunning;

    public Vs2QQRunnerViewModel(
        IVs2QQProcessService vs2QQProcessService,
        ISnackbarService snackbarService)
    {
        _vs2QQProcessService = vs2QQProcessService;
        _snackbarService = snackbarService;

        _startVs2QQCommand = new AsyncRelayCommand(StartVs2QQAsync, () => !_isRunning);
        _stopVs2QQCommand = new AsyncRelayCommand(StopVs2QQAsync, () => _isRunning);
        _clearConsoleCommand = new RelayCommand(() => Vs2QQConsoleLines.Clear());

        _vs2QQProcessService.OutputReceived += OnOutputReceived;
        _vs2QQProcessService.StatusChanged += OnStatusChanged;

        UpdateStatus(_vs2QQProcessService.CurrentStatus);
    }

    public ObservableCollection<string> Vs2QQConsoleLines { get; } = [];

    public string Vs2QQRuntimeStateText
    {
        get => _vs2QQRuntimeStateText;
        private set => SetProperty(ref _vs2QQRuntimeStateText, value);
    }

    public bool IsVs2QQConsoleAutoFollow
    {
        get => _isVs2QQConsoleAutoFollow;
        set => SetProperty(ref _isVs2QQConsoleAutoFollow, value);
    }

    public ICommand StartVs2QQCommand => _startVs2QQCommand;
    public ICommand StopVs2QQCommand => _stopVs2QQCommand;
    public ICommand ClearVs2QQConsoleCommand => _clearConsoleCommand;

    private void OnOutputReceived(object? sender, string line)
    {
        System.Windows.Application.Current.Dispatcher.BeginInvoke(() =>
        {
            Vs2QQConsoleLines.Add(line);
            while (Vs2QQConsoleLines.Count > 1000)
            {
                Vs2QQConsoleLines.RemoveAt(0);
            }
        });
    }

    private void OnStatusChanged(object? sender, Vs2QQRuntimeStatus status)
    {
        System.Windows.Application.Current.Dispatcher.BeginInvoke(() =>
        {
            UpdateStatus(status);
            SendMessage(new Vs2QQStatusChangedMessage(status));
        });
    }

    private void UpdateStatus(Vs2QQRuntimeStatus status)
    {
        _isRunning = status.IsRunning;
        Vs2QQRuntimeStateText = status.IsRunning
            ? $"运行中 (PID: {status.ProcessId})"
            : "未运行";

        _startVs2QQCommand.RaiseCanExecuteChanged();
        _stopVs2QQCommand.RaiseCanExecuteChanged();
    }

    private async Task StartVs2QQAsync()
    {
        var result = await _vs2QQProcessService.StartAsync(new Vs2QQLaunchSettings());
        if (result.IsSuccess)
        {
            _snackbarService.Show("Vs2QQ", "Vs2QQ 机器人已启动", ControlAppearance.Success, null, TimeSpan.FromSeconds(3));
        }
        else
        {
            _snackbarService.Show("Vs2QQ", result.Message ?? "启动失败", ControlAppearance.Danger, null, TimeSpan.FromSeconds(3));
        }
    }

    private async Task StopVs2QQAsync()
    {
        var result = await _vs2QQProcessService.StopAsync(TimeSpan.FromSeconds(10));
        if (result.IsSuccess)
        {
            _snackbarService.Show("Vs2QQ", "Vs2QQ 机器人已停止", ControlAppearance.Success, null, TimeSpan.FromSeconds(3));
        }
        else
        {
            _snackbarService.Show("Vs2QQ", result.Message ?? "停止失败", ControlAppearance.Danger, null, TimeSpan.FromSeconds(3));
        }
    }

    public override void Cleanup()
    {
        _vs2QQProcessService.OutputReceived -= OnOutputReceived;
        _vs2QQProcessService.StatusChanged -= OnStatusChanged;
        base.Cleanup();
    }
}
