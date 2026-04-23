using System.Diagnostics;
using VSL.Application;
using VSL.Domain;

namespace VSL.Infrastructure.Services;

public sealed class ServerProcessService(IPackageService packageService) : IServerProcessService
{
    private readonly SemaphoreSlim _processGate = new(1, 1);
    private Process? _process;
    private ServerProfile? _currentProfile;

    public event EventHandler<string>? OutputReceived;

    public event EventHandler<ServerRuntimeStatus>? StatusChanged;

    public ServerRuntimeStatus CurrentStatus { get; private set; } = new();

    public async Task<OperationResult> StartAsync(ServerProfile profile, CancellationToken cancellationToken = default)
    {
        await _processGate.WaitAsync(cancellationToken);
        try
        {
            if (_process is { HasExited: false })
            {
                return OperationResult.Failed("服务器已在运行中。");
            }

            var installPath = packageService.GetInstallPath(profile.Version);
            var serverExe = Path.Combine(installPath, "VintagestoryServer.exe");
            if (!File.Exists(serverExe))
            {
                return OperationResult.Failed($"未找到服务端程序: {serverExe}");
            }

            Directory.CreateDirectory(profile.DataPath);
            var writableCheck = EnsureProfileWritable(profile);
            if (!writableCheck.IsSuccess)
            {
                return writableCheck;
            }

            var process = new Process
            {
                StartInfo = new ProcessStartInfo
                {
                    FileName = serverExe,
                    WorkingDirectory = installPath,
                    Arguments = $"--dataPath \"{profile.DataPath}\"",
                    RedirectStandardOutput = true,
                    RedirectStandardError = true,
                    RedirectStandardInput = true,
                    CreateNoWindow = true,
                    UseShellExecute = false
                },
                EnableRaisingEvents = true
            };

            process.OutputDataReceived += OnOutputDataReceived;
            process.ErrorDataReceived += OnOutputDataReceived;
            process.Exited += OnProcessExited;

            var started = process.Start();
            if (!started)
            {
                return OperationResult.Failed("启动服务端失败。");
            }

            process.BeginOutputReadLine();
            process.BeginErrorReadLine();

            _process = process;
            _currentProfile = profile;
            CurrentStatus = new ServerRuntimeStatus
            {
                IsRunning = true,
                ProcessId = process.Id,
                StartedAtUtc = DateTimeOffset.UtcNow,
                ProfileId = profile.Id
            };
            StatusChanged?.Invoke(this, CurrentStatus);
            OutputReceived?.Invoke(this, $"[system] 服务器进程已启动，PID={process.Id}");

            return OperationResult.Success("服务器已启动。");
        }
        catch (Exception ex)
        {
            return OperationResult.Failed("启动服务端失败。", ex);
        }
        finally
        {
            _processGate.Release();
        }
    }

    public async Task<OperationResult> StopAsync(TimeSpan gracefulTimeout, CancellationToken cancellationToken = default)
    {
        await _processGate.WaitAsync(cancellationToken);
        try
        {
            if (_process is null || _process.HasExited)
            {
                return OperationResult.Success("服务器未运行。");
            }

            try
            {
                await SendCommandInternalAsync("/stop", cancellationToken);
                using var timeoutCts = CancellationTokenSource.CreateLinkedTokenSource(cancellationToken);
                timeoutCts.CancelAfter(gracefulTimeout);
                await _process.WaitForExitAsync(timeoutCts.Token);
                return OperationResult.Success("服务器已优雅停止。");
            }
            catch (OperationCanceledException)
            {
                _process.Kill(entireProcessTree: true);
                await _process.WaitForExitAsync(cancellationToken);
                OutputReceived?.Invoke(this, "[system] 服务器未在超时时间内退出，已强制终止。");
                return OperationResult.Success("服务器已强制停止。");
            }
            catch (Exception ex)
            {
                return OperationResult.Failed("停止服务器失败。", ex);
            }
        }
        finally
        {
            _processGate.Release();
        }
    }

    public async Task<OperationResult> SendCommandAsync(string command, CancellationToken cancellationToken = default)
    {
        await _processGate.WaitAsync(cancellationToken);
        try
        {
            return await SendCommandInternalAsync(command, cancellationToken);
        }
        finally
        {
            _processGate.Release();
        }
    }

    private async Task<OperationResult> SendCommandInternalAsync(string command, CancellationToken cancellationToken)
    {
        if (_process is null || _process.HasExited)
        {
            return OperationResult.Failed("服务器未运行。");
        }

        var normalized = string.IsNullOrWhiteSpace(command) ? string.Empty : command.Trim();
        if (string.IsNullOrWhiteSpace(normalized))
        {
            return OperationResult.Failed("命令不能为空。");
        }

        if (!normalized.StartsWith('/'))
        {
            normalized = "/" + normalized;
        }

        await _process.StandardInput.WriteLineAsync(normalized.AsMemory(), cancellationToken);
        await _process.StandardInput.FlushAsync(cancellationToken);
        OutputReceived?.Invoke(this, $"[cmd] {normalized}");
        return OperationResult.Success();
    }

    private void OnOutputDataReceived(object sender, DataReceivedEventArgs e)
    {
        if (!string.IsNullOrWhiteSpace(e.Data))
        {
            OutputReceived?.Invoke(this, e.Data);
        }
    }

    private void OnProcessExited(object? sender, EventArgs e)
    {
        var previousProfileId = _currentProfile?.Id;
        CurrentStatus = new ServerRuntimeStatus
        {
            IsRunning = false,
            ProcessId = null,
            StartedAtUtc = null,
            ProfileId = previousProfileId
        };

        StatusChanged?.Invoke(this, CurrentStatus);
        OutputReceived?.Invoke(this, "[system] 服务器进程已退出。");

        if (_process is not null)
        {
            _process.OutputDataReceived -= OnOutputDataReceived;
            _process.ErrorDataReceived -= OnOutputDataReceived;
            _process.Exited -= OnProcessExited;
            _process.Dispose();
            _process = null;
        }
    }

    private static OperationResult EnsureProfileWritable(ServerProfile profile)
    {
        var logsPath = WorkspaceLayout.GetLogsPath(profile.DataPath);
        Directory.CreateDirectory(logsPath);

        var filesToProbe = new[]
        {
            profile.ActiveSaveFile,
            Path.Combine(logsPath, "server-main.log"),
            Path.Combine(logsPath, "server-chat.log"),
            Path.Combine(logsPath, "server-debug.log"),
            Path.Combine(logsPath, "server-audit.log")
        };

        foreach (var filePath in filesToProbe)
        {
            var probeResult = ProbeReadWrite(filePath);
            if (!probeResult.IsSuccess)
            {
                return probeResult;
            }
        }

        return OperationResult.Success();
    }

    private static OperationResult ProbeReadWrite(string filePath)
    {
        try
        {
            var directory = Path.GetDirectoryName(filePath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            using var stream = new FileStream(
                filePath,
                FileMode.OpenOrCreate,
                FileAccess.ReadWrite,
                FileShare.None);
            return OperationResult.Success();
        }
        catch (UnauthorizedAccessException ex)
        {
            return OperationResult.Failed($"服务器文件无写入权限：{filePath}", ex);
        }
        catch (IOException ex)
        {
            return OperationResult.Failed(
                $"服务器文件被占用：{filePath}。可能已有服务器实例正在运行，请先停止后再启动。",
                ex);
        }
    }
}
