using System.Collections.Concurrent;
using System.Globalization;
using System.Net.WebSockets;
using System.Text;
using System.Text.Json;
using System.Text.Json.Nodes;
using System.Text.RegularExpressions;
using Microsoft.Data.Sqlite;
using VSL.Application;
using VSL.Domain;

namespace VSL.Infrastructure.Services;

public sealed class Vs2QQProcessService : IVs2QQProcessService
{
    private static int _encodingProviderRegistered;
    private readonly SemaphoreSlim _runtimeGate = new(1, 1);
    private CancellationTokenSource? _runCts;
    private Task? _runTask;
    private Vs2QQRuntimeContext? _runtime;

    public event EventHandler<string>? OutputReceived;

    public event EventHandler<Vs2QQRuntimeStatus>? StatusChanged;

    public Vs2QQRuntimeStatus CurrentStatus { get; private set; } = new();

    public Vs2QQProcessService()
    {
        if (Interlocked.Exchange(ref _encodingProviderRegistered, 1) == 0)
        {
            Encoding.RegisterProvider(CodePagesEncodingProvider.Instance);
        }
    }

    public async Task<OperationResult> StartAsync(Vs2QQLaunchSettings settings, CancellationToken cancellationToken = default)
    {
        await _runtimeGate.WaitAsync(cancellationToken);
        try
        {
            if (_runtime is not null && CurrentStatus.IsRunning)
            {
                return OperationResult.Failed("VS2QQ 已在运行中。");
            }

            var normalizeResult = NormalizeLaunchSettings(settings);
            if (!normalizeResult.IsSuccess || normalizeResult.Value is null)
            {
                return OperationResult.Failed(normalizeResult.Message ?? "VS2QQ 配置无效。");
            }

            var normalized = normalizeResult.Value;
            var storage = new Vs2QQStorage(normalized.DatabasePath);
            var parser = new Vs2QQTalkLineParser();
            var tailer = new Vs2QQLogTailer(
                storage,
                parser,
                normalized.DefaultEncoding,
                normalized.FallbackEncoding,
                EmitOutput);

            Vs2QQRuntimeContext runtime = new(normalized, storage, tailer);
            var oneBot = new Vs2QQOneBotClient(
                normalized.OneBotWsUrl,
                normalized.AccessToken,
                normalized.ReconnectIntervalSec,
                EmitOutput,
                (eventPayload, token) => HandleOneBotEventAsync(runtime, eventPayload, token));
            runtime.OneBot = oneBot;

            _runCts = new CancellationTokenSource();
            _runtime = runtime;
            _runTask = Task.Run(() => RunRuntimeAsync(runtime, _runCts.Token), CancellationToken.None);

            CurrentStatus = new Vs2QQRuntimeStatus
            {
                IsRunning = true,
                ProcessId = Environment.ProcessId,
                StartedAtUtc = DateTimeOffset.UtcNow,
                OneBotWsUrl = normalized.OneBotWsUrl
            };
            StatusChanged?.Invoke(this, CurrentStatus);
            EmitOutput($"[system] VS2QQ 已启动。OneBot={normalized.OneBotWsUrl}");
            EmitOutput($"[system] VS2QQ 数据库：{normalized.DatabasePath}");

            return OperationResult.Success("VS2QQ 已启动。");
        }
        catch (Exception ex)
        {
            return OperationResult.Failed("启动 VS2QQ 失败。", ex);
        }
        finally
        {
            _runtimeGate.Release();
        }
    }

    public async Task<OperationResult> StopAsync(TimeSpan gracefulTimeout, CancellationToken cancellationToken = default)
    {
        Task? runTask;
        CancellationTokenSource? cts;

        await _runtimeGate.WaitAsync(cancellationToken);
        try
        {
            if (_runtime is null || _runTask is null || !CurrentStatus.IsRunning)
            {
                return OperationResult.Success("VS2QQ 未运行。");
            }

            runTask = _runTask;
            cts = _runCts;
            cts?.Cancel();
        }
        finally
        {
            _runtimeGate.Release();
        }

        try
        {
            var timeoutTask = Task.Delay(gracefulTimeout, cancellationToken);
            var completed = await Task.WhenAny(runTask!, timeoutTask);
            cancellationToken.ThrowIfCancellationRequested();

            if (!ReferenceEquals(completed, runTask))
            {
                return OperationResult.Failed("停止 VS2QQ 超时。");
            }

            await runTask!;
            return OperationResult.Success("VS2QQ 已停止。");
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            throw;
        }
        catch (Exception ex)
        {
            return OperationResult.Failed("停止 VS2QQ 失败。", ex);
        }
    }

    private async Task RunRuntimeAsync(Vs2QQRuntimeContext runtime, CancellationToken cancellationToken)
    {
        try
        {
            var oneBotTask = runtime.OneBot.RunForeverAsync(cancellationToken);
            var pollTask = PollLogsLoopAsync(runtime, cancellationToken);
            await Task.WhenAll(oneBotTask, pollTask);
        }
        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
        {
            // Normal cancellation.
        }
        catch (Exception ex)
        {
            EmitOutput($"[system] VS2QQ 运行异常: {ex.Message}");
        }
        finally
        {
            await FinalizeRuntimeAsync(runtime);
        }
    }

    private async Task FinalizeRuntimeAsync(Vs2QQRuntimeContext runtime)
    {
        bool shouldNotifyStopped = false;
        string? wsUrl = null;
        CancellationTokenSource? ctsToDispose = null;

        await _runtimeGate.WaitAsync();
        try
        {
            if (!ReferenceEquals(_runtime, runtime))
            {
                return;
            }

            wsUrl = runtime.Settings.OneBotWsUrl;
            ctsToDispose = _runCts;
            _runCts = null;
            _runTask = null;
            _runtime = null;
            shouldNotifyStopped = CurrentStatus.IsRunning;

            CurrentStatus = new Vs2QQRuntimeStatus
            {
                IsRunning = false,
                ProcessId = null,
                StartedAtUtc = null,
                OneBotWsUrl = wsUrl
            };
        }
        finally
        {
            _runtimeGate.Release();
        }

        ctsToDispose?.Dispose();
        await runtime.DisposeAsync();

        if (shouldNotifyStopped)
        {
            StatusChanged?.Invoke(this, CurrentStatus);
            EmitOutput("[system] VS2QQ 已停止。");
        }
    }

    private async Task PollLogsLoopAsync(Vs2QQRuntimeContext runtime, CancellationToken cancellationToken)
    {
        while (!cancellationToken.IsCancellationRequested)
        {
            try
            {
                await PollLogsOnceAsync(runtime, cancellationToken);
            }
            catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
            {
                break;
            }
            catch (Exception ex)
            {
                EmitOutput($"[warn] VS2QQ 日志轮询异常: {ex.Message}");
            }

            await Task.Delay(TimeSpan.FromSeconds(runtime.Settings.PollIntervalSec), cancellationToken);
        }
    }

    private async Task PollLogsOnceAsync(Vs2QQRuntimeContext runtime, CancellationToken cancellationToken)
    {
        var servers = runtime.Storage.ListActiveServers();
        foreach (var server in servers)
        {
            cancellationToken.ThrowIfCancellationRequested();
            IReadOnlyList<Vs2QQTalkMessage> messages;
            try
            {
                messages = runtime.Tailer.PollServer(server);
            }
            catch (Exception ex)
            {
                EmitOutput($"[warn] 轮询服务器日志失败 server={server.ServerId}: {ex.Message}");
                continue;
            }

            foreach (var message in messages)
            {
                await ForwardTalkMessageAsync(runtime, message, cancellationToken);
            }
        }
    }

    private async Task ForwardTalkMessageAsync(Vs2QQRuntimeContext runtime, Vs2QQTalkMessage talk, CancellationToken cancellationToken)
    {
        var groups = runtime.Storage.ListGroupsForServer(talk.ServerId);
        if (groups.Count == 0)
        {
            return;
        }

        var boundQq = runtime.Storage.FindQqByPlayer(talk.Sender);
        var senderLine = boundQq.HasValue
            ? $"{talk.Sender}(QQ:{boundQq.Value}): {talk.Content}"
            : $"{talk.Sender}: {talk.Content}";
        var payload = $"[VS:{talk.ServerId}] {talk.Timestamp}\n{senderLine}";
        EmitOutput($"[vs2qq] 日志转发 server={talk.ServerId} groups={groups.Count} sender={talk.Sender}");

        foreach (var groupId in groups)
        {
            try
            {
                await runtime.OneBot.SendGroupMsgAsync(groupId, payload, cancellationToken);
            }
            catch (Exception ex)
            {
                EmitOutput($"[warn] 发送群消息失败 group={groupId} server={talk.ServerId}: {ex.Message}");
            }
        }
    }

    private async Task HandleOneBotEventAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, CancellationToken cancellationToken)
    {
        if (!string.Equals(GetString(eventPayload, "post_type"), "message", StringComparison.OrdinalIgnoreCase))
        {
            return;
        }

        var userId = GetInt64(eventPayload, "user_id");
        var selfId = GetInt64(eventPayload, "self_id", -1);
        if (userId > 0 && userId == selfId)
        {
            return;
        }

        var rawMessage = GetString(eventPayload, "raw_message").Trim();
        if (!rawMessage.StartsWith('/'))
        {
            return;
        }

        try
        {
            await HandleCommandAsync(runtime, eventPayload, rawMessage, cancellationToken);
        }
        catch (Exception ex)
        {
            EmitOutput($"[warn] 命令处理异常: {ex.Message}");
            try
            {
                await ReplyAsync(runtime, eventPayload, $"Command error: {ex.Message}", cancellationToken);
            }
            catch (Exception replyEx)
            {
                EmitOutput($"[warn] 命令异常回包失败: {replyEx.Message}");
            }
        }
    }

    private async Task HandleCommandAsync(
        Vs2QQRuntimeContext runtime,
        JsonObject eventPayload,
        string rawCommand,
        CancellationToken cancellationToken)
    {
        var firstSpace = rawCommand.IndexOf(' ');
        var command = (firstSpace >= 0 ? rawCommand[..firstSpace] : rawCommand).Trim().ToLowerInvariant();
        var args = firstSpace >= 0 ? rawCommand[(firstSpace + 1)..].Trim() : string.Empty;

        switch (command)
        {
            case "/help":
                await ReplyAsync(runtime, eventPayload, BuildHelpText(), cancellationToken);
                return;
            case "/bindqq":
                await HandleBindQqAsync(runtime, eventPayload, args, cancellationToken);
                return;
            case "/unbindqq":
                await HandleUnbindQqAsync(runtime, eventPayload, cancellationToken);
                return;
            case "/mybind":
                await HandleMyBindAsync(runtime, eventPayload, cancellationToken);
                return;
            case "/bindserver":
                await HandleBindServerAsync(runtime, eventPayload, args, cancellationToken);
                return;
            case "/unbindserver":
                await HandleUnbindServerAsync(runtime, eventPayload, args, cancellationToken);
                return;
            case "/listserver":
                await HandleListServerAsync(runtime, eventPayload, cancellationToken);
                return;
            case "/bindserverregex":
                await HandleBindServerRegexAsync(runtime, eventPayload, args, cancellationToken);
                return;
            default:
                await ReplyAsync(runtime, eventPayload, "Unknown command. Use /help.", cancellationToken);
                return;
        }
    }

    private async Task HandleBindQqAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, string args, CancellationToken cancellationToken)
    {
        var playerName = args.Trim();
        if (string.IsNullOrWhiteSpace(playerName))
        {
            await ReplyAsync(runtime, eventPayload, "Usage: /bindqq <player_name>", cancellationToken);
            return;
        }

        var userId = GetInt64(eventPayload, "user_id");
        if (userId <= 0)
        {
            await ReplyAsync(runtime, eventPayload, "Cannot identify user.", cancellationToken);
            return;
        }

        runtime.Storage.BindQq(userId, playerName);
        await ReplyAsync(runtime, eventPayload, $"Bound QQ {userId} -> player '{playerName}'.", cancellationToken);
    }

    private async Task HandleUnbindQqAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, CancellationToken cancellationToken)
    {
        var userId = GetInt64(eventPayload, "user_id");
        if (userId <= 0)
        {
            await ReplyAsync(runtime, eventPayload, "Cannot identify user.", cancellationToken);
            return;
        }

        var deleted = runtime.Storage.UnbindQq(userId);
        await ReplyAsync(runtime, eventPayload, deleted ? "QQ binding removed." : "No QQ binding found.", cancellationToken);
    }

    private async Task HandleMyBindAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, CancellationToken cancellationToken)
    {
        var userId = GetInt64(eventPayload, "user_id");
        if (userId <= 0)
        {
            await ReplyAsync(runtime, eventPayload, "Cannot identify user.", cancellationToken);
            return;
        }

        var binding = runtime.Storage.GetQqBinding(userId);
        if (binding is null)
        {
            await ReplyAsync(runtime, eventPayload, "No QQ binding. Use /bindqq <player_name>.", cancellationToken);
            return;
        }

        await ReplyAsync(runtime, eventPayload, $"QQ {binding.Value.QqId} is bound to player '{binding.Value.PlayerName}'.", cancellationToken);
    }

    private async Task HandleBindServerAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, string args, CancellationToken cancellationToken)
    {
        if (!IsGroupMessage(eventPayload))
        {
            await ReplyAsync(runtime, eventPayload, "Use this command in a group chat.", cancellationToken);
            return;
        }
        if (!HasAdminPermission(runtime, eventPayload))
        {
            await ReplyAsync(runtime, eventPayload, "Permission denied. Admin/owner only.", cancellationToken);
            return;
        }

        var match = Regex.Match(args, @"^(\S+)\s+(.+)$");
        if (!match.Success)
        {
            await ReplyAsync(runtime, eventPayload, "Usage: /bindserver <server_id> <log_path>", cancellationToken);
            return;
        }

        var serverId = match.Groups[1].Value.Trim();
        var rawLogPath = StripQuotes(match.Groups[2].Value.Trim());
        var logPath = ResolvePath(rawLogPath);
        var groupId = GetInt64(eventPayload, "group_id");

        runtime.Storage.UpsertServer(serverId, logPath);
        runtime.Storage.BindGroupServer(groupId, serverId);
        runtime.Tailer.PrimeServer(serverId, logPath);

        await ReplyAsync(
            runtime,
            eventPayload,
            $"Group {groupId} is now bound to server '{serverId}'. Log: {logPath}",
            cancellationToken);
    }

    private async Task HandleUnbindServerAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, string args, CancellationToken cancellationToken)
    {
        if (!IsGroupMessage(eventPayload))
        {
            await ReplyAsync(runtime, eventPayload, "Use this command in a group chat.", cancellationToken);
            return;
        }
        if (!HasAdminPermission(runtime, eventPayload))
        {
            await ReplyAsync(runtime, eventPayload, "Permission denied. Admin/owner only.", cancellationToken);
            return;
        }
        if (string.IsNullOrWhiteSpace(args))
        {
            await ReplyAsync(runtime, eventPayload, "Usage: /unbindserver <server_id>", cancellationToken);
            return;
        }

        var serverId = args.Split(' ', StringSplitOptions.RemoveEmptyEntries)[0];
        var groupId = GetInt64(eventPayload, "group_id");
        var deleted = runtime.Storage.UnbindGroupServer(groupId, serverId);

        await ReplyAsync(
            runtime,
            eventPayload,
            deleted
                ? $"Unbound server '{serverId}' from group {groupId}."
                : $"Server '{serverId}' was not bound to this group.",
            cancellationToken);
    }

    private async Task HandleListServerAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, CancellationToken cancellationToken)
    {
        if (!IsGroupMessage(eventPayload))
        {
            await ReplyAsync(runtime, eventPayload, "Use this command in a group chat.", cancellationToken);
            return;
        }

        var groupId = GetInt64(eventPayload, "group_id");
        var servers = runtime.Storage.ListGroupServers(groupId);
        if (servers.Count == 0)
        {
            await ReplyAsync(runtime, eventPayload, "No bound servers in this group.", cancellationToken);
            return;
        }

        var lines = new List<string> { "Bound servers:" };
        lines.AddRange(servers.Select(x => $"- {x.ServerId}: {x.LogPath}"));
        await ReplyAsync(runtime, eventPayload, string.Join('\n', lines), cancellationToken);
    }

    private async Task HandleBindServerRegexAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, string args, CancellationToken cancellationToken)
    {
        if (!IsGroupMessage(eventPayload))
        {
            await ReplyAsync(runtime, eventPayload, "Use this command in a group chat.", cancellationToken);
            return;
        }
        if (!HasAdminPermission(runtime, eventPayload))
        {
            await ReplyAsync(runtime, eventPayload, "Permission denied. Admin/owner only.", cancellationToken);
            return;
        }

        var match = Regex.Match(args, @"^(\S+)\s+(.+)$");
        if (!match.Success)
        {
            await ReplyAsync(runtime, eventPayload, "Usage: /bindserverregex <server_id> <regex>", cancellationToken);
            return;
        }

        var serverId = match.Groups[1].Value.Trim();
        var regexValue = StripQuotes(match.Groups[2].Value.Trim());
        var ok = runtime.Storage.SetServerRegex(serverId, regexValue);
        if (!ok)
        {
            await ReplyAsync(runtime, eventPayload, $"Server '{serverId}' not found. Bind server first.", cancellationToken);
            return;
        }

        await ReplyAsync(runtime, eventPayload, $"Custom regex updated for server '{serverId}'.", cancellationToken);
    }

    private async Task ReplyAsync(Vs2QQRuntimeContext runtime, JsonObject eventPayload, string message, CancellationToken cancellationToken)
    {
        if (IsGroupMessage(eventPayload))
        {
            var groupId = GetInt64(eventPayload, "group_id");
            if (groupId > 0)
            {
                await runtime.OneBot.SendGroupMsgAsync(groupId, message, cancellationToken);
                return;
            }
        }

        var userId = GetInt64(eventPayload, "user_id");
        if (userId > 0)
        {
            await runtime.OneBot.SendPrivateMsgAsync(userId, message, cancellationToken);
        }
    }

    private static bool IsGroupMessage(JsonObject eventPayload)
    {
        return string.Equals(GetString(eventPayload, "message_type"), "group", StringComparison.OrdinalIgnoreCase);
    }

    private static bool HasAdminPermission(Vs2QQRuntimeContext runtime, JsonObject eventPayload)
    {
        var userId = GetInt64(eventPayload, "user_id");
        if (runtime.SuperUsers.Contains(userId))
        {
            return true;
        }

        if (eventPayload["sender"] is not JsonObject senderObject)
        {
            return false;
        }

        var role = GetString(senderObject, "role");
        return role is "admin" or "owner";
    }

    private static string BuildHelpText()
    {
        return """
            VS2QQ commands:
            /help
            /bindqq <player_name>
            /unbindqq
            /mybind
            /bindserver <server_id> <log_path> (group admin/owner)
            /unbindserver <server_id> (group admin/owner)
            /listserver (group)
            /bindserverregex <server_id> <regex> (group admin/owner)
            """;
    }

    private static string ResolvePath(string value)
    {
        if (string.IsNullOrWhiteSpace(value))
        {
            return string.Empty;
        }

        try
        {
            return Path.GetFullPath(Environment.ExpandEnvironmentVariables(value.Trim()));
        }
        catch
        {
            return value.Trim();
        }
    }

    private static string StripQuotes(string value)
    {
        if (value.Length >= 2 && value[0] == value[^1] && (value[0] == '"' || value[0] == '\''))
        {
            return value[1..^1];
        }

        return value;
    }

    private static string GetString(JsonObject obj, string key)
    {
        return obj.TryGetPropertyValue(key, out var node) && node is not null
            ? node.ToString()
            : string.Empty;
    }

    private static long GetInt64(JsonObject obj, string key, long fallback = 0)
    {
        if (!obj.TryGetPropertyValue(key, out var node) || node is null)
        {
            return fallback;
        }

        if (node is JsonValue valueNode)
        {
            if (valueNode.TryGetValue<long>(out var longValue))
            {
                return longValue;
            }

            if (valueNode.TryGetValue<int>(out var intValue))
            {
                return intValue;
            }
        }

        return long.TryParse(node.ToString(), NumberStyles.Integer, CultureInfo.InvariantCulture, out var parsed)
            ? parsed
            : fallback;
    }

    private void EmitOutput(string message)
    {
        OutputReceived?.Invoke(this, message);
    }

    private static OperationResult<Vs2QQLaunchSettings> NormalizeLaunchSettings(Vs2QQLaunchSettings settings)
    {
        var wsUrl = (settings.OneBotWsUrl ?? string.Empty).Trim();
        if (string.IsNullOrWhiteSpace(wsUrl))
        {
            return OperationResult<Vs2QQLaunchSettings>.Failed("缺少 OneBot WebSocket 地址。");
        }

        if (!Uri.TryCreate(wsUrl, UriKind.Absolute, out var wsUri)
            || (wsUri.Scheme != "ws" && wsUri.Scheme != "wss"))
        {
            return OperationResult<Vs2QQLaunchSettings>.Failed("OneBot WebSocket 地址格式无效，必须是 ws:// 或 wss://。");
        }

        var dbPath = string.IsNullOrWhiteSpace(settings.DatabasePath)
            ? Path.Combine(WorkspaceLayout.WorkspaceRoot, "vs2qq", "vs2qq.db")
            : settings.DatabasePath.Trim();
        if (!Path.IsPathRooted(dbPath))
        {
            dbPath = Path.Combine(WorkspaceLayout.WorkspaceRoot, dbPath);
        }
        dbPath = Path.GetFullPath(dbPath);

        var reconnectInterval = settings.ReconnectIntervalSec <= 0 ? 5 : settings.ReconnectIntervalSec;
        var pollInterval = settings.PollIntervalSec <= 0 ? 1.0 : settings.PollIntervalSec;
        var defaultEncoding = string.IsNullOrWhiteSpace(settings.DefaultEncoding) ? "utf-8" : settings.DefaultEncoding.Trim();
        var fallbackEncoding = string.IsNullOrWhiteSpace(settings.FallbackEncoding) ? "gbk" : settings.FallbackEncoding.Trim();
        var normalizedSuperUsers = (settings.SuperUsers ?? [])
            .Where(x => x > 0)
            .Distinct()
            .ToArray();

        return OperationResult<Vs2QQLaunchSettings>.Success(new Vs2QQLaunchSettings
        {
            OneBotWsUrl = wsUrl,
            AccessToken = string.IsNullOrWhiteSpace(settings.AccessToken) ? null : settings.AccessToken.Trim(),
            ReconnectIntervalSec = reconnectInterval,
            DatabasePath = dbPath,
            PollIntervalSec = pollInterval,
            DefaultEncoding = defaultEncoding,
            FallbackEncoding = fallbackEncoding,
            SuperUsers = normalizedSuperUsers
        });
    }

    private sealed class Vs2QQRuntimeContext : IAsyncDisposable
    {
        private int _disposedFlag;

        public Vs2QQRuntimeContext(
            Vs2QQLaunchSettings settings,
            Vs2QQStorage storage,
            Vs2QQLogTailer tailer)
        {
            Settings = settings;
            Storage = storage;
            Tailer = tailer;
            SuperUsers = settings.SuperUsers?.ToHashSet() ?? [];
        }

        public Vs2QQLaunchSettings Settings { get; }

        public HashSet<long> SuperUsers { get; }

        public Vs2QQStorage Storage { get; }

        public Vs2QQLogTailer Tailer { get; }

        public Vs2QQOneBotClient OneBot { get; set; } = null!;

        public async ValueTask DisposeAsync()
        {
            if (Interlocked.Exchange(ref _disposedFlag, 1) == 1)
            {
                return;
            }

            await OneBot.DisposeAsync();
            Storage.Dispose();
        }
    }

    private sealed class Vs2QQOneBotClient : IAsyncDisposable
    {
        private static readonly JsonSerializerOptions JsonOptions = new()
        {
            WriteIndented = false
        };

        private readonly Uri _wsUri;
        private readonly string? _accessToken;
        private readonly int _reconnectIntervalSec;
        private readonly Action<string> _log;
        private readonly Func<JsonObject, CancellationToken, Task> _eventHandler;
        private readonly ConcurrentDictionary<string, TaskCompletionSource<JsonObject>> _echoWaiters = new();
        private readonly SemaphoreSlim _sendGate = new(1, 1);
        private readonly object _socketGate = new();
        private ClientWebSocket? _socket;

        public Vs2QQOneBotClient(
            string wsUrl,
            string? accessToken,
            int reconnectIntervalSec,
            Action<string> log,
            Func<JsonObject, CancellationToken, Task> eventHandler)
        {
            _wsUri = new Uri(wsUrl, UriKind.Absolute);
            _accessToken = accessToken;
            _reconnectIntervalSec = reconnectIntervalSec;
            _log = log;
            _eventHandler = eventHandler;
        }

        public async Task RunForeverAsync(CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested)
            {
                using var socket = new ClientWebSocket();
                if (!string.IsNullOrWhiteSpace(_accessToken))
                {
                    socket.Options.SetRequestHeader("Authorization", $"Bearer {_accessToken}");
                }

                try
                {
                    _log($"[onebot] Connecting {_wsUri} ...");
                    await socket.ConnectAsync(_wsUri, cancellationToken);
                    SetSocket(socket);
                    _log("[onebot] Connected.");
                    await ConsumeMessagesAsync(socket, cancellationToken);
                }
                catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                {
                    break;
                }
                catch (Exception ex)
                {
                    _log($"[onebot] Disconnected: {ex.Message}");
                }
                finally
                {
                    SetSocket(null);
                    FailPendingWaiters(new InvalidOperationException("OneBot connection closed."));
                }

                await Task.Delay(TimeSpan.FromSeconds(_reconnectIntervalSec), cancellationToken);
            }
        }

        public async Task SendGroupMsgAsync(long groupId, string message, CancellationToken cancellationToken)
        {
            var parameters = new JsonObject
            {
                ["group_id"] = groupId,
                ["message"] = message
            };

            await CallActionAsync("send_group_msg", parameters, TimeSpan.FromSeconds(20), cancellationToken);
        }

        public async Task SendPrivateMsgAsync(long userId, string message, CancellationToken cancellationToken)
        {
            var parameters = new JsonObject
            {
                ["user_id"] = userId,
                ["message"] = message
            };

            await CallActionAsync("send_private_msg", parameters, TimeSpan.FromSeconds(20), cancellationToken);
        }

        public async Task<JsonNode?> CallActionAsync(
            string action,
            JsonObject parameters,
            TimeSpan timeout,
            CancellationToken cancellationToken)
        {
            var echo = Guid.NewGuid().ToString("N");
            var waiter = new TaskCompletionSource<JsonObject>(TaskCreationOptions.RunContinuationsAsynchronously);
            if (!_echoWaiters.TryAdd(echo, waiter))
            {
                throw new InvalidOperationException("Cannot create action waiter.");
            }

            try
            {
                var payload = new JsonObject
                {
                    ["action"] = action,
                    ["params"] = parameters,
                    ["echo"] = echo
                };

                await SendTextAsync(payload.ToJsonString(JsonOptions), cancellationToken);

                var delayTask = Task.Delay(timeout, cancellationToken);
                var completed = await Task.WhenAny(waiter.Task, delayTask);
                cancellationToken.ThrowIfCancellationRequested();
                if (!ReferenceEquals(completed, waiter.Task))
                {
                    throw new TimeoutException(
                        $"OneBot action timeout: {action}. " +
                        "未收到动作回包，请检查 OneBot WS 地址/AccessToken/协议版本是否匹配。");
                }

                var response = await waiter.Task;
                var status = response["status"]?.ToString();
                if (!string.Equals(status, "ok", StringComparison.OrdinalIgnoreCase))
                {
                    var retCode = response["retcode"]?.ToString();
                    var msg = response["msg"]?.ToString();
                    throw new InvalidOperationException($"OneBot action failed: action={action}, retcode={retCode}, msg={msg}");
                }

                return response["data"];
            }
            finally
            {
                _echoWaiters.TryRemove(echo, out _);
            }
        }

        public async ValueTask DisposeAsync()
        {
            SetSocket(null);
            FailPendingWaiters(new OperationCanceledException("OneBot client disposed."));

            ClientWebSocket? snapshot;
            lock (_socketGate)
            {
                snapshot = _socket;
                _socket = null;
            }

            if (snapshot is not null)
            {
                try
                {
                    if (snapshot.State is WebSocketState.Open or WebSocketState.CloseReceived)
                    {
                        using var cts = new CancellationTokenSource(TimeSpan.FromSeconds(2));
                        await snapshot.CloseAsync(WebSocketCloseStatus.NormalClosure, "dispose", cts.Token);
                    }
                }
                catch
                {
                    // Ignore shutdown errors.
                }
                finally
                {
                    snapshot.Dispose();
                }
            }
        }

        private void SetSocket(ClientWebSocket? socket)
        {
            lock (_socketGate)
            {
                _socket = socket;
            }
        }

        private ClientWebSocket? GetSocket()
        {
            lock (_socketGate)
            {
                return _socket;
            }
        }

        private async Task SendTextAsync(string text, CancellationToken cancellationToken)
        {
            var socket = GetSocket();
            if (socket is null || socket.State != WebSocketState.Open)
            {
                throw new InvalidOperationException("OneBot is not connected.");
            }

            await _sendGate.WaitAsync(cancellationToken);
            try
            {
                var bytes = Encoding.UTF8.GetBytes(text);
                await socket.SendAsync(
                    new ArraySegment<byte>(bytes),
                    WebSocketMessageType.Text,
                    endOfMessage: true,
                    cancellationToken);
            }
            finally
            {
                _sendGate.Release();
            }
        }

        private async Task ConsumeMessagesAsync(ClientWebSocket socket, CancellationToken cancellationToken)
        {
            while (!cancellationToken.IsCancellationRequested && socket.State == WebSocketState.Open)
            {
                var text = await ReceiveTextAsync(socket, cancellationToken);
                if (text is null)
                {
                    break;
                }

                JsonNode? node;
                try
                {
                    node = JsonNode.Parse(text);
                }
                catch
                {
                    continue;
                }

                if (node is not JsonObject payload)
                {
                    continue;
                }

                var echoValue = payload["echo"]?.ToString();
                if (!string.IsNullOrWhiteSpace(echoValue)
                    && _echoWaiters.TryGetValue(echoValue, out var waiter))
                {
                    waiter.TrySetResult(payload);
                    continue;
                }

                if (payload["post_type"] is not null)
                {
                    _ = Task.Run(async () =>
                    {
                        try
                        {
                            await _eventHandler(payload, cancellationToken);
                        }
                        catch (OperationCanceledException) when (cancellationToken.IsCancellationRequested)
                        {
                            // Normal shutdown.
                        }
                        catch (Exception ex)
                        {
                            _log($"[warn] OneBot 事件处理异常: {ex.Message}");
                        }
                    }, cancellationToken);
                }
            }
        }

        private static async Task<string?> ReceiveTextAsync(ClientWebSocket socket, CancellationToken cancellationToken)
        {
            var buffer = new byte[8 * 1024];
            using var stream = new MemoryStream();

            while (true)
            {
                var result = await socket.ReceiveAsync(new ArraySegment<byte>(buffer), cancellationToken);
                if (result.MessageType == WebSocketMessageType.Close)
                {
                    try
                    {
                        if (socket.State == WebSocketState.CloseReceived)
                        {
                            await socket.CloseOutputAsync(WebSocketCloseStatus.NormalClosure, "close-received", cancellationToken);
                        }
                    }
                    catch
                    {
                        // Ignore close errors.
                    }

                    return null;
                }

                if (result.Count > 0)
                {
                    await stream.WriteAsync(buffer.AsMemory(0, result.Count), cancellationToken);
                }

                if (result.EndOfMessage)
                {
                    break;
                }
            }

            if (stream.Length == 0)
            {
                return string.Empty;
            }

            return Encoding.UTF8.GetString(stream.ToArray());
        }

        private void FailPendingWaiters(Exception exception)
        {
            foreach (var item in _echoWaiters.Values)
            {
                item.TrySetException(exception);
            }

            _echoWaiters.Clear();
        }
    }

    private sealed class Vs2QQStorage : IDisposable
    {
        private readonly object _sync = new();
        private readonly SqliteConnection _connection;
        private bool _disposed;

        public Vs2QQStorage(string dbPath)
        {
            var directory = Path.GetDirectoryName(dbPath);
            if (!string.IsNullOrWhiteSpace(directory))
            {
                Directory.CreateDirectory(directory);
            }

            _connection = new SqliteConnection($"Data Source={dbPath}");
            _connection.Open();
            using (var pragma = _connection.CreateCommand())
            {
                pragma.CommandText = "PRAGMA foreign_keys = ON;";
                pragma.ExecuteNonQuery();
            }
            InitializeSchema();
        }

        public void Dispose()
        {
            lock (_sync)
            {
                if (_disposed)
                {
                    return;
                }

                _disposed = true;
                _connection.Dispose();
            }
        }

        public void BindQq(long qqId, string playerName)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    INSERT INTO qq_bindings (qq_id, player_name, created_at, updated_at)
                    VALUES ($qqId, $playerName, $createdAt, $updatedAt)
                    ON CONFLICT(qq_id) DO UPDATE SET
                        player_name = excluded.player_name,
                        updated_at = excluded.updated_at;
                    """;
                command.Parameters.AddWithValue("$qqId", qqId);
                command.Parameters.AddWithValue("$playerName", playerName);
                command.Parameters.AddWithValue("$createdAt", GetUtcNowIso());
                command.Parameters.AddWithValue("$updatedAt", GetUtcNowIso());
                command.ExecuteNonQuery();
            }
        }

        public bool UnbindQq(long qqId)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "DELETE FROM qq_bindings WHERE qq_id = $qqId;";
                command.Parameters.AddWithValue("$qqId", qqId);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public (long QqId, string PlayerName)? GetQqBinding(long qqId)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT qq_id, player_name FROM qq_bindings WHERE qq_id = $qqId LIMIT 1;";
                command.Parameters.AddWithValue("$qqId", qqId);
                using var reader = command.ExecuteReader();
                if (!reader.Read())
                {
                    return null;
                }

                return (reader.GetInt64(0), reader.GetString(1));
            }
        }

        public long? FindQqByPlayer(string playerName)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    SELECT qq_id
                    FROM qq_bindings
                    WHERE lower(player_name) = lower($playerName)
                    LIMIT 1;
                    """;
                command.Parameters.AddWithValue("$playerName", playerName);
                var value = command.ExecuteScalar();
                if (value is null || value == DBNull.Value)
                {
                    return null;
                }

                return Convert.ToInt64(value, CultureInfo.InvariantCulture);
            }
        }

        public void UpsertServer(string serverId, string logPath, string? chatRegex = null)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    INSERT INTO servers (server_id, log_path, chat_regex, enabled, created_at, updated_at)
                    VALUES ($serverId, $logPath, $chatRegex, 1, $createdAt, $updatedAt)
                    ON CONFLICT(server_id) DO UPDATE SET
                        log_path = excluded.log_path,
                        chat_regex = COALESCE(excluded.chat_regex, servers.chat_regex),
                        enabled = 1,
                        updated_at = excluded.updated_at;
                    """;
                command.Parameters.AddWithValue("$serverId", serverId);
                command.Parameters.AddWithValue("$logPath", logPath);
                command.Parameters.AddWithValue("$chatRegex", (object?)chatRegex ?? DBNull.Value);
                command.Parameters.AddWithValue("$createdAt", GetUtcNowIso());
                command.Parameters.AddWithValue("$updatedAt", GetUtcNowIso());
                command.ExecuteNonQuery();
            }
        }

        public bool SetServerRegex(string serverId, string chatRegex)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    UPDATE servers
                    SET chat_regex = $chatRegex, updated_at = $updatedAt
                    WHERE server_id = $serverId;
                    """;
                command.Parameters.AddWithValue("$chatRegex", chatRegex);
                command.Parameters.AddWithValue("$updatedAt", GetUtcNowIso());
                command.Parameters.AddWithValue("$serverId", serverId);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public void BindGroupServer(long groupId, string serverId)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    INSERT OR IGNORE INTO group_servers (group_id, server_id, created_at)
                    VALUES ($groupId, $serverId, $createdAt);
                    """;
                command.Parameters.AddWithValue("$groupId", groupId);
                command.Parameters.AddWithValue("$serverId", serverId);
                command.Parameters.AddWithValue("$createdAt", GetUtcNowIso());
                command.ExecuteNonQuery();
            }
        }

        public bool UnbindGroupServer(long groupId, string serverId)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "DELETE FROM group_servers WHERE group_id = $groupId AND server_id = $serverId;";
                command.Parameters.AddWithValue("$groupId", groupId);
                command.Parameters.AddWithValue("$serverId", serverId);
                return command.ExecuteNonQuery() > 0;
            }
        }

        public IReadOnlyList<Vs2QQGroupServerRecord> ListGroupServers(long groupId)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    SELECT s.server_id, s.log_path, s.chat_regex, s.enabled
                    FROM servers s
                    JOIN group_servers gs ON gs.server_id = s.server_id
                    WHERE gs.group_id = $groupId
                    ORDER BY s.server_id;
                    """;
                command.Parameters.AddWithValue("$groupId", groupId);
                using var reader = command.ExecuteReader();
                var result = new List<Vs2QQGroupServerRecord>();
                while (reader.Read())
                {
                    result.Add(new Vs2QQGroupServerRecord(
                        reader.GetString(0),
                        reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetString(2),
                        reader.GetInt64(3) == 1));
                }

                return result;
            }
        }

        public IReadOnlyList<Vs2QQServerRecord> ListActiveServers()
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    SELECT s.server_id, s.log_path, s.chat_regex
                    FROM servers s
                    WHERE s.enabled = 1
                      AND EXISTS (
                        SELECT 1 FROM group_servers gs WHERE gs.server_id = s.server_id
                      )
                    ORDER BY s.server_id;
                    """;
                using var reader = command.ExecuteReader();
                var result = new List<Vs2QQServerRecord>();
                while (reader.Read())
                {
                    result.Add(new Vs2QQServerRecord(
                        reader.GetString(0),
                        reader.GetString(1),
                        reader.IsDBNull(2) ? null : reader.GetString(2)));
                }

                return result;
            }
        }

        public IReadOnlyList<long> ListGroupsForServer(string serverId)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    SELECT group_id
                    FROM group_servers
                    WHERE server_id = $serverId
                    ORDER BY group_id;
                    """;
                command.Parameters.AddWithValue("$serverId", serverId);
                using var reader = command.ExecuteReader();
                var result = new List<long>();
                while (reader.Read())
                {
                    result.Add(reader.GetInt64(0));
                }

                return result;
            }
        }

        public (string FileSignature, long Offset)? GetLogOffset(string serverId)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText = "SELECT file_signature, offset FROM log_offsets WHERE server_id = $serverId LIMIT 1;";
                command.Parameters.AddWithValue("$serverId", serverId);
                using var reader = command.ExecuteReader();
                if (!reader.Read())
                {
                    return null;
                }

                return (reader.GetString(0), reader.GetInt64(1));
            }
        }

        public void SetLogOffset(string serverId, string fileSignature, long offset)
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    INSERT INTO log_offsets (server_id, file_signature, offset, updated_at)
                    VALUES ($serverId, $fileSignature, $offset, $updatedAt)
                    ON CONFLICT(server_id) DO UPDATE SET
                        file_signature = excluded.file_signature,
                        offset = excluded.offset,
                        updated_at = excluded.updated_at;
                    """;
                command.Parameters.AddWithValue("$serverId", serverId);
                command.Parameters.AddWithValue("$fileSignature", fileSignature);
                command.Parameters.AddWithValue("$offset", offset);
                command.Parameters.AddWithValue("$updatedAt", GetUtcNowIso());
                command.ExecuteNonQuery();
            }
        }

        private void InitializeSchema()
        {
            lock (_sync)
            {
                using var command = _connection.CreateCommand();
                command.CommandText =
                    """
                    CREATE TABLE IF NOT EXISTS qq_bindings (
                        qq_id INTEGER PRIMARY KEY,
                        player_name TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS servers (
                        server_id TEXT PRIMARY KEY,
                        log_path TEXT NOT NULL,
                        chat_regex TEXT,
                        enabled INTEGER NOT NULL DEFAULT 1,
                        created_at TEXT NOT NULL,
                        updated_at TEXT NOT NULL
                    );

                    CREATE TABLE IF NOT EXISTS group_servers (
                        group_id INTEGER NOT NULL,
                        server_id TEXT NOT NULL,
                        created_at TEXT NOT NULL,
                        PRIMARY KEY (group_id, server_id),
                        FOREIGN KEY (server_id) REFERENCES servers(server_id) ON DELETE CASCADE
                    );

                    CREATE TABLE IF NOT EXISTS log_offsets (
                        server_id TEXT PRIMARY KEY,
                        file_signature TEXT NOT NULL,
                        offset INTEGER NOT NULL,
                        updated_at TEXT NOT NULL,
                        FOREIGN KEY (server_id) REFERENCES servers(server_id) ON DELETE CASCADE
                    );

                    CREATE INDEX IF NOT EXISTS idx_group_servers_server_id
                        ON group_servers (server_id);
                    """;
                command.ExecuteNonQuery();
            }
        }

        private static string GetUtcNowIso()
        {
            return DateTimeOffset.UtcNow.ToString("yyyy-MM-ddTHH:mm:ss", CultureInfo.InvariantCulture);
        }
    }

    private sealed class Vs2QQLogTailer
    {
        private readonly Vs2QQStorage _storage;
        private readonly Vs2QQTalkLineParser _parser;
        private readonly string _defaultEncoding;
        private readonly string _fallbackEncoding;
        private readonly Action<string> _log;
        private readonly Dictionary<string, (string Signature, long Offset)> _offsetCache = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _lineRemainder = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, (string Signature, long Offset)> _companionOffsetCache = new(StringComparer.OrdinalIgnoreCase);
        private readonly Dictionary<string, string> _companionLineRemainder = new(StringComparer.OrdinalIgnoreCase);
        private readonly HashSet<string> _missingWarned = new(StringComparer.OrdinalIgnoreCase);

        public Vs2QQLogTailer(
            Vs2QQStorage storage,
            Vs2QQTalkLineParser parser,
            string defaultEncoding,
            string fallbackEncoding,
            Action<string> log)
        {
            _storage = storage;
            _parser = parser;
            _defaultEncoding = defaultEncoding;
            _fallbackEncoding = fallbackEncoding;
            _log = log;
        }

        public void PrimeServer(string serverId, string logPath)
        {
            var path = new FileInfo(logPath);
            if (!path.Exists)
            {
                return;
            }

            var signature = BuildFileSignature(path);
            var offset = path.Length;
            SetOffset(serverId, signature, offset);
            _lineRemainder.Remove(serverId);

            foreach (var companionPath in GetCompanionLogPaths(path.FullName))
            {
                var companion = new FileInfo(companionPath);
                if (!companion.Exists)
                {
                    continue;
                }

                var companionKey = BuildCompanionKey(serverId, companion.FullName);
                _companionOffsetCache[companionKey] = (BuildFileSignature(companion), companion.Length);
                _companionLineRemainder.Remove(companionKey);
            }
        }

        public IReadOnlyList<Vs2QQTalkMessage> PollServer(Vs2QQServerRecord server)
        {
            var serverId = server.ServerId;
            var path = new FileInfo(server.LogPath);
            if (!path.Exists)
            {
                var missingKey = BuildMissingKey(serverId, path.FullName);
                if (_missingWarned.Add(missingKey))
                {
                    _log($"[warn] VS2QQ 日志文件不存在 server={serverId}: {path.FullName}");
                }

                return [];
            }

            _missingWarned.Remove(BuildMissingKey(serverId, path.FullName));

            var signature = BuildFileSignature(path);
            var fileSize = path.Length;
            if (!_offsetCache.TryGetValue(serverId, out var state))
            {
                var persisted = _storage.GetLogOffset(serverId);
                if (persisted.HasValue)
                {
                    state = persisted.Value;
                    _offsetCache[serverId] = state;
                }
            }

            if (state == default || string.IsNullOrWhiteSpace(state.Signature))
            {
                SetOffset(serverId, signature, fileSize);
                return [];
            }

            long offset = state.Offset;
            if (!string.Equals(state.Signature, signature, StringComparison.Ordinal))
            {
                offset = 0;
                _lineRemainder.Remove(serverId);
            }

            if (offset > fileSize)
            {
                offset = 0;
                _lineRemainder.Remove(serverId);
            }

            byte[] chunk;
            using (var stream = path.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                stream.Seek(offset, SeekOrigin.Begin);
                var remaining = fileSize - offset;
                if (remaining <= 0)
                {
                    chunk = [];
                }
                else
                {
                    chunk = new byte[remaining];
                    var read = stream.Read(chunk, 0, chunk.Length);
                    if (read < chunk.Length)
                    {
                        Array.Resize(ref chunk, read);
                    }
                }
            }

            var newOffset = offset + chunk.Length;
            SetOffset(serverId, signature, newOffset);
            var result = new List<Vs2QQTalkMessage>();
            if (chunk.Length > 0)
            {
                var text = DecodeChunk(chunk);
                if (!string.IsNullOrEmpty(text))
                {
                    if (_lineRemainder.TryGetValue(serverId, out var remainder) && !string.IsNullOrEmpty(remainder))
                    {
                        text = remainder + text;
                    }

                    string[] lines;
                    if (text.EndsWith('\n') || text.EndsWith('\r'))
                    {
                        lines = text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
                        _lineRemainder[serverId] = string.Empty;
                    }
                    else
                    {
                        lines = text.Split(['\r', '\n'], StringSplitOptions.None);
                        if (lines.Length == 0)
                        {
                            _lineRemainder[serverId] = text;
                            lines = [];
                        }
                        else
                        {
                            _lineRemainder[serverId] = lines[^1];
                            lines = lines[..^1];
                        }
                    }

                    foreach (var line in lines)
                    {
                        var parsed = _parser.Parse(line, server.ChatRegex);
                        if (parsed is null)
                        {
                            continue;
                        }

                        result.Add(new Vs2QQTalkMessage(
                            serverId,
                            parsed.Value.Timestamp,
                            parsed.Value.Sender,
                            parsed.Value.Content));
                    }
                }
            }

            foreach (var companionPath in GetCompanionLogPaths(path.FullName))
            {
                var companionMessages = PollCompanionLog(serverId, companionPath, server.ChatRegex);
                if (companionMessages.Count > 0)
                {
                    result.AddRange(companionMessages);
                }
            }

            return result;
        }

        private IReadOnlyList<Vs2QQTalkMessage> PollCompanionLog(string serverId, string logPath, string? chatRegex)
        {
            var path = new FileInfo(logPath);
            var companionKey = BuildCompanionKey(serverId, path.FullName);
            if (!path.Exists)
            {
                var missingKey = BuildMissingKey(serverId, path.FullName);
                _missingWarned.Add(missingKey);
                return [];
            }

            _missingWarned.Remove(BuildMissingKey(serverId, path.FullName));

            var signature = BuildFileSignature(path);
            var fileSize = path.Length;
            _companionOffsetCache.TryGetValue(companionKey, out var state);
            if (state == default || string.IsNullOrWhiteSpace(state.Signature))
            {
                _companionOffsetCache[companionKey] = (signature, fileSize);
                return [];
            }

            long offset = state.Offset;
            if (!string.Equals(state.Signature, signature, StringComparison.Ordinal))
            {
                offset = 0;
                _companionLineRemainder.Remove(companionKey);
            }

            if (offset > fileSize)
            {
                offset = 0;
                _companionLineRemainder.Remove(companionKey);
            }

            byte[] chunk;
            using (var stream = path.Open(FileMode.Open, FileAccess.Read, FileShare.ReadWrite))
            {
                stream.Seek(offset, SeekOrigin.Begin);
                var remaining = fileSize - offset;
                if (remaining <= 0)
                {
                    chunk = [];
                }
                else
                {
                    chunk = new byte[remaining];
                    var read = stream.Read(chunk, 0, chunk.Length);
                    if (read < chunk.Length)
                    {
                        Array.Resize(ref chunk, read);
                    }
                }
            }

            _companionOffsetCache[companionKey] = (signature, offset + chunk.Length);
            if (chunk.Length == 0)
            {
                return [];
            }

            var text = DecodeChunk(chunk);
            if (string.IsNullOrEmpty(text))
            {
                return [];
            }

            if (_companionLineRemainder.TryGetValue(companionKey, out var remainder) && !string.IsNullOrEmpty(remainder))
            {
                text = remainder + text;
            }

            string[] lines;
            if (text.EndsWith('\n') || text.EndsWith('\r'))
            {
                lines = text.Split(['\r', '\n'], StringSplitOptions.RemoveEmptyEntries);
                _companionLineRemainder[companionKey] = string.Empty;
            }
            else
            {
                lines = text.Split(['\r', '\n'], StringSplitOptions.None);
                if (lines.Length == 0)
                {
                    _companionLineRemainder[companionKey] = text;
                    return [];
                }

                _companionLineRemainder[companionKey] = lines[^1];
                lines = lines[..^1];
            }

            var result = new List<Vs2QQTalkMessage>();
            foreach (var line in lines)
            {
                var parsed = _parser.Parse(line, chatRegex);
                if (parsed is null)
                {
                    continue;
                }

                result.Add(new Vs2QQTalkMessage(
                    serverId,
                    parsed.Value.Timestamp,
                    parsed.Value.Sender,
                    parsed.Value.Content));
            }

            return result;
        }

        private string DecodeChunk(byte[] chunk)
        {
            try
            {
                return GetEncoding(_defaultEncoding).GetString(chunk);
            }
            catch
            {
                try
                {
                    return GetEncoding(_fallbackEncoding).GetString(chunk);
                }
                catch
                {
                    return Encoding.UTF8.GetString(chunk);
                }
            }
        }

        private static Encoding GetEncoding(string name)
        {
            if (string.IsNullOrWhiteSpace(name))
            {
                return Encoding.UTF8;
            }

            try
            {
                return Encoding.GetEncoding(name.Trim());
            }
            catch
            {
                return Encoding.UTF8;
            }
        }

        private void SetOffset(string serverId, string fileSignature, long offset)
        {
            _offsetCache[serverId] = (fileSignature, offset);
            _storage.SetLogOffset(serverId, fileSignature, offset);
        }

        private static string BuildFileSignature(FileInfo file)
        {
            // Keep signature stable while file is appended; only rotate when file identity changes.
            return $"{file.FullName}:{file.CreationTimeUtc.Ticks}";
        }

        private static IReadOnlyList<string> GetCompanionLogPaths(string logPath)
        {
            var fileName = Path.GetFileName(logPath);
            var directory = Path.GetDirectoryName(logPath);
            if (string.IsNullOrWhiteSpace(fileName) || string.IsNullOrWhiteSpace(directory))
            {
                return [];
            }

            if (string.Equals(fileName, "server-main.log", StringComparison.OrdinalIgnoreCase))
            {
                return
                [
                    Path.Combine(directory, "server-chat.log"),
                    Path.Combine(directory, "server-audit.log")
                ];
            }

            if (string.Equals(fileName, "server-chat.log", StringComparison.OrdinalIgnoreCase))
            {
                return
                [
                    Path.Combine(directory, "server-main.log"),
                    Path.Combine(directory, "server-audit.log")
                ];
            }

            if (string.Equals(fileName, "server-audit.log", StringComparison.OrdinalIgnoreCase))
            {
                return
                [
                    Path.Combine(directory, "server-main.log"),
                    Path.Combine(directory, "server-chat.log")
                ];
            }

            return [];
        }

        private static string BuildCompanionKey(string serverId, string path)
        {
            return $"{serverId}|{path}";
        }

        private static string BuildMissingKey(string serverId, string path)
        {
            return $"{serverId}@{path}";
        }
    }

    private sealed class Vs2QQTalkLineParser
    {
        private static readonly string[] KnownTimeFormats =
        [
            "yyyy-MM-dd HH:mm:ss",
            "yyyy/MM/dd HH:mm:ss",
            "yyyy-MM-ddTHH:mm:ss",
            "d.M.yyyy HH:mm:ss",
            "M/d/yyyy HH:mm:ss"
        ];

        private static readonly Regex[] DefaultPatterns =
        [
            new(@"^(?<time>\d{1,2}\.\d{1,2}\.\d{4}\s+\d{2}:\d{2}:\d{2})\s*\[(?:Talk|Chat)\]\s*(?:\d+\s*\|\s*)?(?<sender>[^:]{1,64}):\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^(?<time>\d{1,2}\.\d{1,2}\.\d{4}\s+\d{2}:\d{2}:\d{2}).*?\[(?:Talk|Chat)\]\s*(?:\d+\s*\|\s*)?(?<sender>[^:]{1,64}):\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^(?<time>\d{1,2}\.\d{1,2}\.\d{4}\s+\d{2}:\d{2}:\d{2}).*?Message to all in group \d+:\s*(?<sender>[^:]{1,64}):\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^(?<time>\d{1,2}\.\d{1,2}\.\d{4}\s+\d{2}:\d{2}:\d{2}).*?<(?<sender>[^>]{1,64})>\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^\[(?<time>[^\]]+)\]\s*\[(?:Talk|Chat)\]\s*(?:\d+\s*\|\s*)?(?<sender>[^:]{1,64}):\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^(?<time>\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}:\d{2}).*?\[(?:Talk|Chat)\]\s*(?:\d+\s*\|\s*)?(?<sender>[^:]{1,64}):\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^(?<time>\d{4}[-/]\d{2}[-/]\d{2}[ T]\d{2}:\d{2}:\d{2}).*?<(?<sender>[^>]{1,64})>\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^\[(?<time>[^\]]+)\]\s*<(?<sender>[^>]{1,64})>\s*(?<content>.+)$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)
        ];

        private static readonly Regex[] SystemEventPatterns =
        [
            new(@"^(?<time>\d{1,2}\.\d{1,2}\.\d{4}\s+\d{2}:\d{2}:\d{2})\s*\[Event\]\s*(?<player>[^\[\]:]{1,64})\s+\[[^\]]+\](?::\d+)?\s+joins\.$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant),
            new(@"^(?<time>\d{1,2}\.\d{1,2}\.\d{4}\s+\d{2}:\d{2}:\d{2})\s*\[Event\]\s*Player\s+(?<player>[^\.]{1,64})\s+left\.$", RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant)
        ];

        private static readonly Regex DeathAuditPattern =
            new(@"^(?<time>\d{1,2}\.\d{1,2}\.\d{4}\s+\d{2}:\d{2}:\d{2})\s*\[Audit\]\s*(?<player>[^\.]{1,64})\s+died(?:\.\s*Death message:\s*(?<reason>.+))?\s*$",
                RegexOptions.Compiled | RegexOptions.IgnoreCase | RegexOptions.CultureInvariant);

        private readonly Dictionary<string, Regex?> _customPatternCache = new(StringComparer.Ordinal);

        public (string Timestamp, string Sender, string Content)? Parse(string line, string? customRegex)
        {
            if (!string.IsNullOrWhiteSpace(customRegex))
            {
                var customPattern = GetCustomPattern(customRegex);
                if (customPattern is not null)
                {
                    var customMatch = customPattern.Match(line);
                    if (customMatch.Success)
                    {
                        var customResult = ExtractResult(customMatch);
                        if (customResult.HasValue)
                        {
                            return customResult;
                        }
                    }
                }
            }

            foreach (var pattern in DefaultPatterns)
            {
                var match = pattern.Match(line);
                if (!match.Success)
                {
                    continue;
                }

                var result = ExtractResult(match);
                if (result.HasValue)
                {
                    return result;
                }
            }

            var systemEvent = ParseSystemEvent(line);
            if (systemEvent.HasValue)
            {
                return systemEvent;
            }

            return null;
        }

        private Regex? GetCustomPattern(string pattern)
        {
            if (_customPatternCache.TryGetValue(pattern, out var cached))
            {
                return cached;
            }

            try
            {
                var compiled = new Regex(pattern, RegexOptions.Compiled | RegexOptions.CultureInvariant);
                _customPatternCache[pattern] = compiled;
                return compiled;
            }
            catch
            {
                _customPatternCache[pattern] = null;
                return null;
            }
        }

        private static (string Timestamp, string Sender, string Content)? ExtractResult(Match match)
        {
            var sender = match.Groups["sender"].Value.Trim();
            var content = match.Groups["content"].Value.Trim();
            if (string.IsNullOrWhiteSpace(sender) || string.IsNullOrWhiteSpace(content))
            {
                return null;
            }

            var timeRaw = match.Groups["time"].Value.Trim();
            var timestamp = NormalizeTime(timeRaw);
            return (timestamp, sender, content);
        }

        private static (string Timestamp, string Sender, string Content)? ParseSystemEvent(string line)
        {
            var deathMatch = DeathAuditPattern.Match(line);
            if (deathMatch.Success)
            {
                var player = deathMatch.Groups["player"].Value.Trim();
                if (!string.IsNullOrWhiteSpace(player))
                {
                    var timeRaw = deathMatch.Groups["time"].Value.Trim();
                    var timestamp = NormalizeTime(timeRaw);
                    var reason = deathMatch.Groups["reason"].Value.Trim();
                    var content = string.IsNullOrWhiteSpace(reason)
                        ? $"玩家 {player} 死亡"
                        : $"玩家 {player} 死亡：{reason}";

                    return (timestamp, "系统", content);
                }
            }

            foreach (var pattern in SystemEventPatterns)
            {
                var match = pattern.Match(line);
                if (!match.Success)
                {
                    continue;
                }

                var player = match.Groups["player"].Value.Trim();
                if (string.IsNullOrWhiteSpace(player))
                {
                    continue;
                }

                var timeRaw = match.Groups["time"].Value.Trim();
                var timestamp = NormalizeTime(timeRaw);
                var content = line.Contains("joins.", StringComparison.OrdinalIgnoreCase)
                    ? $"玩家 {player} 加入了服务器"
                    : $"玩家 {player} 离开了服务器";

                return (timestamp, "系统", content);
            }

            return null;
        }

        private static string NormalizeTime(string value)
        {
            if (!string.IsNullOrWhiteSpace(value))
            {
                foreach (var format in KnownTimeFormats)
                {
                    if (DateTime.TryParseExact(
                        value,
                        format,
                        CultureInfo.InvariantCulture,
                        DateTimeStyles.AllowWhiteSpaces | DateTimeStyles.AssumeLocal,
                        out var parsed))
                    {
                        return parsed.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                    }
                }

                if (DateTime.TryParse(value, CultureInfo.InvariantCulture, DateTimeStyles.AllowWhiteSpaces, out var freeParsed))
                {
                    return freeParsed.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
                }

                return value;
            }

            return DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss", CultureInfo.InvariantCulture);
        }
    }

    private readonly record struct Vs2QQTalkMessage(string ServerId, string Timestamp, string Sender, string Content);

    private readonly record struct Vs2QQServerRecord(string ServerId, string LogPath, string? ChatRegex);

    private readonly record struct Vs2QQGroupServerRecord(string ServerId, string LogPath, string? ChatRegex, bool Enabled);
}
