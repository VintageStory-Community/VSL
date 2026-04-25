namespace VSL.UI.ViewModels.Messages;

using VSL.Domain;

public sealed record ProfileSelectedMessage(ServerProfile? Profile);

public sealed record VersionListChangedMessage;

public sealed record ModListChangedMessage;

public sealed record SaveListChangedMessage;

public sealed record ServerStatusChangedMessage(VSL.Domain.ServerRuntimeStatus Status);

public sealed record Vs2QQStatusChangedMessage(VSL.Domain.Vs2QQRuntimeStatus Status);

public sealed record ShowToastMessage(string Message, string? Title = null);

public sealed record ShowErrorMessage(string Message, string? Detail = null);

public sealed record NavigationRequestMessage(string NavKey);
