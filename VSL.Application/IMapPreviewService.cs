using VSL.Domain;

namespace VSL.Application;

public interface IMapPreviewService
{
    Task<OperationResult<MapPreviewData>> LoadMapPreviewAsync(
        ServerProfile profile,
        string? saveFilePath = null,
        CancellationToken cancellationToken = default);
}
