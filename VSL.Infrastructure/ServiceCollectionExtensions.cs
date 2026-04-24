using Microsoft.Extensions.DependencyInjection;
using VSL.Application;
using VSL.Infrastructure.Services;

namespace VSL.Infrastructure;

public static class ServiceCollectionExtensions
{
    public static IServiceCollection AddVslInfrastructure(this IServiceCollection services)
    {
        services.AddHttpClient<IVersionCatalogService, VersionCatalogService>();
        services.AddHttpClient<IPackageService, PackageService>();

        services.AddSingleton<ILauncherSettingsService, LauncherSettingsService>();
        services.AddSingleton<IProfileService, ProfileService>();
        services.AddSingleton<IServerConfigService, ServerConfigService>();
        services.AddSingleton<ISaveService, SaveService>();
        services.AddSingleton<IMapPreviewService, MapPreviewService>();
        services.AddSingleton<IModService, ModService>();
        services.AddSingleton<IServerProcessService, ServerProcessService>();
        services.AddSingleton<IVs2QQProcessService, Vs2QQProcessService>();
        services.AddSingleton<ILogTailService, LogTailService>();

        return services;
    }
}
