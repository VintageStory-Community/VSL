using System.IO;
using System.Threading;
using System.Windows;
using System.Windows.Threading;
using Microsoft.Extensions.DependencyInjection;
using VSL.Domain;
using VSL.Infrastructure;
using VSL.UI.ViewModels;
using Wpf.Ui;

namespace VSL.UI;

public partial class App : System.Windows.Application
{
    private Mutex? _singleInstanceMutex;
    private ServiceProvider? _serviceProvider;
    private static string StartupLogPath => Path.Combine(WorkspaceLayout.WorkspaceRoot, "logs", "startup.log");

    protected override void OnStartup(StartupEventArgs e)
    {
        base.OnStartup(e);

        AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;
        DispatcherUnhandledException += App_DispatcherUnhandledException;

        try
        {
            WorkspaceLayout.EnsureWorkspaceExists();
            WriteStartupLog($"WorkspaceMode = {(WorkspaceLayout.IsPortableMode ? "Portable" : "Installed")}");
            WriteStartupLog($"WorkspaceRoot = {WorkspaceLayout.WorkspaceRoot}");

            _singleInstanceMutex = new Mutex(true, "VSL_SINGLE_INSTANCE_MUTEX", out var createdNew);
            if (!createdNew)
            {
                MessageBox.Show("VSL 已在运行。", "提示", MessageBoxButton.OK, MessageBoxImage.Information);
                Shutdown();
                return;
            }

            var services = new ServiceCollection();
            services.AddVslInfrastructure();
            services.AddSingleton<ISnackbarService, SnackbarService>();
            
            services.AddSingleton<VersionManagementViewModel>();
            services.AddSingleton<ProfileManagementViewModel>();
            services.AddSingleton<ServerControlViewModel>();
            services.AddSingleton<ModManagementViewModel>();
            services.AddSingleton<SaveManagementViewModel>();
            services.AddSingleton<SettingsViewModel>();
            services.AddSingleton<Vs2QQRunnerViewModel>();
            services.AddSingleton<MainViewModel>();
            services.AddSingleton<MainWindow>();

            _serviceProvider = services.BuildServiceProvider();

            var mainWindow = _serviceProvider.GetRequiredService<MainWindow>();
            mainWindow.Show();
        }
        catch (Exception ex)
        {
            WriteStartupLog("Startup failed.", ex);
            MessageBox.Show(
                $"VSL 启动失败：{ex.Message}\n\n日志文件：{StartupLogPath}",
                "VSL 启动失败",
                MessageBoxButton.OK,
                MessageBoxImage.Error);
            Shutdown(-1);
        }
    }

    protected override async void OnExit(ExitEventArgs e)
    {
        try
        {
            if (_serviceProvider is not null)
            {
                var mainVm = _serviceProvider.GetService<MainViewModel>();
                if (mainVm is not null)
                {
                    await mainVm.DisposeAsync();
                }

                var serverControlVm = _serviceProvider.GetService<ServerControlViewModel>();
                if (serverControlVm is not null)
                {
                    await serverControlVm.DisposeAsync();
                }

                var versionVm = _serviceProvider.GetService<VersionManagementViewModel>();
                versionVm?.Cleanup();

                var profileVm = _serviceProvider.GetService<ProfileManagementViewModel>();
                profileVm?.Cleanup();

                var modVm = _serviceProvider.GetService<ModManagementViewModel>();
                modVm?.Cleanup();

                var saveVm = _serviceProvider.GetService<SaveManagementViewModel>();
                saveVm?.Cleanup();

                var settingsVm = _serviceProvider.GetService<SettingsViewModel>();
                settingsVm?.Cleanup();

                await _serviceProvider.DisposeAsync();
            }
        }
        finally
        {
            try
            {
                _singleInstanceMutex?.ReleaseMutex();
            }
            catch
            {
                // Ignore if mutex is not owned.
            }

            _singleInstanceMutex?.Dispose();
            _singleInstanceMutex = null;

            base.OnExit(e);
        }
    }

    private void App_DispatcherUnhandledException(object sender, DispatcherUnhandledExceptionEventArgs e)
    {
        WriteStartupLog("DispatcherUnhandledException", e.Exception);
        MessageBox.Show(
            $"发生未处理异常：{e.Exception.Message}\n\n日志文件：{StartupLogPath}",
            "VSL 异常",
            MessageBoxButton.OK,
            MessageBoxImage.Error);
        e.Handled = true;
        Shutdown(-2);
    }

    private static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
    {
        if (e.ExceptionObject is Exception ex)
        {
            WriteStartupLog("UnhandledException", ex);
        }
        else
        {
            WriteStartupLog($"UnhandledException: {e.ExceptionObject}");
        }
    }

    private static void WriteStartupLog(string message, Exception? ex = null)
    {
        try
        {
            var dir = Path.GetDirectoryName(StartupLogPath);
            if (!string.IsNullOrWhiteSpace(dir))
            {
                Directory.CreateDirectory(dir);
            }

            using var writer = new StreamWriter(StartupLogPath, append: true);
            writer.WriteLine($"[{DateTime.Now:yyyy-MM-dd HH:mm:ss.fff}] {message}");
            if (ex is not null)
            {
                writer.WriteLine(ex.ToString());
            }
        }
        catch
        {
            // Ignore logging failures.
        }
    }
}
