using System;
using System.Windows;
using System.Windows.Controls;
using System.Windows.Shell;
using VSL.UI.ViewModels;
using VSL.UI.Views.Pages;
using Wpf.Ui;
using Wpf.Ui.Controls;

namespace VSL.UI;

public partial class MainWindow : FluentWindow
{
    private readonly MainViewModel _viewModel;
    private readonly ISnackbarService _snackbarService;

    public MainWindow(MainViewModel viewModel, ISnackbarService snackbarService)
    {
        _viewModel = viewModel;
        _snackbarService = snackbarService;
        DataContext = _viewModel;
        InitializeComponent();
        _snackbarService.SetSnackbarPresenter(RootSnackbarPresenter);
        SourceInitialized += MainWindow_SourceInitialized;
        Loaded += MainWindow_Loaded;
    }

    private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
    {
        Loaded -= MainWindow_Loaded;
        ContentFrame.Navigate(new VersionProfilesPage());
        await _viewModel.InitializeAsync();
    }

    private void NavigationButton_Click(object sender, RoutedEventArgs e)
    {
        if (sender is System.Windows.Controls.Button button && button.Tag is string pageName)
        {
            NavigateToPage(pageName);
        }
    }

    private void NavigateToPage(string pageName)
    {
        Type? pageType = pageName switch
        {
            "VersionProfilesPage" => typeof(VersionProfilesPage),
            "ServerSettingsPage" => typeof(ServerSettingsPage),
            "AdvancedJsonPage" => typeof(AdvancedJsonPage),
            "SavesPage" => typeof(SavesPage),
            "MapPreviewPage" => typeof(MapPreviewPage),
            "ModsPage" => typeof(ModsPage),
            "Vs2QQConfigPage" => typeof(Vs2QQConfigPage),
            "ConsolePage" => typeof(ConsolePage),
            "Vs2QQRunnerPage" => typeof(Vs2QQRunnerPage),
            "AppSettingsPage" => typeof(AppSettingsPage),
            "AboutPage" => typeof(AboutPage),
            _ => null
        };

        if (pageType != null)
        {
            ContentFrame.Navigate(Activator.CreateInstance(pageType));
        }
    }

    private void MainWindow_SourceInitialized(object? sender, EventArgs e)
    {
        SourceInitialized -= MainWindow_SourceInitialized;
        ApplyComfortableResizeBorder();
    }

    private void ApplyComfortableResizeBorder()
    {
        var chrome = WindowChrome.GetWindowChrome(this);
        if (chrome is null)
        {
            chrome = new WindowChrome();
            WindowChrome.SetWindowChrome(this, chrome);
        }

        var border = chrome.ResizeBorderThickness;
        chrome.ResizeBorderThickness = new Thickness(
            Math.Max(border.Left, 10),
            Math.Max(border.Top, 10),
            Math.Max(border.Right, 10),
            Math.Max(border.Bottom, 10));
    }
}
