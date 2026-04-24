using System.Windows;
using System.Windows.Shell;
using VSL.UI.ViewModels;
using Wpf.Ui;
using Wpf.Ui.Controls;

namespace VSL.UI;

public partial class MainWindow : FluentWindow
{
    private readonly MainViewModel _viewModel;
    private readonly ISnackbarService _snackbarService;
    private readonly NavigationViewItem[] _allNavItems;

    public MainWindow(MainViewModel viewModel, ISnackbarService snackbarService)
    {
        _viewModel = viewModel;
        _snackbarService = snackbarService;
        DataContext = _viewModel;
        InitializeComponent();
        _snackbarService.SetSnackbarPresenter(RootSnackbarPresenter);
        _allNavItems =
        [
            NavVersionProfiles,
            NavSettings,
            NavAdvancedJson,
            NavSaves,
            NavMapPreview,
            NavMods,
            NavVs2QQConfig,
            NavConsole,
            NavVs2QQRunner,
            NavAppSettings,
            NavAbout
        ];
        SourceInitialized += MainWindow_SourceInitialized;
        Loaded += MainWindow_Loaded;
    }

    private async void MainWindow_Loaded(object sender, RoutedEventArgs e)
    {
        Loaded -= MainWindow_Loaded;
        await _viewModel.InitializeAsync();
    }

    private void RootNavigationView_OnSelectionChanged(NavigationView sender, RoutedEventArgs args)
    {
        if (sender.SelectedItem is NavigationViewItem selectedByControl &&
            selectedByControl.Tag is string selectedTag &&
            !string.IsNullOrWhiteSpace(selectedTag))
        {
            ApplyNavSelection(selectedByControl, selectedTag);
            return;
        }

        if (args.OriginalSource is NavigationViewItem raisedItem &&
            raisedItem.Tag is string raisedTag &&
            !string.IsNullOrWhiteSpace(raisedTag))
        {
            ApplyNavSelection(raisedItem, raisedTag);
        }
    }

    private void NavigationItem_OnClick(object sender, RoutedEventArgs e)
    {
        if (sender is NavigationViewItem item &&
            item.Tag is string tag &&
            !string.IsNullOrWhiteSpace(tag))
        {
            ApplyNavSelection(item, tag);
        }
    }

    private void WindowMinimizeButton_OnClick(object sender, RoutedEventArgs e)
    {
        WindowState = WindowState.Minimized;
    }

    private void WindowMaximizeButton_OnClick(object sender, RoutedEventArgs e)
    {
        WindowState = WindowState == WindowState.Maximized
            ? WindowState.Normal
            : WindowState.Maximized;
    }

    private void WindowCloseButton_OnClick(object sender, RoutedEventArgs e)
    {
        Close();
    }

    private void MainWindow_SourceInitialized(object? sender, EventArgs e)
    {
        SourceInitialized -= MainWindow_SourceInitialized;
        ApplyComfortableResizeBorder();
    }

    private void ApplyNavSelection(NavigationViewItem activeItem, string tag)
    {
        _viewModel.SelectedNavKey = tag;
        foreach (var item in _allNavItems)
        {
            item.IsActive = ReferenceEquals(item, activeItem);
        }
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
