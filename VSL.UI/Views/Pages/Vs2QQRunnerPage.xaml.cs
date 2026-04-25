using System.Collections.Specialized;
using System.Windows;
using System.Windows.Controls;
using VSL.UI.ViewModels;

namespace VSL.UI.Views.Pages;

public partial class Vs2QQRunnerPage : UserControl
{
    private Vs2QQRunnerViewModel? _boundViewModel;

    public Vs2QQRunnerPage()
    {
        InitializeComponent();
        Loaded += OnLoaded;
        Unloaded += OnUnloaded;
    }

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        if (Window.GetWindow(this) is MainWindow mainWindow && mainWindow.DataContext is MainViewModel vm)
        {
            AttachToViewModel(vm.Vs2QQRunner);
        }
    }

    private void OnUnloaded(object sender, RoutedEventArgs e)
    {
        DetachFromViewModel();
    }

    private void AttachToViewModel(Vs2QQRunnerViewModel? vm)
    {
        if (vm is null)
        {
            return;
        }

        _boundViewModel = vm;
        DataContext = vm;
        _boundViewModel.Vs2QQConsoleLines.CollectionChanged += OnConsoleLinesChanged;
    }

    private void DetachFromViewModel()
    {
        if (_boundViewModel is null)
        {
            return;
        }

        _boundViewModel.Vs2QQConsoleLines.CollectionChanged -= OnConsoleLinesChanged;
        _boundViewModel = null;
    }

    private void OnConsoleLinesChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        if (_boundViewModel?.IsVs2QQConsoleAutoFollow != true)
        {
            return;
        }

        Dispatcher.BeginInvoke(() =>
        {
            if (Vs2QQConsoleList.Items.Count > 0)
            {
                Vs2QQConsoleList.ScrollIntoView(Vs2QQConsoleList.Items[Vs2QQConsoleList.Items.Count - 1]);
            }
        });
    }
}
