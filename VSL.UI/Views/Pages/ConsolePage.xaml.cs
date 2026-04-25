using System.Collections.Specialized;
using System.Windows;
using System.Windows.Controls;
using VSL.UI.ViewModels;

namespace VSL.UI.Views.Pages;

public partial class ConsolePage : UserControl
{
    private ServerControlViewModel? _boundViewModel;

    public ConsolePage()
    {
        InitializeComponent();
        Loaded += OnLoaded;
        Unloaded += OnUnloaded;
        DataContextChanged += OnDataContextChanged;
    }

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        if (Window.GetWindow(this) is MainWindow mainWindow && mainWindow.DataContext is MainViewModel vm)
        {
            AttachToViewModel(vm.ServerControl);
        }
    }

    private void OnUnloaded(object sender, RoutedEventArgs e)
    {
        DetachFromViewModel();
    }

    private void OnDataContextChanged(object sender, DependencyPropertyChangedEventArgs e)
    {
    }

    private void AttachToViewModel(ServerControlViewModel? vm)
    {
        if (vm is null)
        {
            return;
        }

        _boundViewModel = vm;
        DataContext = vm;
        _boundViewModel.ConsoleLines.CollectionChanged += OnConsoleLinesChanged;
    }

    private void DetachFromViewModel()
    {
        if (_boundViewModel is null)
        {
            return;
        }

        _boundViewModel.ConsoleLines.CollectionChanged -= OnConsoleLinesChanged;
        _boundViewModel = null;
    }

    private void OnConsoleLinesChanged(object? sender, NotifyCollectionChangedEventArgs e)
    {
        if (_boundViewModel?.IsAutoFollow != true)
        {
            return;
        }

        Dispatcher.BeginInvoke(() =>
        {
            if (ConsoleList.Items.Count > 0)
            {
                ConsoleList.ScrollIntoView(ConsoleList.Items[ConsoleList.Items.Count - 1]);
            }
        });
    }
}
