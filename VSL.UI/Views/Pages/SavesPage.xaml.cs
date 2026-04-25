using System.Windows;
using System.Windows.Controls;
using VSL.UI.ViewModels;

namespace VSL.UI.Views.Pages;

public partial class SavesPage : UserControl
{
    public SavesPage()
    {
        InitializeComponent();
        Loaded += OnLoaded;
    }

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        if (Window.GetWindow(this) is MainWindow mainWindow && mainWindow.DataContext is MainViewModel vm)
        {
            DataContext = vm.SaveManagement;
        }
    }
}
