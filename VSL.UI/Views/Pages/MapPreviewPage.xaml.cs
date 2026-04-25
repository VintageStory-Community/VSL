using System.Windows;
using System.Windows.Controls;
using System.Windows.Input;
using System.Windows.Media;
using VSL.UI.ViewModels;

namespace VSL.UI.Views.Pages;

public partial class MapPreviewPage : UserControl
{
    private const double MinZoom = 0.2;
    private const double MaxZoom = 16.0;
    private const double ZoomStep = 1.15;

    private bool _isDragging;
    private Point _dragStartPoint;
    private double _dragStartHorizontalOffset;
    private double _dragStartVerticalOffset;
    private ScrollViewer? _dragScrollViewer;

    public MapPreviewPage()
    {
        InitializeComponent();
        Loaded += OnLoaded;
        ResetCoordinateDisplay();
    }

    private void OnLoaded(object sender, RoutedEventArgs e)
    {
        if (Window.GetWindow(this) is MainWindow mainWindow && mainWindow.DataContext is MainViewModel vm)
        {
            DataContext = vm.SaveManagement;
        }
    }

    private void MapPreviewTabs_OnSelectionChanged(object sender, SelectionChangedEventArgs e)
    {
        ResetCoordinateDisplay();
    }

    private void ColorScrollViewer_OnPreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        HandleMouseWheel(ColorScrollViewer, ColorScaleTransform, e);
    }

    private void GrayscaleScrollViewer_OnPreviewMouseWheel(object sender, MouseWheelEventArgs e)
    {
        HandleMouseWheel(GrayscaleScrollViewer, GrayscaleScaleTransform, e);
    }

    private void ColorScrollViewer_OnPreviewMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        BeginDrag(ColorScrollViewer, e);
    }

    private void GrayscaleScrollViewer_OnPreviewMouseLeftButtonDown(object sender, MouseButtonEventArgs e)
    {
        BeginDrag(GrayscaleScrollViewer, e);
    }

    private void ColorScrollViewer_OnPreviewMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        EndDrag(ColorScrollViewer);
    }

    private void GrayscaleScrollViewer_OnPreviewMouseLeftButtonUp(object sender, MouseButtonEventArgs e)
    {
        EndDrag(GrayscaleScrollViewer);
    }

    private void ColorScrollViewer_OnPreviewMouseMove(object sender, MouseEventArgs e)
    {
        HandleMouseMove(ColorScrollViewer, ColorScaleTransform, e);
    }

    private void GrayscaleScrollViewer_OnPreviewMouseMove(object sender, MouseEventArgs e)
    {
        HandleMouseMove(GrayscaleScrollViewer, GrayscaleScaleTransform, e);
    }

    private void ColorScrollViewer_OnMouseLeave(object sender, MouseEventArgs e)
    {
        if (!_isDragging)
        {
            ResetCoordinateDisplay();
        }
    }

    private void GrayscaleScrollViewer_OnMouseLeave(object sender, MouseEventArgs e)
    {
        if (!_isDragging)
        {
            ResetCoordinateDisplay();
        }
    }

    private void HandleMouseWheel(ScrollViewer scrollViewer, ScaleTransform scaleTransform, MouseWheelEventArgs e)
    {
        var oldScale = Math.Max(0.0001, scaleTransform.ScaleX);
        var factor = e.Delta > 0 ? ZoomStep : 1d / ZoomStep;
        var newScale = Math.Clamp(oldScale * factor, MinZoom, MaxZoom);
        if (Math.Abs(newScale - oldScale) < 0.0001)
        {
            return;
        }

        var pointer = e.GetPosition(scrollViewer);
        var contentX = (scrollViewer.HorizontalOffset + pointer.X) / oldScale;
        var contentY = (scrollViewer.VerticalOffset + pointer.Y) / oldScale;

        scaleTransform.ScaleX = newScale;
        scaleTransform.ScaleY = newScale;

        scrollViewer.UpdateLayout();
        scrollViewer.ScrollToHorizontalOffset(contentX * newScale - pointer.X);
        scrollViewer.ScrollToVerticalOffset(contentY * newScale - pointer.Y);

        UpdateCoordinateDisplay(scrollViewer, scaleTransform, pointer);
        e.Handled = true;
    }

    private void BeginDrag(ScrollViewer scrollViewer, MouseButtonEventArgs e)
    {
        if (e.ChangedButton != MouseButton.Left)
        {
            return;
        }

        _isDragging = true;
        _dragScrollViewer = scrollViewer;
        _dragStartPoint = e.GetPosition(scrollViewer);
        _dragStartHorizontalOffset = scrollViewer.HorizontalOffset;
        _dragStartVerticalOffset = scrollViewer.VerticalOffset;
        scrollViewer.CaptureMouse();
        Mouse.OverrideCursor = Cursors.SizeAll;
        e.Handled = true;
    }

    private void EndDrag(ScrollViewer scrollViewer)
    {
        if (!_isDragging || !ReferenceEquals(_dragScrollViewer, scrollViewer))
        {
            return;
        }

        _isDragging = false;
        _dragScrollViewer = null;
        scrollViewer.ReleaseMouseCapture();
        Mouse.OverrideCursor = null;
    }

    private void HandleMouseMove(ScrollViewer scrollViewer, ScaleTransform scaleTransform, MouseEventArgs e)
    {
        var pointer = e.GetPosition(scrollViewer);
        if (_isDragging && ReferenceEquals(_dragScrollViewer, scrollViewer))
        {
            var delta = pointer - _dragStartPoint;
            scrollViewer.ScrollToHorizontalOffset(_dragStartHorizontalOffset - delta.X);
            scrollViewer.ScrollToVerticalOffset(_dragStartVerticalOffset - delta.Y);
        }

        UpdateCoordinateDisplay(scrollViewer, scaleTransform, pointer);
    }

    private void UpdateCoordinateDisplay(ScrollViewer scrollViewer, ScaleTransform scaleTransform, Point pointer)
    {
        if (DataContext is not SaveManagementViewModel vm
            || !vm.HasMapPreview
            || vm.MapPreviewWidth <= 0
            || vm.MapPreviewHeight <= 0)
        {
            ResetCoordinateDisplay();
            return;
        }

        var scale = Math.Max(0.0001, scaleTransform.ScaleX);
        var imageX = (scrollViewer.HorizontalOffset + pointer.X) / scale;
        var imageY = (scrollViewer.VerticalOffset + pointer.Y) / scale;
        var pixelX = (int)Math.Floor(imageX);
        var pixelY = (int)Math.Floor(imageY);

        if (pixelX < 0 || pixelY < 0 || pixelX >= vm.MapPreviewWidth || pixelY >= vm.MapPreviewHeight)
        {
            ResetCoordinateDisplay();
            return;
        }

        var samplingStep = Math.Max(1, vm.MapPreviewSamplingStep);
        var internalBlockX = vm.MapPreviewMinChunkX * 32 + pixelX * samplingStep;
        var internalBlockZ = vm.MapPreviewMinChunkZ * 32 + pixelY * samplingStep;
        var worldBlockX = vm.MapPreviewMapSizeX > 0 ? internalBlockX - vm.MapPreviewMapSizeX / 2 : internalBlockX;
        var worldBlockZ = vm.MapPreviewMapSizeZ > 0 ? internalBlockZ - vm.MapPreviewMapSizeZ / 2 : internalBlockZ;
        var chunkX = FloorDiv(worldBlockX, 32);
        var chunkZ = FloorDiv(worldBlockZ, 32);

        MapCoordinateText.Text = $"维度 {vm.MapPreviewDimension} | X:{worldBlockX} Z:{worldBlockZ} | 区块:{chunkX},{chunkZ}";
    }

    private void ResetCoordinateDisplay()
    {
        MapCoordinateText.Text = "坐标: -";
    }

    private static int FloorDiv(int value, int divisor)
    {
        var quotient = value / divisor;
        var remainder = value % divisor;
        if (remainder != 0 && ((value < 0) ^ (divisor < 0)))
        {
            quotient--;
        }
        return quotient;
    }
}
