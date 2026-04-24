using System;
using System.Linq;
using System.Windows.Controls;
using System.Windows.Threading;

namespace VSL.UI.Views.Pages;

public partial class VersionProfilesPage : UserControl
{
    public VersionProfilesPage()
    {
        InitializeComponent();
        Loaded += (_, _) => ScheduleListColumnResize();
        SizeChanged += (_, _) => ScheduleListColumnResize();
        InstalledVersionsList.SizeChanged += (_, _) => ResizeListColumns();
        ProfilesList.SizeChanged += (_, _) => ResizeListColumns();
    }

    private void ScheduleListColumnResize()
    {
        _ = Dispatcher.BeginInvoke(DispatcherPriority.Loaded, new Action(ResizeListColumns));
    }

    private void ResizeListColumns()
    {
        ResizeGridViewColumns(
            InstalledVersionsList,
            weights: [0.18, 0.62, 0.20],
            minimumWidths: [130, 420, 140]);

        ResizeGridViewColumns(
            ProfilesList,
            weights: [0.20, 0.14, 0.66],
            minimumWidths: [160, 120, 420]);
    }

    private static void ResizeGridViewColumns(ListView listView, double[] weights, double[] minimumWidths)
    {
        if (listView.View is not GridView gridView ||
            gridView.Columns.Count != weights.Length ||
            weights.Length != minimumWidths.Length)
        {
            return;
        }

        var availableWidth = listView.ActualWidth - 26;
        if (availableWidth <= 0)
        {
            return;
        }

        var minimumTotal = minimumWidths.Sum();
        if (availableWidth <= minimumTotal)
        {
            var shrinkScale = availableWidth / minimumTotal;
            for (var i = 0; i < gridView.Columns.Count; i++)
            {
                gridView.Columns[i].Width = Math.Max(60, minimumWidths[i] * shrinkScale);
            }

            return;
        }

        var extraWidth = availableWidth - minimumTotal;
        var weightTotal = weights.Sum();
        for (var i = 0; i < gridView.Columns.Count; i++)
        {
            var weightedExtra = extraWidth * (weights[i] / weightTotal);
            gridView.Columns[i].Width = minimumWidths[i] + weightedExtra;
        }
    }
}
