using System;
using System.Collections.Generic;
using System.Windows.Controls;

namespace VSL.UI.Services
{
    public interface INavigationService
    {
        event EventHandler<Type>? Navigated;
        Type? CurrentPage { get; }
        void NavigateTo<T>() where T : Page;
        void NavigateTo(Type pageType);
        bool CanGoBack { get; }
        void GoBack();
    }

    public class NavigationService : INavigationService
    {
        private Frame? _frame;
        private readonly Stack<Type> _navigationHistory = new();

        public event EventHandler<Type>? Navigated;

        public Type? CurrentPage { get; private set; }

        public bool CanGoBack => _navigationHistory.Count > 1;

        public void SetFrame(Frame frame)
        {
            _frame = frame;
        }

        public void NavigateTo<T>() where T : Page
        {
            NavigateTo(typeof(T));
        }

        public void NavigateTo(Type pageType)
        {
            if (_frame == null || pageType == null)
                return;

            if (CurrentPage != null && CurrentPage != pageType)
            {
                _navigationHistory.Push(CurrentPage);
            }

            var page = Activator.CreateInstance(pageType) as Page;
            if (page != null)
            {
                _frame.Navigate(page);
                CurrentPage = pageType;
                Navigated?.Invoke(this, pageType);
            }
        }

        public void GoBack()
        {
            if (_frame == null || !CanGoBack)
                return;

            _navigationHistory.Pop();
            
            if (_navigationHistory.Count > 0)
            {
                var previousPage = _navigationHistory.Peek();
                NavigateTo(previousPage);
                _navigationHistory.Pop();
            }
        }
    }
}
