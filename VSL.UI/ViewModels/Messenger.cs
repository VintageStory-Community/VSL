namespace VSL.UI.ViewModels;

public interface IMessenger
{
    void Send<TMessage>(TMessage message) where TMessage : class;
    void Register<TMessage>(object recipient, Action<TMessage> handler) where TMessage : class;
    void Unregister<TMessage>(object recipient) where TMessage : class;
    void UnregisterAll(object recipient);
}

public sealed class Messenger : IMessenger
{
    private static readonly Lazy<Messenger> _default = new(() => new Messenger());
    public static IMessenger Default => _default.Value;

    private readonly Dictionary<Type, List<WeakReference>> _recipients = [];
    private readonly object _lock = new();

    public void Send<TMessage>(TMessage message) where TMessage : class
    {
        lock (_lock)
        {
            if (!_recipients.TryGetValue(typeof(TMessage), out var list))
            {
                return;
            }

            var deadRefs = new List<WeakReference>();
            foreach (var weakRef in list)
            {
                if (weakRef.Target is IMessageHandler<TMessage> handler)
                {
                    handler.Handle(message);
                }
                else
                {
                    deadRefs.Add(weakRef);
                }
            }

            foreach (var dead in deadRefs)
            {
                list.Remove(dead);
            }
        }
    }

    public void Register<TMessage>(object recipient, Action<TMessage> handler) where TMessage : class
    {
        lock (_lock)
        {
            if (!_recipients.TryGetValue(typeof(TMessage), out var list))
            {
                list = [];
                _recipients[typeof(TMessage)] = list;
            }

            list.Add(new WeakReference(new MessageHandler<TMessage>(recipient, handler)));
        }
    }

    public void Unregister<TMessage>(object recipient) where TMessage : class
    {
        lock (_lock)
        {
            if (!_recipients.TryGetValue(typeof(TMessage), out var list))
            {
                return;
            }

            list.RemoveAll(wr =>
            {
                if (wr.Target is IMessageHandler<TMessage> handler)
                {
                    return handler.Recipient == recipient;
                }
                return true;
            });
        }
    }

    public void UnregisterAll(object recipient)
    {
        lock (_lock)
        {
            foreach (var list in _recipients.Values)
            {
                list.RemoveAll(wr =>
                {
                    if (wr.Target is IMessageHandler handler)
                    {
                        return handler.Recipient == recipient;
                    }
                    return true;
                });
            }
        }
    }

    private interface IMessageHandler
    {
        object Recipient { get; }
    }

    private interface IMessageHandler<TMessage> : IMessageHandler
    {
        void Handle(TMessage message);
    }

    private sealed class MessageHandler<TMessage>(object recipient, Action<TMessage> handler) : IMessageHandler<TMessage>
    {
        public object Recipient => recipient;
        public void Handle(TMessage message) => handler(message);
    }
}

public class ObservableObjectWithMessenger : ObservableObject
{
    private readonly IMessenger _messenger;

    protected ObservableObjectWithMessenger() : this(Messenger.Default) { }

    protected ObservableObjectWithMessenger(IMessenger messenger)
    {
        _messenger = messenger;
    }

    protected void SendMessage<TMessage>(TMessage message) where TMessage : class
    {
        _messenger.Send(message);
    }

    protected void RegisterForMessage<TMessage>(Action<TMessage> handler) where TMessage : class
    {
        _messenger.Register(this, handler);
    }

    protected void UnregisterFromMessage<TMessage>() where TMessage : class
    {
        _messenger.Unregister<TMessage>(this);
    }

    public virtual void Cleanup()
    {
        _messenger.UnregisterAll(this);
    }
}
