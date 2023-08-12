namespace WorkerService;

public class Worker : BackgroundService
{
    private readonly ILogger<Worker> _logger;
    private readonly Consumer _consumer;
    private CancellationToken _cancellationToken;
    private int _offset = 0;

    private bool _sequential = false;

    public Worker(ILogger<Worker> logger)
    {
        _logger = logger;
        _consumer = new Consumer();
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        ThreadPool.SetMaxThreads(2, 2);
        _cancellationToken = stoppingToken;
        while (!stoppingToken.IsCancellationRequested)
        {
            //_logger.LogInformation("Worker running at: {time}", DateTimeOffset.Now);
            //await Task.Delay(1000, stoppingToken);
            if (_sequential)
            {
                _offset = _consumer.ConsumeNext(_offset, cancellationToken: stoppingToken);
                await HandleMessage(_offset);
            }
            else
            {
                var thread = Thread.CurrentThread;
                _consumer.HandleMessage += _consumer_HandleMessage;
                _consumer.HandleError += _consumer_HandleError;
                _consumer.Consume(cancellationToken: stoppingToken);
                _logger.LogInformation("This will never come here.-----");
                await Task.CompletedTask;
            }
        }
    }

    private async Task HandleMessage(int messageId)
    {
        IHandler<String> handler = new MessageHandler();
        await handler.Handle(messageId.ToString(), _cancellationToken);
        _logger.LogInformation($"MessageId: {messageId} send to handler");
    }

    private async void _consumer_HandleMessage(object? sender, int messageId)
    {
        var totalThreads = ThreadPool.ThreadCount;
        ThreadPool.GetAvailableThreads(out int availableThread, out int completionThreads);
        _logger.LogInformation("Thread Pool: TotalThreads={totalThread}, AvailableThreads={} ", totalThreads, availableThread);
        IHandler<String> handler = new MessageHandler();
        try
        {
            await handler.Handle(messageId.ToString(), _cancellationToken);
        }
        catch (Exception ex)
        {
            _logger.LogError(ex.Message);
            _logger.LogInformation("Pausing the consumer");
            _consumer.Pause(20);
            _logger.LogInformation("Consumer paused for 20 Sec");
        }
        _logger.LogInformation($"MessageId: {messageId} send to handler");
        //ThreadPool.QueueUserWorkItem(async (messageId) => { await handler.Handle(messageId?.ToString(), _cancellationToken); }, messageId);
    }

    private void _consumer_HandleError(string messageId)
    {
        _logger.LogError($"An error occured while processing message Id: {messageId}");
    }
}

