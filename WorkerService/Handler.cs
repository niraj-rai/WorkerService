using System;
namespace WorkerService
{
    public interface IHandler<TRequest> where TRequest : class
    {
        Task Handle(TRequest? messageId, CancellationToken cancellationToken = default);
    }

    public class MessageHandler : IHandler<String>
    {
        public async Task Handle(string? messageId, CancellationToken cancellationToken = default)
        {
            if (cancellationToken.IsCancellationRequested)
                Console.WriteLine($"------ Cancelled Processing Message Id: {messageId}");
            else
            {
                var seconds = DateTime.Now.Second;
                int.TryParse(messageId, out int id);
                if (id % 10 == 0)
                {
                    throw new Exception($"Failed to process request with exception MessageId: {messageId}");
                }
                else
                {
                    var thread = Thread.CurrentThread;
                    Console.WriteLine($"------ Processing Message Id: {messageId} on Thread: Id={thread.ManagedThreadId}, Name={thread.Name}");
                    await Task.Delay(10000, cancellationToken);
                    Console.WriteLine($"------ Processing Completed for Message Id: {messageId}");
                }
            }
        }
    }
}