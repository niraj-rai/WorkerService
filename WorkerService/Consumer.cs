using System;
using System.Threading;

namespace WorkerService
{
    public interface IConsumer
    {
        event EventHandler<int>? HandleMessage;
        event Action<string>? HandleError;

        void Consume(CancellationToken cancellationToken = default);
        int ConsumeNext(int offset, CancellationToken cancellationToken = default);
        void Pause(int duration = 10); // Duration in seconds
        void Resume();
    }

    public class Consumer : IConsumer
    {
        public event EventHandler<int>? HandleMessage;
        public event Action<string>? HandleError;
        public delegate Task onConsume(int messageId);

        volatile bool _isPaused;
        volatile int _pauseDuration; // Duration in ms
        long _nextResumeOn = DateTime.Now.Ticks;

        public void Pause(int duration = 10)
        {
            if (!_isPaused)
            {
                _nextResumeOn = DateTime.Now.AddSeconds(duration).Ticks;
                _isPaused = true;
            }
        }

        public void Resume()
        {
            _pauseDuration = 0;
            _isPaused = false;
        }

        public void Consume(CancellationToken cancellationToken = default)
        {
            int counter = 1;
            try
            {
                while (!cancellationToken.IsCancellationRequested)
                {
                    if (!_isPaused)
                    {
                        Thread.Sleep(10);
                        HandleMessage?.Invoke(this, counter);
                        counter++;
                    }
                    else
                    {
                        if (DateTime.Now.Ticks > _nextResumeOn)
                            Resume();
                    }
                }
            }
            catch (Exception ex)
            {
                counter++;
                HandleError?.Invoke($"An error occured while processing MessageId # {counter} \n Error Details: {ex.Message}");
            }
        }

        public int ConsumeNext(int offset, CancellationToken cancellationToken = default)
        {
            try
            {
                //if (cancellationToken.IsCancellationRequested)
                //    cancellationToken.ThrowIfCancellationRequested();
                Thread.Sleep(10);
                return offset + 1;
            }
            catch (Exception ex)
            {
                HandleError?.Invoke($"An error occured while processing MessageId # {offset} \n Error Details: {ex.Message}");
                return offset;
            }
        }
    }
}

