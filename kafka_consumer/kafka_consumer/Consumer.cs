using System;
using System.Threading;
using Confluent.Kafka;

namespace t_kafka
{
    public class Consumer
    {
        private static Consumer Instance = null;
        private ConsumerConfig config = null;
        private Thread ListenThread = null;
        private bool IsThreadRun = false;
        private string Topic = "";
        public event Action<string> GetMessageAction;

        public static Consumer GetInstance()
        {
            if (Instance == null)
            {
                Instance = new Consumer();
            }
            return Instance;
        }

        public void SetConfig(string group_id, string server_path)
        {
            this.config = new ConsumerConfig {
                GroupId = group_id,
                BootstrapServers = server_path
            };
        }

        public void SetTopic(string topic)
        {
            this.Topic = topic;
        }

        private void KillThread()
        {
            IsThreadRun = false;
            if (ListenThread != null)
            {
                try
                {
                    if (ListenThread.IsAlive)
                    {
                        ListenThread.Abort();
                    }
                }
                catch
                {

                }
                finally
                {
                    ListenThread = null;
                }
            }
        }

        private void InitThread()
        {
            if (ListenThread == null)
            {
                ListenThread = new Thread(ThreadWork);
                ListenThread.IsBackground = true;
                IsThreadRun = true;
                ListenThread.Start();
            }
        }

        public void Listen()
        {
            KillThread();
            InitThread();
        }

        public void Stop()
        {
            KillThread();
        }

        private void ThreadWork()
        {
            using (IConsumer<Ignore, string> con = new ConsumerBuilder<Ignore, string>(config).Build())
            {
                con.Subscribe(Topic);

                while (IsThreadRun)
                {
                    try
                    {
                        string message = con.Consume(new CancellationTokenSource().Token).Message.Value;
                        GetMessageAction?.Invoke("받음>> " + message);
                    }
                    catch (ThreadAbortException)
                    {

                    }
                    catch (Exception ex)
                    {
                        GetMessageAction?.Invoke("받음 에러>> " + ex.Message);
                    }
                    Thread.Sleep(1);
                }
            }
        }
    

    }
}
