using Confluent.Kafka;
using System;
using System.Collections.Generic;
using System.Threading;

namespace t_kafka
{
    public class Producer
    {
        private static Producer Instance = null;
        private ProducerConfig config = null;
        private Thread SendThread = null;
        private bool IsThreadRun = false;
        private string Topic = "";
        public event Action<string> SendMessageAction;
        private Queue<string> SendMessageQueue = new Queue<string>();

        public static Producer GetInstance()
        {
            if (Instance == null)
            {
                Instance = new Producer();
            }
            return Instance;
        }

        public void AddMessage(string message)
        {
            SendMessageQueue.Enqueue(message);
        }

        public void SetConfig(string server_path)
        {
            this.config = new ProducerConfig
            {
                //GroupId = group_id,
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
            if (SendThread != null)
            {
                try
                {
                    if (SendThread.IsAlive)
                    {
                        SendThread.Abort();
                    }
                }
                catch
                {

                }
                finally
                {
                    SendThread = null;
                }
            }
        }

        private void InitThread()
        {
            if (SendThread == null)
            {
                SendThread = new Thread(ThreadWork);
                SendThread.IsBackground = true;
                IsThreadRun = true;
                SendThread.Start();
            }
        }

        private void ThreadWork()
        {
            using (IProducer<string, string> pro = new ProducerBuilder<string, string>(config).Build())
            {
                while (IsThreadRun)
                {
                    if (SendMessageQueue.Count > 0)
                    {
                        try
                        {
                            string message = SendMessageQueue.Dequeue();
                            pro.Produce(Topic, new Message<string, string> { Key = null, Value = message });
                            SendMessageAction?.Invoke("보냄>> " + message);
                        }
                        catch (ThreadAbortException)
                        {

                        }
                        catch (Exception ex)
                        {
                            SendMessageAction?.Invoke("보냄 에러>> " + ex.Message);
                        }
                    }
                    Thread.Sleep(100);
                }
            }            
        }

        public void Send()
        {
            KillThread();
            InitThread();
        }

        public void Stop()
        {
            KillThread();
        }
    }
}
