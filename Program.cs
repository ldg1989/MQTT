using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet.Protocol;

namespace MQTT
{

    /*
         emqx windows本地部署
    下载地址：https://www.emqx.com/zh/downloads/broker/v5.3.2

    1：打开文件夹 emqx-5.3.2-windows-amd64/bin
    2：在地址栏里输入cmd
    3：在命令行中输入  emqx.cmd start/stop
    4：在浏览器中 http://localhost:18083
    5: 默认账号密码 admin/1qazxsw2!@ 

    6：https://mqttx.app/zh/downloads MQTTX 下载
    7：MQTTX 连接 emqx


     */


    class Program
    {
        // MQTT Broker 配置
        private const string BrokerIp = "127.0.0.1";
        private const int BrokerPort = 1883;
        private const string TopicDeviceData = "device/sensor/data";  // 下位机上报传感器数据
        private const string TopicDeviceCmd = "device/cmd/control";   // 上位机下发控制指令

        static async Task Main(string[] args)
        {
            // 1. 创建托管MQTT客户端（自动重连、心跳、断线恢复）
            var mqttFactory = new MqttFactory();
            var managedClient = mqttFactory.CreateManagedMqttClient();

            // 2. 客户端连接配置
            var mqttClientOptions = new MqttClientOptionsBuilder()
                .WithTcpServer(BrokerIp, BrokerPort)
                .WithClientId($"Client_{Guid.NewGuid():N}") // 唯一客户端ID
                .WithCleanSession()
                .WithWillTopic(TopicDeviceData)              // 遗言消息：客户端异常断开时由 Broker 代发
                .WithWillPayload("设备异常离线")              // MQTTnet 4.x 没有 WithWillMessage，需拆成 Topic/Payload/QoS
                .WithWillQualityOfServiceLevel(MqttQualityOfServiceLevel.AtMostOnce)
                .WithWillRetain(false)// 遗言消息是否持久化
                .WithWillDelayInterval(10)// 遗言消息延迟10秒发送
                .Build();

            var managedOptions = new ManagedMqttClientOptionsBuilder()
                .WithClientOptions(mqttClientOptions)
                .WithAutoReconnectDelay(TimeSpan.FromSeconds(3)) // 断线3秒自动重连
                .Build();

            // 3. 订阅消息回调（上位机监听指令/数据入口）
            managedClient.ApplicationMessageReceivedAsync += OnMessageReceived;

            // 4. 启动连接
            Console.WriteLine("正在连接MQTT Broker...");
            await managedClient.StartAsync(managedOptions);

            // 5. 订阅主题
            await managedClient.SubscribeAsync(TopicDeviceData, MqttQualityOfServiceLevel.AtLeastOnce);
            await managedClient.SubscribeAsync(TopicDeviceCmd, MqttQualityOfServiceLevel.AtLeastOnce);
            Console.WriteLine("连接成功，已订阅主题");

            // 6. 模拟下位机循环上报传感器数据（发布者逻辑）
            _ = Task.Run(async () =>
            {
                int count = 0;
                while (true)
                {
                    if (managedClient.IsConnected)
                    {
                        count++;
                        // 模拟传感器JSON数据
                        string payload = $"{{\"deviceId\":\"dev001\",\"temp\":{25 + Random.Shared.Next(5)},\"hum\":{40 + Random.Shared.Next(10)},\"count\":{count}}}";
                        var msg = new MqttApplicationMessageBuilder()
                            .WithTopic(TopicDeviceData)
                            .WithPayload(payload)
                            .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                            .Build();

                        await managedClient.EnqueueAsync(msg);
                        Console.WriteLine($"\n【下位机上报】{payload}");
                    }
                    await Task.Delay(2000);
                }
            });

            // 7. 控制台输入，模拟上位机下发控制指令
            Console.WriteLine("\n输入指令下发给下位机，exit退出程序：");
            while (true)
            {
                string input = Console.ReadLine();
                if (string.IsNullOrWhiteSpace(input))
                    continue;
                if (input.Equals("exit", StringComparison.OrdinalIgnoreCase))
                    break;

                // 发布控制指令
                var cmdMsg = new MqttApplicationMessageBuilder()
                    .WithTopic(TopicDeviceCmd)
                    .WithPayload(input)
                    .WithQualityOfServiceLevel(MqttQualityOfServiceLevel.AtLeastOnce)
                    .Build();
                await managedClient.EnqueueAsync(cmdMsg);
                Console.WriteLine($"【上位机下发指令】{input}");
            }

            // 关闭客户端
            await managedClient.StopAsync();
            Console.WriteLine("MQTT客户端已断开");
        }

        /// <summary>
        /// 收到MQTT消息回调
        /// </summary>
        static async Task OnMessageReceived(MqttApplicationMessageReceivedEventArgs e)
        {
            var topic = e.ApplicationMessage.Topic;
            var payload = System.Text.Encoding.UTF8.GetString(e.ApplicationMessage.PayloadSegment);

            if (topic == TopicDeviceData)
            {
                Console.WriteLine($"[上位机接收传感器] 主题:{topic} 数据:{payload}");
            }
            else if (topic == TopicDeviceCmd)
            {
                Console.WriteLine($"[下位机收到控制指令] 指令:{payload}");
            }

            await Task.CompletedTask;
        }
    }
}