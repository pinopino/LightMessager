using LightMessager;
using LightMessager.Model;
using LightMessager.Tracker;
using Microsoft.AspNetCore.Mvc;
using Newtonsoft.Json;

namespace WebApplication1.Controllers
{
    [ApiController]
    [Route("[controller]")]
    public class WeatherForecastController : ControllerBase
    {
        private static readonly string[] Summaries = new[]
        {
            "Freezing", "Bracing", "Chilly", "Cool", "Mild", "Warm", "Balmy", "Hot", "Sweltering", "Scorching"
        };

        private ILogger<WeatherForecastController> _logger;
        private RabbitMqHub _mqHub;

        public WeatherForecastController(ILogger<WeatherForecastController> logger, RabbitMqHub rabbitMqHub)
        {
            _logger = logger;
            _mqHub = rabbitMqHub;
        }

        [HttpGet]
        public IActionResult Get()
        {
            // RabbitMqHubʹ��֮ǰ��������ã��������ַ�ʽ��
            // 1. ����Ŀ��appsettings.json�ļ�������
            // 2. �ֶ�����һ��IConfigurationRoot����
            // ���������ļ��ĸ�ʽ���ڸ�demo�����appsetting.json����ʾ���������вο�

            // ʹ��RabbitMqHub.Advanced�ӿ�ֱ�ӷ�����Ϣ
            // ͨ����˵����send֮ǰ��Ϣ���з�������Ӧ���Ѿ����ڶ�Ӧ��exchange��queue�ˣ�
            // ���û�У�����ͨ��rabbitmq��web��������ֶ�����֮��Ҳ���Ե���
            // RabbitMqHub.Advanced��ؽӿ�ͨ������������

            // ע�����Ȥ���¼���������ʾ������MessageTracker����ʾһ�ֿ��ܵ�ʹ�ó���
            var connectionStr = "server=localhost;user=root;password=123456;database=test_mq";
            var tracker = new DbMessageTracker(connectionStr);
            _mqHub.MessageSending += (o, e) => tracker.OnAddMessageState(e);
            _mqHub.MessageSendOK += (o, e) => tracker.OnSetMessageState(e);
            _mqHub.MessageSendFailed += (o, e) => tracker.OnSetMessageState(e);

            // ע�⣬��Ϊ����Ҫ��ʾ��Ϣ���͹�����ʧ�ܵĸ��������������������߳�����
            // ���÷��͹��̾������㣬�Ա����˿������Ͽ����ӵ��쳣������
            // 
            // ��ȡһ��channel����������ѭ����ֱ��ʹ��_mqHub.Send���Ա�����ѭ����ֱ�ӷ��ͣ�
            // ������ǻ���Ҫ��ǰ׼����exchange��routekey�ȶ�������������ͼ�������Ĭ��exchange��
            var channel = _mqHub.Advance.GetChannelWrapper(publishConfirm: true);
            var list = new List<Order>();
            var random = new Random();
            for (var i = 0; i < 30; i++)
            {
                var order = new Order
                {
                    OrderId = $"���Զ���{i}",
                    Price = 100M,
                    ProductCode = "Computer",
                    Quantity = 10,
                    CreatedTime = DateTime.Now
                };

                Thread.Sleep(random.Next(500, 2500));

                SendOrders(ref channel, order, tracker);
            }
            channel.WaitForConfirms();

            // �������֮����Լ��tracker��û�з��ͳɹ�����Ϣ���������г����ٴη���
            var retryList = tracker.GetRetryItems();
            _logger.LogWarning("�ܹ���" + retryList.Count + "����Ϣ��Ҫ�ط�");

            // �����������Ϊ����Ա�����Ǻ������Щ��Ϣ���ط���
            // �����ʱ���һ��redelivered��ͷ���ԣ�ֻ����������֣�����ô�����consumeʱʹ�õ���
            var header = new Dictionary<string, object>();
            header.Add("redelivered", true);
            foreach (var item in retryList)
            {
                var order = JsonConvert.DeserializeObject<Order>(item.ToString());
                SendOrders(ref channel, order, tracker, header);
            }
            channel.WaitForConfirms();

            return Ok("ok");
        }

        private void SendOrders(ref ChannelWrapper channel, Order order, DbMessageTracker tracker, IDictionary<string, object> headers = null)
        {
            try
            {
                channel.Publish(order, string.Empty, "Order", true, headers);
            }
            catch
            {
                if (channel.IsClosed)
                {
                    if (!_mqHub.Advance.Connection.IsOpen)
                    {
                        _mqHub = new RabbitMqHub();
                        _mqHub.MessageSending += (o, e) => tracker.OnAddMessageState(e);
                        _mqHub.MessageSendOK += (o, e) => tracker.OnSetMessageState(e);
                        _mqHub.MessageSendFailed += (o, e) => tracker.OnSetMessageState(e);
                    }
                    channel = _mqHub.Advance.GetChannelWrapper(publishConfirm: true);
                }
            }
        }
    }
}