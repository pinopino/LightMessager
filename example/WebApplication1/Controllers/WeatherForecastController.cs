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
            // RabbitMqHub使用之前会加载配置，共有两种方式：
            // 1. 在项目的appsettings.json文件中配置
            // 2. 手动传入一个IConfigurationRoot对象
            // 具体配置文件的格式，在该demo程序的appsetting.json中有示例，请自行参考

            // 使用RabbitMqHub.Advanced接口直接发送消息
            // 通常来说调用send之前消息队列服务器上应该已经存在对应的exchange和queue了；
            // 如果没有，除了通过rabbitmq的web管理界面手动创建之外也可以调用
            // RabbitMqHub.Advanced相关接口通过代码来创建

            // 注册感兴趣的事件，这里用示例类型MessageTracker来演示一种可能的使用场景
            var connectionStr = "server=localhost;user=root;password=123456;database=test_mq";
            var tracker = new DbMessageTracker(connectionStr);
            _mqHub.MessageSending += (o, e) => tracker.OnAddMessageState(e);
            _mqHub.MessageSendOK += (o, e) => tracker.OnSetMessageState(e);
            _mqHub.MessageSendFailed += (o, e) => tracker.OnSetMessageState(e);

            // 注意，因为这里要演示消息发送过程中失败的各种情况，所以这里采用线程休眠
            // 来让发送过程尽量慢点，以便服务端可以做断开连接等异常操作；
            // 
            // 获取一个channel（不建议在循环中直接使用_mqHub.Send）以便能在循环中直接发送，
            // 因此我们还需要提前准备好exchange，routekey等东西（这里我们图方便采用默认exchange）
            var channel = _mqHub.Advance.GetChannelWrapper(publishConfirm: true);
            var list = new List<Order>();
            var random = new Random();
            for (var i = 0; i < 30; i++)
            {
                var order = new Order
                {
                    OrderId = $"电脑订单{i}",
                    Price = 100M,
                    ProductCode = "Computer",
                    Quantity = 10,
                    CreatedTime = DateTime.Now
                };

                Thread.Sleep(random.Next(500, 2500));

                SendOrders(ref channel, order, tracker);
            }
            channel.WaitForConfirms();

            // 发送完毕之后可以检查tracker中没有发送成功的消息，可以自行尝试再次发送
            var retryList = tracker.GetRetryItems();
            _logger.LogWarning("总共有" + retryList.Count + "条消息需要重发");

            // 在这个点上作为程序员的你是很清楚这些消息是重发的
            // 如果此时添加一个redelivered的头属性（只能是这个名字），那么库会在consume时使用到它
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