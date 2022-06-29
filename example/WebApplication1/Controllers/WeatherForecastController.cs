using LightMessager;
using Microsoft.AspNetCore.Mvc;

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

        private readonly ILogger<WeatherForecastController> _logger;
        private readonly RabbitMqHub _rabbitMqHub;

        public WeatherForecastController(ILogger<WeatherForecastController> logger, RabbitMqHub rabbitMqHub)
        {
            _logger = logger;
            _rabbitMqHub = rabbitMqHub;
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
            var exchange = "zkb_zkb";
            var routeKey = "dev:zkb:sync_order";

            // 注册感兴趣的事件，这里用示例类型MessageTracker来演示一种可能的使用场景
            var tracker = new MessageTracker();
            _rabbitMqHub.MessageSending += tracker.OnMessageSending;
            _rabbitMqHub.MessageSendOK += tracker.OnMessageSendOK;
            _rabbitMqHub.MessageSendFailed += tracker.OnMessageSendFailed;

            // 发送一条消息
            var msg = new { Text = "hello world" };
            _rabbitMqHub.Advanced.Send(msg, exchange, routeKey, mandatory: true);

            // 发送多条消息
            var msgs = new List<dynamic>
            {
                new { Text = "hello world1" },
                new { Text = "hello world2" },
                new { Text = "hello world3" }
            };
            _rabbitMqHub.Advanced.Send(msgs, exchange, routeKey, mandatory: true, publishConfirm: true);

            return Ok("ok");
        }
    }
}