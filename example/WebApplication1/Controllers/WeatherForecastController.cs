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
            // RabbitMqHubʹ��֮ǰ��������ã��������ַ�ʽ��
            // 1. ����Ŀ��appsettings.json�ļ�������
            // 2. �ֶ�����һ��IConfigurationRoot����
            // ���������ļ��ĸ�ʽ���ڸ�demo�����appsetting.json����ʾ���������вο�

            // ʹ��RabbitMqHub.Advanced�ӿ�ֱ�ӷ�����Ϣ
            // ͨ����˵����send֮ǰ��Ϣ���з�������Ӧ���Ѿ����ڶ�Ӧ��exchange��queue�ˣ�
            // ���û�У�����ͨ��rabbitmq��web��������ֶ�����֮��Ҳ���Ե���
            // RabbitMqHub.Advanced��ؽӿ�ͨ������������
            var exchange = "zkb_zkb";
            var routeKey = "dev:zkb:sync_order";

            // ע�����Ȥ���¼���������ʾ������MessageTracker����ʾһ�ֿ��ܵ�ʹ�ó���
            var tracker = new MessageTracker();
            _rabbitMqHub.MessageSending += tracker.OnMessageSending;
            _rabbitMqHub.MessageSendOK += tracker.OnMessageSendOK;
            _rabbitMqHub.MessageSendFailed += tracker.OnMessageSendFailed;

            // ����һ����Ϣ
            var msg = new { Text = "hello world" };
            _rabbitMqHub.Advanced.Send(msg, exchange, routeKey, mandatory: true);

            // ���Ͷ�����Ϣ
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