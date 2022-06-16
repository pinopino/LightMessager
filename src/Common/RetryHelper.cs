using LightMessager.Exceptions;
using NLog;
using System;
using System.Threading.Tasks;

namespace LightMessager.Common
{
    internal static class RetryHelper
    {
        private static Logger _logger = LogManager.GetLogger("RetryHelper");

        private const string ErrorMessage1 = "Maximum number of tries exceeded to perform the action.";
        private const string ErrorMessage2 = "Predicate `shouldRetry` return false when perform the action.";
        private const string WarningMessage1 = "Force retry based on result. Retrying... {0}/{1}";
        private const string WarningMessage2 = "Exception occurred performing an action. Retrying... {0}/{1}";

        public static void Retry(Action action, int maxRetry, int backoffMs = 100,
                Predicate<Exception> shouldRetry = null)
        {
            Retry(() =>
            {
                action();
                return true;
            },
            maxRetry, backoffMs, shouldRetry, null);
        }

        public static Task RetryAsync(Action action, int maxRetry, int backoffMs = 100,
                Predicate<Exception> shouldRetry = null)
        {
            return RetryAsync(() =>
            {
                Task.Run(action);
                return Task.FromResult(true);
            },
            maxRetry, backoffMs, shouldRetry, null);
        }

        public static T Retry<T>(Func<T> func, int maxRetry, int backoffMs = 100,
                Predicate<Exception> shouldRetry = null, Predicate<T> retryOnResult = null)
        {
            var retryCount = 0;
            while (true)
            {
                try
                {
                    var result = func();
                    if (retryCount != maxRetry && retryOnResult?.Invoke(result) == true)
                    {
                        var message = string.Format(WarningMessage1, retryCount, maxRetry);
                        _logger.Warn(message);
                        throw new Exception(message);
                    }

                    return result;
                }
                catch (Exception ex)
                {
                    if (retryCount >= maxRetry)
                        throw new LightMessagerException(ErrorMessage1, ex);

                    if (shouldRetry?.Invoke(ex) == true)
                        _logger.Warn(ex, string.Format(WarningMessage2, retryCount, maxRetry));
                    else
                        throw new LightMessagerException(ErrorMessage2, ex);

                    // 间隔一段时间+随机扰乱
                    var jitter = RandomUtil.Random.Next(0, 100);
                    var backoff = (int)Math.Pow(2, retryCount) * backoffMs;
                    Task.Delay(backoff + jitter).Wait();

                    retryCount++;
                    backoffMs *= 2;
                }
            }
        }

        public static async Task<T> RetryAsync<T>(Func<Task<T>> func, int maxRetry, int backoffMs = 100,
                Predicate<Exception> shouldRetry = null, Predicate<T> retryOnResult = null)
        {
            var retryCount = 0;
            while (true)
            {
                try
                {
                    var result = await func();
                    if (retryCount != maxRetry && retryOnResult?.Invoke(result) == true)
                    {
                        var message = string.Format(WarningMessage1, retryCount, maxRetry);
                        _logger.Warn(message);
                        throw new Exception(message);
                    }

                    return result;
                }
                catch (Exception ex)
                {
                    if (retryCount >= maxRetry)
                        throw new LightMessagerException(ErrorMessage1, ex);

                    if (shouldRetry?.Invoke(ex) == true)
                        _logger.Warn(ex, string.Format(WarningMessage2, retryCount, maxRetry));
                    else
                        throw new LightMessagerException(ErrorMessage2, ex);

                    // 间隔一段时间+随机扰乱
                    var jitter = RandomUtil.Random.Next(0, 100);
                    var backoff = (int)Math.Pow(2, retryCount) * backoffMs;
                    await Task.Delay(backoff + jitter);

                    retryCount++;
                    backoffMs *= 2;
                }
            }
        }
    }
}
