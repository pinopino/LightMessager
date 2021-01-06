using System;

namespace LightMessager.Model
{
    // 说明：讲道理并不需要对这些枚举值做位运算，只是修改消息状态时需要一次性表达多种状态值所以用了Flags
    [Flags]
    public enum MessageState
    {
        None = 0,
        /// <summary>
        /// Created: 1
        /// </summary>
        Created = 1 << 0,
        /// <summary>
        /// Confirmed: 2
        /// </summary>
        Confirmed = 1 << 1,
        /// <summary>
        /// Consumed: 4
        /// </summary>
        Consumed = 1 << 2,
        /// <summary>
        /// Error: 8
        /// </summary>
        Error = 1 << 3,
        /// <summary>
        /// Error_Unroutable: 16
        /// </summary>
        Error_Unroutable = 1 << 4,
        /// <summary>
        /// Error_NoExchangeFound: 32
        /// </summary>
        Error_NoExchangeFound = 1 << 5
    }
}
