using System;

namespace LightMessager.Model
{
    // 说明：讲道理并不需要对这些枚举值做位运算，只是修改消息状态时需要一次性表达多种状态值所以用了Flags
    [Flags]
    public enum MessageState
    {
        /// <summary>
        /// Created: 0
        /// </summary>
        Created = 0,
        /// <summary>
        /// Error: 1
        /// </summary>
        Error = 1 << 0,
        /// <summary>
        /// Error_Unroutable: 2
        /// </summary>
        Error_Unroutable = 1 << 1,
        /// <summary>
        /// Error_NoExchangeFound: 4
        /// </summary>
        Error_NoExchangeFound = 1 << 2,
        /// <summary>
        /// Confirmed: 8
        /// </summary>
        Confirmed = 1 << 3,
        /// <summary>
        /// Consumed: 16
        /// </summary>
        Consumed = 1 << 4
    }
}
