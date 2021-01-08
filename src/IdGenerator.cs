using System;

namespace LightMessager
{
    public sealed class IdGenerator
    {
        public static string GenerateMessageId()
        {
            return Guid.NewGuid().ToString();
        }
    }
}
