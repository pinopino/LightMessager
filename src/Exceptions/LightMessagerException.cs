using System;

namespace LightMessager.Exceptions
{
    public sealed class LightMessagerException : Exception
    {
        public LightMessagerException(string message = "", Exception InnerException = null)
            : base(message, InnerException)
        { }
    }
}
