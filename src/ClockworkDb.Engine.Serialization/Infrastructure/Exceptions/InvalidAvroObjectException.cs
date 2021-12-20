

namespace ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

public class InvalidAvroObjectException : Exception
{
    internal InvalidAvroObjectException(string s)
        : base(s)
    {
    }

    internal InvalidAvroObjectException(string s, Exception inner)
        : base(s, inner)
    {
    }
}