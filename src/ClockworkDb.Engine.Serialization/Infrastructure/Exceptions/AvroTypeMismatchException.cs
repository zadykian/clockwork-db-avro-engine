

namespace ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

internal class AvroTypeMismatchException : Exception
{
    internal AvroTypeMismatchException(string s)
        : base(s)
    {

    }
    internal AvroTypeMismatchException(string s, Exception inner)
        : base(s, inner)
    {

    }
}