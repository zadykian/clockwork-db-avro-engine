namespace ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

internal class AvroRuntimeException : Exception
{
    internal AvroRuntimeException(string s)
        : base(s)
    {

    }
    internal AvroRuntimeException(string s, Exception inner)
        : base(s, inner)
    {

    }
}