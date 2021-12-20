namespace ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

internal class AvroException : Exception
{
    internal AvroException(string s)
        : base(s)
    {
    }

    internal AvroException(string s, Exception inner)
        : base(s, inner)
    {
    }
}