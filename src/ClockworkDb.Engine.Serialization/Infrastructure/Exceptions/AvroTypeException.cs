namespace ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

internal class AvroTypeException : AvroException
{
    internal AvroTypeException(string s)
        : base(s)
    {
    }
}