namespace ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

internal class SchemaParseException : AvroException
{
    internal SchemaParseException(string s)
        : base(s)
    {
    }
}