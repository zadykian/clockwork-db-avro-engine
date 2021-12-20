namespace ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

internal class MissingSchemaException : Exception
{
    internal MissingSchemaException(string s)
        : base(s)
    {
    }

    internal MissingSchemaException(string s, Exception inner)
        : base(s, inner)
    {
    }
}