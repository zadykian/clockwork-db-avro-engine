namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
///     Class containing standard properties for different avro types.
/// </summary>
internal static class StandardProperties
{
    internal static readonly HashSet<string> Primitive = new() { Token.Type };

    internal static readonly HashSet<string> Record = new()
    {
        Token.Type,
        Token.Name,
        Token.Namespace,
        Token.Doc,
        Token.Aliases,
        Token.Fields
    };

    internal static readonly HashSet<string> Enumeration = new()
    {
        Token.Type,
        Token.Name,
        Token.Namespace,
        Token.Doc,
        Token.Aliases,
        Token.Symbols
    };
}