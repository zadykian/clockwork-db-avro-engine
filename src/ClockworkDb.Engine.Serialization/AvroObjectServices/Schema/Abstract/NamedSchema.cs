using System.Collections.ObjectModel;
using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

/// <summary>
///     Class representing an named schema: record, enumeration or fixed.
///     For more details please see <a href="http://avro.apache.org/docs/current/spec.html#Names">the specification</a>.
/// </summary>
internal abstract class NamedSchema : TypeSchema
{
    private readonly NamedEntityAttributes attributes;

    internal NamedSchema(
        NamedEntityAttributes nameAttributes,
        Type runtimeType,
        Dictionary<string, string> attributes)
        : base(runtimeType, attributes)
    {
        if (nameAttributes == null)
        {
            throw new ArgumentNullException(nameof(nameAttributes));
        }

        this.attributes = nameAttributes;
    }

    internal string FullName => attributes.Name.FullName;

    internal override string Name => attributes.Name.Name;

    internal string Namespace => attributes.Name.Namespace;

    internal ReadOnlyCollection<string> Aliases => attributes.Aliases.AsReadOnly();

    internal string Doc => attributes.Doc;
}