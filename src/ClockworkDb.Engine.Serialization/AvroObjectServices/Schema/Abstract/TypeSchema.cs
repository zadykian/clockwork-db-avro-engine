using System.Globalization;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

/// <summary>
///     Base class for all type schemas.
///     For more details please see <a href="http://avro.apache.org/docs/current/spec.html">the specification</a>.
/// </summary>
internal abstract class TypeSchema : BuildSchema.Schema
{
    protected TypeSchema(Type runtimeType, IDictionary<string, string> attributes) : base(attributes)
    {
        if (runtimeType == null)
        {
            throw new ArgumentNullException(nameof(runtimeType));
        }

        RuntimeType = runtimeType;
    }

    internal Type RuntimeType { get; }

    internal abstract AvroType Type { get; }

    internal virtual bool CanRead(TypeSchema writerSchema) { return Type == writerSchema.Type; }

    internal virtual string Name => Type.ToString().ToLower(CultureInfo.InvariantCulture);
}