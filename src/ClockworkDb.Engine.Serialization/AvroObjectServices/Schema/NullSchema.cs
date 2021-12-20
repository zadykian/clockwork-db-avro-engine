using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
///     Class represents a null schema.
///     For more details please see <a href="http://avro.apache.org/docs/current/spec.html#schema_primitive">the specification</a>.
/// </summary>
internal class NullSchema : PrimitiveTypeSchema
{
    internal NullSchema(Type runtimeType)
        : this(runtimeType, new Dictionary<string, string>())
    {
    }

    internal NullSchema()
        : this(typeof(object))
    {
    }

    internal NullSchema(Type runtimeType, Dictionary<string, string> attributes)
        : base(runtimeType, attributes)
    {
    }

    internal override AvroType Type => AvroType.Null;
}