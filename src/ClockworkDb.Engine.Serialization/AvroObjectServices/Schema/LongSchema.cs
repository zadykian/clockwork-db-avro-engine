using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
///     Class represents a long schema.
///     For more details please see <a href="http://avro.apache.org/docs/current/spec.html#schema_primitive">the specification</a>.
/// </summary>
internal sealed class LongSchema : PrimitiveTypeSchema
{
    internal LongSchema()
        : this(typeof(long))
    {
    }

    internal LongSchema(Type type)
        : this(type, new Dictionary<string, string>())
    {
    }

    internal LongSchema(Type type, Dictionary<string, string> attributes)
        : base(type, attributes)
    {
    }

    internal override AvroType Type => AvroType.Long;
}