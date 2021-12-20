using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
///     Class represents a boolean schema.
///     For more details please see <a href="http://avro.apache.org/docs/current/spec.html#schema_primitive">the specification</a>.
/// </summary>
internal sealed class BooleanSchema : PrimitiveTypeSchema
{
    internal BooleanSchema()
        : this(new Dictionary<string, string>())
    {
    }

    internal BooleanSchema(Dictionary<string, string> attributes)
        : base(typeof(bool), attributes)
    {
    }

    internal override AvroType Type => AvroType.Boolean;
}