using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
/// Class represents a double schema.
/// </summary>
internal sealed class DoubleSchema : PrimitiveTypeSchema
{
    /// <summary>
    /// Initializes a new instance of the <see cref="DoubleSchema"/> class.
    /// </summary>
    internal DoubleSchema() : this(new Dictionary<string, string>())
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="DoubleSchema"/> class.
    /// </summary>
    /// <param name="attributes">The attributes.</param>
    internal DoubleSchema(Dictionary<string, string> attributes) : base(typeof(double), attributes)
    {
    }

    internal override AvroType Type => AvroType.Double;
}