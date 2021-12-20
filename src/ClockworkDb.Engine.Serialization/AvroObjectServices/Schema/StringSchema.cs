using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
/// Class represents a string schema.
/// </summary>
internal class StringSchema : PrimitiveTypeSchema
{
	internal StringSchema()
		: this(typeof(string))
	{
	}

	internal StringSchema(Type type)
		: this(type, new Dictionary<string, string>())
	{
	}

	internal StringSchema(Type type, Dictionary<string, string> attributes)
		: base(type, attributes)
	{
	}

	internal override AvroType Type => AvroType.String;
}