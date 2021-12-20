using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
/// Class represents an int schema.
/// </summary>
internal class IntSchema : PrimitiveTypeSchema
{
	internal IntSchema()
		: this(typeof(int))
	{
	}

	internal IntSchema(Type runtimeType)
		: this(runtimeType, new Dictionary<string, string>())
	{
	}

	internal IntSchema(Type runtimeType, Dictionary<string, string> attributes)
		: base(runtimeType, attributes)
	{
	}

	internal override AvroType Type => AvroType.Int;
}