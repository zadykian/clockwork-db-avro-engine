using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
///     Represents a fixed schema.
///     For more details please see <a href="http://avro.apache.org/docs/current/spec.html#Fixed">the specification</a>.
/// </summary>
internal sealed class FixedSchema : NamedSchema
{
	internal FixedSchema(NamedEntityAttributes namedEntityAttributes, int size, Type runtimeType)
		: this(namedEntityAttributes, size, runtimeType, new Dictionary<string, string>())
	{
	}

	internal FixedSchema(
		NamedEntityAttributes namedEntityAttributes,
		int size,
		Type runtimeType,
		Dictionary<string, string> attributes) : base(namedEntityAttributes, runtimeType, attributes)
	{
		if (size <= 0)
		{
			throw new ArgumentOutOfRangeException(nameof(size));
		}

		Size = size;
	}

	internal int Size { get; }

	internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
	{
		if (seenSchemas.Contains(this))
		{
			writer.WriteValue(FullName);
			return;
		}

		seenSchemas.Add(this);
		writer.WriteStartObject();
		writer.WriteProperty("type", "fixed");
		writer.WriteProperty("name", Name);
		writer.WriteOptionalProperty("namespace", Namespace);
		writer.WriteOptionalProperty("aliases", Aliases);
		writer.WriteProperty("size", Size);
		writer.WriteEndObject();
	}

	internal override AvroType Type => AvroType.Fixed;
}