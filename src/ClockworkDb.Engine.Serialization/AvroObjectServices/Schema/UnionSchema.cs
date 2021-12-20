using System.Collections.ObjectModel;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
/// Class representing a union schema.
/// </summary>
internal sealed class UnionSchema : TypeSchema
{
	private readonly List<TypeSchema> schemas;

	internal UnionSchema(
		List<TypeSchema> schemas,
		Type runtimeType,
		Dictionary<string, string> attributes)
		: base(runtimeType, attributes)
	{
		this.schemas = schemas ?? throw new ArgumentNullException(nameof(schemas));
	}

	internal UnionSchema(
		List<TypeSchema> schemas,
		Type runtimeType)
		: this(schemas, runtimeType, new Dictionary<string, string>())
	{
	}

	internal ReadOnlyCollection<TypeSchema> Schemas => schemas.AsReadOnly();

	internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
	{
		writer.WriteStartArray();
		schemas.ForEach(_ => _.ToJson(writer, seenSchemas));
		writer.WriteEndArray();
	}

	internal override AvroType Type => AvroType.Union;
}