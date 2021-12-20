using System.Collections.ObjectModel;
using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
/// Class represents a record schema.
/// </summary>
internal sealed class RecordSchema : NamedSchema
{
	private readonly List<RecordField> fields;
	private readonly Dictionary<string, RecordField> fieldsByName;

	internal RecordSchema(
		NamedEntityAttributes namedAttributes,
		Type runtimeType,
		Dictionary<string, string> attributes)
		: base(namedAttributes, runtimeType, attributes)
	{
		fields = new List<RecordField>();
		fieldsByName = new Dictionary<string, RecordField>(StringComparer.InvariantCultureIgnoreCase);
	}

	internal RecordSchema(NamedEntityAttributes namedAttributes, Type runtimeType)
		: this(namedAttributes, runtimeType, new Dictionary<string, string>())
	{
	}

	internal void AddField(RecordField field)
	{
		if (field == null)
		{
			throw new ArgumentNullException(nameof(field));
		}

		fields.Add(field);
		fieldsByName.Add(field.Name, field);
	}

	internal bool TryGetField(string fieldName, out RecordField result)
	{
		return fieldsByName.TryGetValue(fieldName, out result);
	}

	internal RecordField GetField(string fieldName)
	{
		return fieldsByName[fieldName];
	}

	internal ReadOnlyCollection<RecordField> Fields => fields.AsReadOnly();

	internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
	{
		if (seenSchemas.Contains(this))
		{
			writer.WriteValue(FullName);
			return;
		}

		seenSchemas.Add(this);
		writer.WriteStartObject();
		writer.WriteProperty("type", "record");
		writer.WriteProperty("name", Name);
		writer.WriteOptionalProperty("namespace", Namespace);
		writer.WriteOptionalProperty("doc", Doc);
		writer.WriteOptionalProperty("aliases", Aliases);
		writer.WritePropertyName("fields");
		writer.WriteStartArray();
		fields.ForEach(_ => _.ToJson(writer, seenSchemas));
		writer.WriteEndArray();
		writer.WriteEndObject();
	}

	internal override AvroType Type => AvroType.Record;
}