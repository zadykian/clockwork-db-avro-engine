using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.Features;
using ClockworkDb.Engine.Serialization.Features.Deserialize;

namespace ClockworkDb.Engine.Serialization;

/// <summary>
/// API which provides functionality for:
/// <para> 1) Objects serialization/deserialization </para>
/// <para> AVRO schema generation </para>
/// </summary>
public static class ApacheAvroApi
{
	/// <summary>
	/// Serializes given object to AVRO format (including header with metadata)
	/// </summary>
	public static IEnumerable<byte> Serialize(object obj)
	{
		using var resultStream = new MemoryStream();
		var schema = Schema.Create(obj);
		using (var writer = new Encoder(schema, resultStream))
		{
			writer.Append(obj);
		}

		return resultStream.ToArray();
	}

	/// <summary>
	/// Deserializes AVRO object to .NET instance.
	/// </summary>
	public static async ValueTask<T> DeserializeAsync<T>(IEnumerable<byte> avroBytes)
	{
		await using var stream = new MemoryStream(avroBytes.ToArray());
		return Decoder.Decode<T>(stream, Schema.Create(typeof(T)));
	}

	/// <summary>
	/// Generates schema for given .NET Type
	/// </summary>
	public static string GenerateSchema(Type type)
	{
		var reader = new ReflectionSchemaBuilder(new AvroSerializerSettings()).BuildSchema(type);
		return reader.ToString();
	}
}