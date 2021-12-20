using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.Features;
using ClockworkDb.Engine.Serialization.Features.Deserialize;

namespace ClockworkDb.Engine.Serialization;

public static class ApacheAvroApi
{
	/// <summary>
	/// Serializes given object to AVRO format (including header with metadata)
	/// </summary>
	public static byte[] Serialize(object obj)
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
	public static async ValueTask<T> DeserializeAsync<T>(byte[] avroBytes)
	{
		await using var stream = new MemoryStream(avroBytes);
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

	/// <summary>
	/// Extracts data schema from given AVRO object
	/// </summary>
	public static string GetSchema(byte[] avroBytes)
	{
		using var stream = new MemoryStream(avroBytes);
		var headerDecoder = new HeaderDecoder();
		var schema = headerDecoder.GetSchema(stream);
		return schema;
	}

	/// <summary>
	/// Extracts data schema from AVRO file under given path
	/// </summary>
	public static string GetSchema(string filePath)
	{
		using var stream = new FileStream(filePath, FileMode.Open);
		var headerDecoder = new HeaderDecoder();
		var schema = headerDecoder.GetSchema(stream);
		return schema;
	}
}