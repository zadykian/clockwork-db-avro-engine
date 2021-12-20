using ClockworkDb.Engine.Serialization.Features.GetSchema;

namespace ClockworkDb.Engine.Serialization;

public static partial class AvroConvert
{
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

    /// <summary>
    /// Extracts data schema from given AVRO stream
    /// </summary>
    public static string GetSchema(Stream avroStream)
    {
        var headerDecoder = new HeaderDecoder();
        var schema = headerDecoder.GetSchema(avroStream);
        return schema;
    }
}