

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.Features.Serialize;

namespace ClockworkDb.Engine.Serialization;

public static partial class AvroConvert
{
    /// <summary>
    /// Serializes given object to AVRO format (including header with metadata)
    /// </summary>
    public static byte[] Serialize(object obj)
    {
        return Serialize(obj, CodecType.Null);
    }

    /// <summary>
    /// Serializes given object to AVRO format (including header with metadata)
    /// Choosing <paramref name="codecType"/> reduces output object size
    /// </summary>
    public static byte[] Serialize(object obj, CodecType codecType)
    {
        using MemoryStream resultStream = new MemoryStream();
        var schema = Schema.Create(obj);
        using (var writer = new Encoder(schema, resultStream, codecType))
        {
            writer.Append(obj);
        }
        var result = resultStream.ToArray();
        return result;
    }
}