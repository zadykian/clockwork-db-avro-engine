using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.Features.Deserialize;

namespace ClockworkDb.Engine.Serialization;

public static partial class AvroConvert
{
    /// <summary>
    /// Deserializes AVRO object to .NET type.
    /// </summary>
    public static T Deserialize<T>(byte[] avroBytes)
    {
        using var stream = new MemoryStream(avroBytes);
        var decoder = new Decoder();
        var deserialized = decoder.Decode<T>(
            stream,
            Schema.Create(typeof(T))
        );
        return deserialized;
    }

    /// <summary>
    /// Deserializes AVRO object to .NET type
    /// </summary>
    public static dynamic Deserialize(byte[] avroBytes, Type targetType)
    {
        object result = typeof(AvroConvert)
            .GetMethod("Deserialize", new[] { typeof(byte[]) })
            ?.MakeGenericMethod(targetType)
            .Invoke(null, new object[] { avroBytes });

        return result;
    }
}