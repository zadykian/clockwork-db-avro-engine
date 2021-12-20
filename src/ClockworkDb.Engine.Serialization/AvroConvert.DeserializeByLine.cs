

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.Features.DeserializeByLine;

namespace ClockworkDb.Engine.Serialization;

public static partial class AvroConvert
{
    /// <summary>
    /// Opens AVRO object deserializer which allows to read large collection of AVRO objects one by one
    /// </summary>
    public static ILineReader<T> OpenDeserializer<T>(Stream stream)
    {
        var reader = Decoder.OpenReader<T>(stream, Schema.Create(typeof(T)));

        return reader;
    }
}