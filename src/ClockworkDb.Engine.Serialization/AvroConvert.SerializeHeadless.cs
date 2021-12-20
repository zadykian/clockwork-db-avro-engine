

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Write;

namespace ClockworkDb.Engine.Serialization;

public static partial class AvroConvert
{
    /// <summary>
    /// Serializes given object to AVRO format - <c>excluding</c> header
    /// </summary>
    public static byte[] SerializeHeadless(object obj, string schema)
    {
        MemoryStream resultStream = new MemoryStream();
        var encoder = new Writer(resultStream);
        var writer = Resolver.ResolveWriter(Schema.Create(schema));
        writer(obj, encoder);

        var result = resultStream.ToArray();
        return result;
    }
}