

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.Features.Merge;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization;

public static partial class AvroConvert
{
    /// <summary>
    /// Merge multiple Avro objects of type T into one Avro object of type IEnumerable T
    /// </summary>
    public static byte[] Merge<T>(IEnumerable<byte[]> avroObjects)
    {
        var itemSchema = Schema.Create(typeof(T));
        var targetSchema = Schema.Create(typeof(List<T>));
        var mergeDecoder = new MergeDecoder();

        List<DataBlock> avroDataBlocks = new List<DataBlock>();

        avroObjects = avroObjects.ToList();
        for (int i = 0; i < avroObjects.Count(); i++)
        {
            var avroFileContent = mergeDecoder.ExtractAvroObjectContent(avroObjects.ElementAt(i));
            if (!itemSchema.CanRead(avroFileContent.Header.Schema))
            {
                throw new InvalidAvroObjectException($"Schema from object of index [{i}] is not compatible with schema of type [{typeof(T)}]");
            }

            avroDataBlocks.AddRange(avroFileContent.DataBlocks);
        }

        using (MemoryStream resultStream = new MemoryStream())
        {
            using (var encoder = new MergeEncoder(resultStream))
            {
                encoder.WriteHeader(targetSchema.ToString(), CodecType.Null);

                encoder.WriteData(avroDataBlocks);
            }

            var result = resultStream.ToArray();
            return result;

        }
    }
}