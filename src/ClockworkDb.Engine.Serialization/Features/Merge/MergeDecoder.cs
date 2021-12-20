

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader.Codec;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.Features.Merge;

internal class MergeDecoder
{
    internal AvroObjectContent ExtractAvroObjectContent(byte[] avroObject)
    {
        using var stream = new MemoryStream(avroObject);
        var reader = new Reader(stream);

        // validate header 
        byte[] firstBytes = new byte[DataFileConstants.AvroHeader.Length];

        try
        {
            reader.ReadFixed(firstBytes);
        }
        catch (EndOfStreamException)
        {
            //stream shorter than AvroHeader
        }

        //does not contain header
        if (!firstBytes.SequenceEqual(DataFileConstants.AvroHeader))
        {
            throw new InvalidAvroObjectException("Object does not contain Avro Header");
        }
        else
        {
            AvroObjectContent result = new AvroObjectContent();
            var header = reader.ReadHeader();
            result.Codec = AbstractCodec.CreateCodecFromString(header.GetMetadata(DataFileConstants.CodecMetadataKey));

            reader.ReadFixed(header.SyncData);

            result.Header = header;
            result.Header.Schema = Schema.Create(result.Header.GetMetadata(DataFileConstants.SchemaMetadataKey));

            do
            {
                var blockContent = new DataBlock
                {
                    ItemsCount = reader.ReadLong(),
                    Data = reader.ReadDataBlock(header.SyncData, result.Codec)
                };

                result.DataBlocks.Add(blockContent);

            } while (!reader.IsReadToEnd());

            return result;
        }
    }
}