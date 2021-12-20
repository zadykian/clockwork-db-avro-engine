using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.Features;

internal class HeaderDecoder
{
    internal string GetSchema(Stream stream)
    {
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
            var header = reader.ReadHeader();

            return header.GetMetadata(DataFileConstants.SchemaMetadataKey);
        }
    }
}