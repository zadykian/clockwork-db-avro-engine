using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.Features
{
    internal class Decoder
    {
        internal T Decode<T>(Stream stream, TypeSchema readSchema)
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

                TypeSchema writeSchema = Schema.Create(header.GetMetadata(DataFileConstants.SchemaMetadataKey));
                readSchema = readSchema ?? writeSchema;
                var resolver = new Resolver(writeSchema, readSchema);

                // read in sync data 
                reader.ReadFixed(header.SyncData);

                return Read<T>(reader, header, resolver);
            }
        }


        internal T Read<T>(Reader reader, Header header, Resolver resolver)
        {
            long itemsCount = 0;
            byte[] dataBlock = new byte[0];

            do
            {
                itemsCount += reader.ReadLong();
                var data = reader.ReadDataBlock(header.SyncData);

                int array1OriginalLength = dataBlock.Length;
                Array.Resize(ref dataBlock, array1OriginalLength + data.Length);
                Array.Copy(data, 0, dataBlock, array1OriginalLength, data.Length);

            } while (!reader.IsReadToEnd());


            reader = new Reader(new MemoryStream(dataBlock));

            return resolver.Resolve<T>(reader);
        }
    }
}