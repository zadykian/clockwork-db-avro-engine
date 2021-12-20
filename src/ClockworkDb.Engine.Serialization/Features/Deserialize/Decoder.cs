using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.Features.Deserialize;

internal static class Decoder
{
	internal static T Decode<T>(Stream stream, TypeSchema readSchema)
	{
		var reader = new Reader(stream);

		// validate header 
		var firstBytes = new byte[DataFileConstants.AvroHeader.Length];

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

		var header = reader.ReadHeader();

		var writeSchema = Schema.Create(header.GetMetadata(DataFileConstants.SchemaMetadataKey));
		readSchema ??= writeSchema;
		var resolver = new Resolver(writeSchema, readSchema);

		// read in sync data 
		reader.ReadFixed(header.SyncData);
		return Read<T>(reader, header, resolver);
	}

	private static T Read<T>(Reader reader, Header header, Resolver resolver)
	{
		long itemsCount = 0;
		var dataBlock = Array.Empty<byte>();

		do
		{
			itemsCount += reader.ReadLong();
			var data = reader.ReadDataBlock(header.SyncData);

			var array1OriginalLength = dataBlock.Length;
			Array.Resize(ref dataBlock, array1OriginalLength + data.Length);
			Array.Copy(data, 0, dataBlock, array1OriginalLength, data.Length);
		} while (!reader.IsReadToEnd());

		reader = new Reader(new MemoryStream(dataBlock));
		return resolver.Resolve<T>(reader, itemsCount);
	}
}