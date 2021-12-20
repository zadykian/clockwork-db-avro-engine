// using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
// using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
// using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
// using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
// using ClockworkDb.Engine.Serialization.Features.DeserializeByLine.LineReaders;
// using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;
//
// namespace ClockworkDb.Engine.Serialization.Features.DeserializeByLine;
//
// internal class Decoder
// {
//     internal static ILineReader<T> OpenReader<T>(Stream stream, TypeSchema readSchema)
//     {
//         var reader = new Reader(stream);
//
//         // validate header 
//         byte[] firstBytes = new byte[DataFileConstants.AvroHeader.Length];
//
//         try
//         {
//             reader.ReadFixed(firstBytes);
//         }
//         catch (EndOfStreamException)
//         {
//             //stream shorter than AvroHeader
//         }
//
//         //headless
//         if (!firstBytes.SequenceEqual(DataFileConstants.AvroHeader))
//         {
//             if (readSchema == null)
//             {
//                 throw new MissingSchemaException("Provide valid schema for the Avro data");
//             }
//             var resolver = new Resolver(readSchema, readSchema);
//             stream.Seek(0, SeekOrigin.Begin);
//             return new ListLineReader<T>(reader, resolver);
//         }
//
//         var header = reader.ReadHeader();
//
//         readSchema ??= Schema.Create(header.GetMetadata(DataFileConstants.SchemaMetadataKey));
//         var writeSchema = Schema.Create(header.GetMetadata(DataFileConstants.SchemaMetadataKey));
//
//         // read in sync data 
//         reader.ReadFixed(header.SyncData);
//         return new BaseLineReader<T>(reader, header.SyncData, writeSchema, readSchema);
//     }
// }

// todo: remove