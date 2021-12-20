﻿// using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
// using ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;
//
// namespace ClockworkDb.Engine.Serialization.Features.DeserializeByLine.LineReaders;
//
// internal class ListLineReader<T> : ILineReader<T>
// {
//     private readonly IReader reader;
//     private readonly Resolver resolver;
//     private int itemsCount;
//
//     public ListLineReader(IReader reader, Resolver resolver)
//     {
//         this.reader = reader;
//         this.resolver = resolver;
//
//         itemsCount = (int)reader.ReadArrayStart();
//     }
//     public bool HasNext()
//     {
//         if (itemsCount == 0)
//         {
//             itemsCount = (int)reader.ReadArrayNext();
//             return itemsCount != 0;
//         }
//         else
//         {
//             return true;
//         }
//     }
//
//     public T ReadNext()
//     {
//         var result = resolver.Resolve<T>(reader);
//         itemsCount--;
//         return result;
//     }
//
//     public void Dispose()
//     {
//     }
// }

// todo: remove