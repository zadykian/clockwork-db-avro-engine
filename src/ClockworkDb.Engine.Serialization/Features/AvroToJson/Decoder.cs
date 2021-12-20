#region license
/**Copyright (c) 2021 Adrian Strugala
*
* Licensed under the CC BY-NC-SA 3.0 License(the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* https://creativecommons.org/licenses/by-nc-sa/3.0/
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*
* You are free to use or modify the code for personal usage.
* For commercial usage purchase the product at
*
* https://xabe.net/product/avroconvert/
*/
#endregion

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader.Codec;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.Features.AvroToJson;

internal class Decoder
{
    internal object Decode(Stream stream, TypeSchema schema)
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
            if (schema == null)
            {
                throw new MissingSchemaException("Provide valid schema for the Avro data");
            }
            var resolver = new Resolver(schema);
            stream.Seek(0, SeekOrigin.Begin);
            return resolver.Resolve(reader);
        }
        else
        {
            var header = reader.ReadHeader();

            schema = schema ?? Schema.Create(header.GetMetadata(DataFileConstants.SchemaMetadataKey));
            var resolver = new Resolver(schema);

            reader.ReadFixed(header.SyncData);
            var codec = AbstractCodec.CreateCodecFromString(header.GetMetadata(DataFileConstants.CodecMetadataKey));

            return Read(reader, header, codec, resolver);
        }
    }


    internal object Read(Reader reader, Header header, AbstractCodec codec, Resolver resolver)
    {
        var result = new List<object>();

        do
        {
            long itemsCount = reader.ReadLong();
            var data = reader.ReadDataBlock(header.SyncData, codec);

            reader = new Reader(new MemoryStream(data));

            if (itemsCount > 1)
            {
                for (int i = 0; i < itemsCount; i++)
                {
                    result.Add(resolver.Resolve(reader));
                }
            }
            else
            {
                return resolver.Resolve(reader);
            }

        } while (!reader.IsReadToEnd());


        return result;
    }
}