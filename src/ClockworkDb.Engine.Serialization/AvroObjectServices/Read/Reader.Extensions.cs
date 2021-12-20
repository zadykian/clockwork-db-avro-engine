using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader.Codec;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read;

internal partial class Reader : IReader
{
    internal Header ReadHeader()
    {
        var header = new Header();

        long len = ReadMapStart();
        if (len > 0)
        {
            do
            {
                for (long i = 0; i < len; i++)
                {
                    string key = ReadString();
                    byte[] val = ReadBytes();
                    header.AddMetadata(key, val);
                }
            } while ((len = ReadMapNext()) != 0);
        }

        return header;
    }

    internal byte[] ReadDataBlock(byte[] syncData, AbstractCodec codec)
    {
        var dataBlock = ReadRawBlock();
        ReadAndValidateSync(syncData);
        dataBlock = codec.Decompress(dataBlock);

        return dataBlock;
    }

    private byte[] ReadRawBlock()
    {
        var blockSize = ReadLong();

        var dataBlock = new byte[blockSize];
        ReadFixed(dataBlock, 0, (int)blockSize);

        return dataBlock;
    }

    internal void ReadAndValidateSync(byte[] expectedSync)
    {
        var syncBuffer = new byte[DataFileConstants.SyncSize];
        ReadFixed(syncBuffer);

        if (!syncBuffer.SequenceEqual(expectedSync))
            throw new AvroRuntimeException("Invalid sync!");
    }
}