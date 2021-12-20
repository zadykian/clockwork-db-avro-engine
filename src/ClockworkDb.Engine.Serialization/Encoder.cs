using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Write;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization;

internal class Encoder : IDisposable
{
    internal delegate void WriteItem(object value, IWriter encoder);

    // ReSharper disable once PrivateFieldCanBeConvertedToLocalVariable
    private readonly TypeSchema schema;
    private readonly Stream stream;
    private MemoryStream tempBuffer;
    private readonly Writer writer;
    private IWriter tempWriter;
    private readonly WriteItem writeItem;
    private bool isOpen;
    private bool headerWritten;
    private int blockCount;
    private readonly int syncInterval;
    private readonly Header header;


    internal Encoder(TypeSchema schema, Stream outStream)
    {
        stream = outStream;
        header = new Header();
        this.schema = schema;
        syncInterval = DataFileConstants.DefaultSyncInterval;

        blockCount = 0;
        writer = new Writer(stream);
        tempBuffer = new MemoryStream();
        tempWriter = new Writer(tempBuffer);

        GenerateSyncData();
        header.AddMetadata(DataFileConstants.CodecMetadataKey, "gzip");
        header.AddMetadata(DataFileConstants.SchemaMetadataKey, this.schema.ToString());

        writeItem = Resolver.ResolveWriter(schema);

        isOpen = true;
    }

    internal void Append(object datum)
    {
        AssertOpen();
        EnsureHeader();

        writeItem(datum, tempWriter);

        blockCount++;
        WriteIfBlockFull();
    }

    private void EnsureHeader()
    {
        if (!headerWritten)
        {
            WriteHeader();
            headerWritten = true;
        }
    }

    private void Sync()
    {
        AssertOpen();
        WriteBlock();
    }

    private void WriteHeader()
    {
        writer.WriteHeader(header);
    }

    private void AssertOpen()
    {
        if (!isOpen) throw new AvroException("Cannot complete operation: avro file/stream not open");
    }

    private void WriteIfBlockFull()
    {
        if (tempBuffer.Position >= syncInterval)
            WriteBlock();
    }

    private void WriteBlock()
    {
        if (blockCount > 0)
        {
            byte[] dataToWrite = tempBuffer.ToArray();

            writer.WriteDataBlock(GZipCompressor.Compress(dataToWrite), header.SyncData, blockCount);

            // reset block buffer
            blockCount = 0;
            tempBuffer = new MemoryStream();
            tempWriter = new Writer(tempBuffer);
        }
    }

    private void GenerateSyncData()
    {
        header.SyncData = new byte[16];

        Random random = new Random();
        random.NextBytes(header.SyncData);
    }

    public void Dispose()
    {
        EnsureHeader();
        Sync();
        stream.Flush();
        stream.Dispose();
        isOpen = false;
    }
}