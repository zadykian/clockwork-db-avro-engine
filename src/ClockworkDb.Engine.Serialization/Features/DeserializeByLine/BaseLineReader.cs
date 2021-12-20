using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Features.DeserializeByLine.LineReaders;

namespace ClockworkDb.Engine.Serialization.Features.DeserializeByLine;

internal class BaseLineReader<T> : ILineReader<T>
{
    private readonly Reader reader;
    private readonly byte[] syncDate;
    private readonly TypeSchema writeSchema;
    private readonly TypeSchema readSchema;
    private ILineReader<T> lineReaderInternal;

    internal BaseLineReader(Reader reader, byte[] syncDate, TypeSchema writeSchema, TypeSchema readSchema)
    {
        this.reader = reader;
        this.syncDate = syncDate;
        this.writeSchema = writeSchema;
        this.readSchema = readSchema;

        LoadNextDataBlock();
    }


    public bool HasNext()
    {
        var hasNext = lineReaderInternal.HasNext();

        if (!hasNext)
        {
            hasNext = !reader.IsReadToEnd();

            if (hasNext)
            {
                LoadNextDataBlock();
                return lineReaderInternal.HasNext();
            }
        }

        return hasNext;
    }

    private void LoadNextDataBlock()
    {
        var resolver = new Resolver(writeSchema, readSchema);

        var itemsCount = reader.ReadLong();

        var dataBlock = reader.ReadDataBlock(syncDate);
        var dataReader = new Reader(new MemoryStream(dataBlock));


        if (itemsCount > 1)
        {
            lineReaderInternal = new BlockLineReader<T>(dataReader, resolver, itemsCount);
            return;
        }

        if (writeSchema.Type == AvroType.Array)
        {
            lineReaderInternal = new ListLineReader<T>(dataReader, new Resolver(((ArraySchema)writeSchema).ItemSchema, readSchema));
            return;
        }

        lineReaderInternal = new RecordLineReader<T>(dataReader, resolver);
    }

    public T ReadNext()
    {
        return lineReaderInternal.ReadNext();
    }

    public void Dispose()
    {
        lineReaderInternal.Dispose();
    }
}