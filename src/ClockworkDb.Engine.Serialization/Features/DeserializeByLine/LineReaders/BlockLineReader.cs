using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;

namespace ClockworkDb.Engine.Serialization.Features.DeserializeByLine.LineReaders;

public class BlockLineReader<T> : ILineReader<T>
{
    private readonly Reader reader;
    private readonly Resolver resolver;
    private long blockCount;

    internal BlockLineReader(Reader reader, Resolver resolver, long blockCount)
    {
        this.reader = reader;
        this.resolver = resolver;
        this.blockCount = blockCount;
    }

    public bool HasNext()
    {
        return blockCount != 0;
    }

    public T ReadNext()
    {
        var result = resolver.Resolve<T>(reader);
        blockCount--;

        return result;
    }

    public void Dispose()
    {
    }
}