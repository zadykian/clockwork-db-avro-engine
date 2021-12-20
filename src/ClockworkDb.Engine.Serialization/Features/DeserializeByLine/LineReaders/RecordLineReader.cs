using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;

namespace ClockworkDb.Engine.Serialization.Features.DeserializeByLine.LineReaders;

internal class RecordLineReader<T> : ILineReader<T>
{
    private readonly Reader dataReader;
    private readonly Resolver resolver;

    private bool hasNext;

    public RecordLineReader(Reader dataReader, Resolver resolver)
    {
        this.dataReader = dataReader;
        this.resolver = resolver;
        hasNext = true;
    }

    public bool HasNext()
    {
        if (hasNext)
        {
            hasNext = false;
            return true;
        }

        return hasNext;
    }

    public T ReadNext()
    {
        return resolver.Resolve<T>(dataReader);
    }

    public void Dispose()
    {

    }
}