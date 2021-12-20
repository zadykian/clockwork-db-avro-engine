namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    internal object ResolveLong(Type type, IReader reader)
    {
        long value = reader.ReadLong();
        return value;
    }
}