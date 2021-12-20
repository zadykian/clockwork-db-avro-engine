using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    private object ResolveLogical(LogicalTypeSchema writerSchema, LogicalTypeSchema readerSchema, IReader reader, Type type)
    {
        var value = Resolve(writerSchema.BaseTypeSchema, readerSchema.BaseTypeSchema, reader, type);

        var result = writerSchema.ConvertToLogicalValue(value, writerSchema, type);
        return result;
    }
}