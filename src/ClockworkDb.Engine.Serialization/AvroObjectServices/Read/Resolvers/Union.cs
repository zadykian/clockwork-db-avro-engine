using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    protected virtual object ResolveUnion(UnionSchema writerSchema, TypeSchema readerSchema, IReader d, Type type)
    {
        int index = d.ReadUnionIndex();
        TypeSchema ws = writerSchema.Schemas[index];

        if (readerSchema is UnionSchema unionSchema)
            readerSchema = FindBranch(unionSchema, ws);
        else
        if (!readerSchema.CanRead(ws))
            throw new AvroException("Schema mismatch. Reader: " + _readerSchema + ", writer: " + _writerSchema);

        return Resolve(ws, readerSchema, d, type);
    }

    protected static TypeSchema FindBranch(UnionSchema us, TypeSchema schema)
    {
        var resultSchema = us.Schemas.FirstOrDefault(s => s.Type == schema.Type);

        if (resultSchema == null)
        {
            throw new AvroException("No matching schema for " + schema + " in " + us);
        }

        return resultSchema;
    }
}