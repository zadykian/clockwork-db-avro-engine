using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    private object ResolveUnion(
        UnionSchema currentWriterSchema,
        TypeSchema currentReaderSchema,
        IReader d,
        Type type)
    {
        int index = d.ReadUnionIndex();
        TypeSchema ws = currentWriterSchema.Schemas[index];

        if (currentReaderSchema is UnionSchema unionSchema)
            currentReaderSchema = FindBranch(unionSchema, ws);
        else
        if (!currentReaderSchema.CanRead(ws))
            throw new AvroException("Schema mismatch. Reader: " + currentReaderSchema + ", writer: " + currentWriterSchema);

        return Resolve(ws, currentReaderSchema, d, type);
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