using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    private readonly TypeSchema readerSchema;
    private readonly TypeSchema writerSchema;

    internal Resolver(TypeSchema writerSchema, TypeSchema readerSchema)
    {
        this.readerSchema = readerSchema;
        this.writerSchema = writerSchema;
    }

    internal T Resolve<T>(IReader reader) => (T) Resolve(writerSchema, readerSchema, reader, typeof(T));

    private object Resolve(
        TypeSchema currentWriterSchema,
        TypeSchema currentReaderSchema,
        IReader d,
        Type type)
    {
        try
        {
            if (currentReaderSchema.Type == AvroType.Union && currentWriterSchema.Type != AvroType.Union)
            {
                currentReaderSchema = FindBranch(currentReaderSchema as UnionSchema, currentWriterSchema);
            }

            switch (currentWriterSchema.Type)
            {
                case AvroType.Null:
                    return null;
                case AvroType.Boolean:
                    return d.ReadBoolean();
                case AvroType.Int:
                    return d.ReadInt();
                case AvroType.Long:
                    return ResolveLong(type, d);
                case AvroType.Float:
                    return d.ReadFloat();
                case AvroType.Double:
                    return d.ReadDouble();
                case AvroType.String:
                    return ResolveString(type, d);
                case AvroType.Bytes:
                    return d.ReadBytes();
                case AvroType.Logical:
                    return ResolveLogical((LogicalTypeSchema)currentWriterSchema, (LogicalTypeSchema)currentReaderSchema, d, type);
                case AvroType.Error:
                case AvroType.Record:
                    return ResolveRecord((RecordSchema)currentWriterSchema, (RecordSchema)currentReaderSchema, d, type);
                case AvroType.Enum:
                    return ResolveEnum((EnumSchema)currentWriterSchema, currentReaderSchema, d, type);
                case AvroType.Fixed:
                    return ResolveFixed((FixedSchema)currentWriterSchema, currentReaderSchema, d, type);
                case AvroType.Map:
                    return ResolveMap((MapSchema)currentWriterSchema, currentReaderSchema, d, type);
                case AvroType.Union:
                    return ResolveUnion((UnionSchema)currentWriterSchema, currentReaderSchema, d, type);
                default:
                    throw new AvroException("Unknown schema type: " + currentWriterSchema);
            }
        }
        catch (Exception e)
        {
            throw new AvroTypeMismatchException($"Unable to deserialize [{currentWriterSchema.Name}] of schema [{currentWriterSchema.Type}] to the target type [{type}]", e);
        }
    }
}