using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Skip;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    private readonly Skipper skipper;
    private readonly TypeSchema readerSchema;
    private readonly TypeSchema writerSchema;

    internal Resolver(TypeSchema writerSchema, TypeSchema readerSchema)
    {
        this.readerSchema = readerSchema;
        this.writerSchema = writerSchema;

        skipper = new Skipper();
    }

    internal T Resolve<T>(IReader reader) => (T) Resolve(writerSchema, readerSchema, reader, typeof(T));

    private object Resolve(
        TypeSchema writerSchema,
        TypeSchema readerSchema,
        IReader d,
        Type type)
    {
        try
        {
            if (readerSchema.Type == AvroType.Union && writerSchema.Type != AvroType.Union)
            {
                readerSchema = FindBranch(readerSchema as UnionSchema, writerSchema);
            }

            switch (writerSchema.Type)
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
                    return ResolveLogical((LogicalTypeSchema)writerSchema, (LogicalTypeSchema)readerSchema, d, type);
                case AvroType.Error:
                case AvroType.Record:
                    return ResolveRecord((RecordSchema)writerSchema, (RecordSchema)readerSchema, d, type);
                case AvroType.Enum:
                    return ResolveEnum((EnumSchema)writerSchema, readerSchema, d, type);
                case AvroType.Fixed:
                    return ResolveFixed((FixedSchema)writerSchema, readerSchema, d, type);
                case AvroType.Map:
                    return ResolveMap((MapSchema)writerSchema, readerSchema, d, type);
                case AvroType.Union:
                    return ResolveUnion((UnionSchema)writerSchema, readerSchema, d, type);
                default:
                    throw new AvroException("Unknown schema type: " + writerSchema);
            }
        }
        catch (Exception e)
        {
            throw new AvroTypeMismatchException($"Unable to deserialize [{writerSchema.Name}] of schema [{writerSchema.Type}] to the target type [{type}]", e);
        }
    }
}