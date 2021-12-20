using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Skip;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read;

internal partial class Resolver
{
    private readonly Skipper _skipper;
    private readonly TypeSchema _readerSchema;
    private readonly TypeSchema _writerSchema;

    internal Resolver(TypeSchema writerSchema, TypeSchema readerSchema)
    {
        _readerSchema = readerSchema;
        _writerSchema = writerSchema;

        _skipper = new Skipper();
    }

    internal T Resolve<T>(IReader reader, long itemsCount = 0)
    {
        if (itemsCount > 1)
        {
            return (T)ResolveArray(
                _writerSchema,
                _readerSchema,
                reader, typeof(T), itemsCount);
        }

        var result = Resolve(_writerSchema, _readerSchema, reader, typeof(T));
        return (T)result;
    }

    internal object Resolve(
        TypeSchema writerSchema,
        TypeSchema readerSchema,
        IReader d,
        Type type)
    {
        try
        {
            if (readerSchema.Type == AvroType.Union && writerSchema.Type != AvroType.Union)
            {
                readerSchema = Resolvers.Resolver.FindBranch(readerSchema as UnionSchema, writerSchema);
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
                case AvroType.Array:
                    return ResolveArray(writerSchema, readerSchema, d, type);
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