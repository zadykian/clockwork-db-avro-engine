using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;
using ClockworkDb.Engine.Serialization.Features.Serialize;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write;

internal static class Resolver
{
    internal delegate void Writer<in T>(T t);

    private static readonly Array Array;
    private static readonly Map Map;
    private static readonly Null Null;
    private static readonly String String;
    private static readonly Record Record;
    private static readonly Enum Enum;
    private static readonly Fixed Fixed;
    private static readonly Union Union;
    private static readonly Long Long;
    private static readonly Uuid Uuid;
    private static readonly Decimal Decimal;
    private static readonly Duration Duration;
    private static readonly TimestampMilliseconds TimestampMilliseconds;

    static Resolver()
    {
        Array = new Array();
        Null = new Null();
        Map = new Map();
        String = new String();
        Record = new Record();
        Enum = new Enum();
        Fixed = new Fixed();
        Union = new Union();
        Long = new Long();
        Uuid = new Uuid();
        Decimal = new Decimal();
        Duration = new Duration();
        TimestampMilliseconds = new TimestampMilliseconds();
    }

    internal static Encoder.WriteItem ResolveWriter(TypeSchema schema)
    {
        switch (schema.Type)
        {
            case AvroType.Null:
                return Null.Resolve;
            case AvroType.Boolean:
                return (v, e) => Write<bool>(v, schema.Type, e.WriteBoolean);
            case AvroType.Int:
                return (v, e) => Write<int>(v, schema.Type, e.WriteInt);
            case AvroType.Long:
                return Long.Resolve;
            case AvroType.Float:
                return (v, e) => Write<float>(v, schema.Type, e.WriteFloat);
            case AvroType.Double:
                return (v, e) => Write<double>(v, schema.Type, e.WriteDouble);
            case AvroType.String:
                return String.Resolve;
            case AvroType.Bytes:
                return (v, e) => Write<byte[]>(v, schema.Type, e.WriteBytes);
            case AvroType.Error:
            case AvroType.Logical:
            {
                var logicalTypeSchema = (LogicalTypeSchema)schema;
                switch (logicalTypeSchema.LogicalTypeName)
                {
                    case LogicalTypeSchema.LogicalTypeEnum.Uuid:
                        return Uuid.Resolve((UuidSchema)logicalTypeSchema);
                    case LogicalTypeSchema.LogicalTypeEnum.Decimal:
                        return Decimal.Resolve((DecimalSchema)logicalTypeSchema);
                    case LogicalTypeSchema.LogicalTypeEnum.TimestampMilliseconds:
                        return TimestampMilliseconds.Resolve((TimestampMillisecondsSchema)logicalTypeSchema);
                    case LogicalTypeSchema.LogicalTypeEnum.Duration:
                        return Duration.Resolve((DurationSchema)logicalTypeSchema);
                }
            }
                return String.Resolve;
            case AvroType.Record:
                return Record.Resolve((RecordSchema)schema);
            case AvroType.Enum:
                return Enum.Resolve((EnumSchema)schema);
            case AvroType.Fixed:
                return Fixed.Resolve((FixedSchema)schema);
            case AvroType.Array:
                return Array.Resolve((ArraySchema)schema);
            case AvroType.Map:
                return Map.Resolve((MapSchema)schema);
            case AvroType.Union:
                return Union.Resolve((UnionSchema)schema);
            default:
                return (v, e) =>
                    throw new AvroTypeMismatchException(
                        $"Tried to write against [{schema}] schema, but found [{v.GetType()}] type");
        }
    }

    /// <summary>
    /// A generic method to serialize primitive Avro types.
    /// </summary>
    /// <typeparam name="S">Type of the C# type to be serialized</typeparam>
    /// <param name="value">The value to be serialized</param>
    /// <param name="tag">The schema type tag</param>
    /// <param name="writer">The writer which should be used to write the given type.</param>
    private static void Write<S>(object value, AvroType tag, Writer<S> writer)
    {
        if (value == null)
        {
            value = default(S);
        }

        if (!(value is S))
            throw new AvroTypeMismatchException(
                $"[{typeof(S)}] required to write against [{tag}] schema but found " + value?.GetType());

        writer((S)value);
    }
}