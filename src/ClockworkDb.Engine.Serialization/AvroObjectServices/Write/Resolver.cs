using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;
using ClockworkDb.Engine.Serialization.Features;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write;

internal static class Resolver
{
	private delegate void Writer<in T>(T t);

	private static readonly AvroArray array;
	private static readonly Map map;
	private static readonly Null @null;
	private static readonly AvroString @string;
	private static readonly Record record;
	private static readonly AvroEnum @enum;
	private static readonly Fixed @fixed;
	private static readonly Union union;
	private static readonly Long @long;
	private static readonly Uuid uuid;
	private static readonly AvroDecimal @decimal;

	private static readonly Duration duration;
	// private static readonly TimestampMilliseconds timestampMilliseconds;

	static Resolver()
	{
		array = new AvroArray();
		@null = new Null();
		map = new Map();
		@string = new AvroString();
		record = new Record();
		@enum = new AvroEnum();
		@fixed = new Fixed();
		union = new Union();
		@long = new Long();
		uuid = new Uuid();
		@decimal = new AvroDecimal();
		duration = new Duration();
		// timestampMilliseconds = new TimestampMilliseconds();
	}

	internal static Encoder.WriteItem ResolveWriter(TypeSchema schema)
	{
		switch (schema.Type)
		{
			case AvroType.Null:
				return @null.Resolve;
			case AvroType.Boolean:
				return (v, e) => Write<bool>(v, schema.Type, e.WriteBoolean);
			case AvroType.Int:
				return (v, e) => Write<int>(v, schema.Type, e.WriteInt);
			case AvroType.Long:
				return @long.Resolve;
			case AvroType.Float:
				return (v, e) => Write<float>(v, schema.Type, e.WriteFloat);
			case AvroType.Double:
				return (v, e) => Write<double>(v, schema.Type, e.WriteDouble);
			case AvroType.String:
				return @string.Resolve;
			case AvroType.Bytes:
				return (v, e) => Write<byte[]>(v, schema.Type, e.WriteBytes);
			case AvroType.Error:
			case AvroType.Logical:
			{
				var logicalTypeSchema = (LogicalTypeSchema)schema;
				switch (logicalTypeSchema.LogicalTypeName)
				{
					case LogicalTypeSchema.LogicalTypeEnum.Uuid:
						return uuid.Resolve((UuidSchema)logicalTypeSchema);
					case LogicalTypeSchema.LogicalTypeEnum.Decimal:
						return @decimal.Resolve((DecimalSchema)logicalTypeSchema);
					// case LogicalTypeSchema.LogicalTypeEnum.TimestampMilliseconds:
					//     return timestampMilliseconds.Resolve((TimestampMillisecondsSchema)logicalTypeSchema);
					case LogicalTypeSchema.LogicalTypeEnum.Duration:
						return duration.Resolve((DurationSchema)logicalTypeSchema);
				}
			}
				return @string.Resolve;
			case AvroType.Record:
				return record.Resolve((RecordSchema)schema);
			case AvroType.Enum:
				return @enum.Resolve((EnumSchema)schema);
			case AvroType.Fixed:
				return @fixed.Resolve((FixedSchema)schema);
			case AvroType.Array:
				return array.Resolve((ArraySchema)schema);
			case AvroType.Map:
				return map.Resolve((MapSchema)schema);
			case AvroType.Union:
				return union.Resolve((UnionSchema)schema);
			default:
				return (v, e) =>
					throw new AvroTypeMismatchException(
						$"Tried to write against [{schema}] schema, but found [{v.GetType()}] type");
		}
	}

	/// <summary>
	/// A generic method to serialize primitive Avro types.
	/// </summary>
	/// <typeparam name="TS">Type of the C# type to be serialized</typeparam>
	/// <param name="value">The value to be serialized</param>
	/// <param name="tag">The schema type tag</param>
	/// <param name="writer">The writer which should be used to write the given type.</param>
	private static void Write<TS>(object value, AvroType tag, Writer<TS> writer)
	{
		value ??= default(TS);

		if (value is not TS typedValue)
			throw new AvroTypeMismatchException(
				$"[{typeof(TS)}] required to write against [{tag}] schema but found " + value?.GetType());

		writer(typedValue);
	}
}