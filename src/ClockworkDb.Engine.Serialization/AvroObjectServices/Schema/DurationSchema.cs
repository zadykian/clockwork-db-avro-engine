using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

internal sealed class DurationSchema : LogicalTypeSchema
{
    public DurationSchema() : this(typeof(TimeSpan))
    {
    }
    public DurationSchema(Type runtimeType) : base(runtimeType)
    {
        BaseTypeSchema = new FixedSchema(
            new NamedEntityAttributes(new SchemaName("duration"), new List<string>(), ""),
            12,
            typeof(TimeSpan));
    }

    internal override AvroType Type => AvroType.Logical;
    internal override TypeSchema BaseTypeSchema { get; set; }
    internal override string LogicalTypeName => "duration";

    internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
    {
        var baseSchema = (FixedSchema)BaseTypeSchema;
        writer.WriteStartObject();
        writer.WriteProperty("type", baseSchema.Type.ToString().ToLowerInvariant());
        writer.WriteProperty("size", baseSchema.Size);
        writer.WriteProperty("name", baseSchema.Name);
        writer.WriteProperty("logicalType", LogicalTypeName);
        writer.WriteEndObject();
    }

    internal object ConvertToBaseValue(object logicalValue, DurationSchema schema)
    {
        var duration = (TimeSpan)logicalValue;

        var baseSchema = (FixedSchema) schema.BaseTypeSchema;
        byte[] bytes = new byte[baseSchema.Size];
        var monthsBytes = BitConverter.GetBytes(0);
        var daysBytes = BitConverter.GetBytes(duration.Days);

        var milliseconds = ((duration.Hours * 60 + duration.Minutes) * 60 + duration.Seconds) * 1000 + duration.Milliseconds;
        var millisecondsBytes = BitConverter.GetBytes(milliseconds);


        Array.Copy(monthsBytes, 0, bytes, 0, 4);
        Array.Copy(daysBytes, 0, bytes, 4, 4);
        Array.Copy(millisecondsBytes, 0, bytes, 8, 4);


        if (!BitConverter.IsLittleEndian)
            Array.Reverse(bytes); //reverse it so we get little endian.

        return bytes;
    }

    internal override object ConvertToLogicalValue(object baseValue, LogicalTypeSchema schema, Type type)
    {
        byte[] baseBytes = (byte[])baseValue;
        if (!BitConverter.IsLittleEndian)
            Array.Reverse(baseBytes); //reverse it so we get big endian.

        int months = BitConverter.ToInt32(baseBytes.Skip(0).Take(4).ToArray(), 0);
        int days = BitConverter.ToInt32(baseBytes.Skip(4).Take(4).ToArray(), 0);
        int milliseconds = BitConverter.ToInt32(baseBytes.Skip(8).Take(4).ToArray(), 0);

        var result = new TimeSpan(months * 30 + days, 0, 0, 0, milliseconds);

        return result;
    }
}