

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

internal abstract class LogicalTypeSchema : TypeSchema
{
    internal class LogicalTypeEnum
    {
        internal const string
            Uuid = "uuid",
            TimestampMilliseconds = "timestamp-millis",
            TimestampMicroseconds = "timestamp-micros",
            Decimal = "decimal",
            Duration = "duration",
            TimeMilliseconds = "time-millis",
            TimeMicrosecond = "time-micros ",
            Date = "date";
    }

    internal abstract TypeSchema BaseTypeSchema { get; set; }
    internal abstract string LogicalTypeName { get; }

     
    protected LogicalTypeSchema(Type runtimeType) : base(runtimeType, new Dictionary<string, string>())
    {
    }

    internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
    {
        writer.WriteStartObject();
        writer.WritePropertyName("type");
        BaseTypeSchema.ToJsonSafe(writer, seenSchemas);
        writer.WriteProperty("logicalType", LogicalTypeName);
        writer.WriteEndObject();
    }

    internal abstract object ConvertToLogicalValue(object baseValue, LogicalTypeSchema schema, Type type);
}