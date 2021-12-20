using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Extensions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

internal sealed class DateSchema : LogicalTypeSchema
{
    public DateSchema() : this(typeof(DateTime))
    {
    }
    public DateSchema(Type runtimeType) : base(runtimeType)
    {
        BaseTypeSchema = new IntSchema();
    }

    internal override AvroType Type => AvroType.Logical;
    internal override TypeSchema BaseTypeSchema { get; set; }
    internal override string LogicalTypeName => LogicalTypeEnum.Date;
    public object ConvertToBaseValue(object logicalValue, LogicalTypeSchema schema)
    {
        var date = ((DateTime)logicalValue).Date;
        return (date - DateTimeExtensions.UnixEpochDateTime).Days;
    }


    internal override object ConvertToLogicalValue(object baseValue, LogicalTypeSchema schema, Type type)
    {
        var noDays = (int)baseValue;
        return DateTimeExtensions.UnixEpochDateTime.AddDays(noDays);
    }
}