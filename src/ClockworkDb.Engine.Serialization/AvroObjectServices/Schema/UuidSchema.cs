using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

internal sealed class UuidSchema : LogicalTypeSchema
{
	public UuidSchema() : this(typeof(Guid))
	{
	}

	public UuidSchema(Type runtimeType) : base(runtimeType)
	{
		BaseTypeSchema = new StringSchema();
	}

	internal override AvroType Type => AvroType.Logical;
	internal override TypeSchema BaseTypeSchema { get; set; }
	internal override string LogicalTypeName => LogicalTypeEnum.Uuid;

	internal override object ConvertToLogicalValue(object baseValue, LogicalTypeSchema schema, Type type)
	{
		return Guid.Parse((string)baseValue);
	}
}