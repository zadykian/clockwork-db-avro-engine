

namespace ClockworkDb.Engine.Serialization.Infrastructure.Attributes;

[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property, Inherited = false, AllowMultiple = false)]
public sealed class NullableSchemaAttribute : Attribute
{
}