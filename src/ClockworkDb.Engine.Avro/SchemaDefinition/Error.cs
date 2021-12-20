using ClockworkDb.Engine.Avro.SchemaObjects;

namespace ClockworkDb.Engine.Avro.SchemaDefinition;

internal static class Error
{
	public static ArgumentException TypeAlreadyDeclared(SchemaTypeInfo schemaTypeInfo)
		=> new($"Type {schemaTypeInfo.TypeName} is already declared in schema.");
}