namespace ClockworkDb.Engine.Avro.Schema;

/// <summary>
/// Information about type declared in Apache Avro schema.
/// </summary>
/// <param name="TypeName">
/// Type name.
/// It have to be unique among all defined types.
/// </param>
/// <param name="Fields">
/// Field related to current type.
/// </param>
public readonly record struct SchemaTypeInfo(
	string TypeName,
	IReadOnlyCollection<SchemaFieldInfo> Fields);