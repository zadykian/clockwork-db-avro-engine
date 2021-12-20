namespace ClockworkDb.Engine.Avro.Schema;

/// <summary>
/// Information about field related to used-defined record type.
/// </summary>
/// <param name="FieldName">
/// Name of a field.
/// </param>
/// <param name="FieldType">
/// Type of a field.
/// </param>
public record SchemaFieldInfo(string FieldName, SchemaFieldType FieldType);