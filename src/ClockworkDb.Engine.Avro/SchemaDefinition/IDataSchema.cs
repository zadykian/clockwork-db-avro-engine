using ClockworkDb.Engine.Avro.SchemaObjects;

namespace ClockworkDb.Engine.Avro.SchemaDefinition;

/// <summary>
/// Data storage's schema representation.
/// </summary>
public interface IDataSchema
{
	/// <summary>
	/// Perform creation of empty AVRO schema, if it doesn't exist already.
	/// </summary>
	ValueTask InitializeAsync();

	/// <summary>
	/// Add new type to AVRO schema.
	/// </summary>
	/// <exception cref="ArgumentException">
	/// Raised when current schema already contains type
	/// with name <paramref name="typeInfo.TypeName"/>.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// Raised in case if AVRO schema is not initialized.
	/// </exception>
	ValueTask AddTypeAsync(SchemaTypeInfo typeInfo);

	/// <summary>
	/// Update existing type in AVRO schema.
	/// </summary>
	/// <exception cref="ArgumentException">
	/// Raised when current schema doesn't contain type
	/// with name <paramref name="typeInfo.TypeName"/>.
	/// </exception>
	/// <exception cref="InvalidOperationException">
	/// Raised in case if AVRO schema is not initialized.
	/// </exception>
	ValueTask UpdateTypeAsync(SchemaTypeInfo typeInfo);

	/// <summary>
	/// Load all types defined in AVRO schema.
	/// </summary>
	/// <exception cref="InvalidOperationException">
	/// Raised in case if AVRO schema is not initialized.
	/// </exception>
	IAsyncEnumerable<SchemaTypeInfo> LoadAllAsync();

	/// <summary>
	/// Get information about type with name <paramref name="typeName"/> defined in AVRO schema.
	/// </summary>
	/// <param name="typeName">
	/// Type's name. Corresponds to <see cref="SchemaTypeInfo.TypeName"/> property.
	/// </param>
	/// <returns></returns>
	ValueTask<SchemaTypeInfo> LoadAsync(string typeName);
}