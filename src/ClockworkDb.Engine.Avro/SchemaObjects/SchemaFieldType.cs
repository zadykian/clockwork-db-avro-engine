namespace ClockworkDb.Engine.Avro.SchemaObjects;

/// <summary>
/// Type of a field at a Avro level.
/// </summary>
public enum SchemaFieldType : byte
{
	/// <summary>
	/// Null (absence of value).
	/// </summary>
	Null,

	/// <summary>
	/// Boolean (true/false).
	/// </summary>
	Boolean,

	/// <summary>
	/// 32 bit signed number.
	/// </summary>
	Integer,

	/// <summary>
	/// 64 bit signed number.
	/// </summary>
	Long,

	/// <summary>
	/// 32 bit floating point number.
	/// </summary>
	Float,

	/// <summary>
	/// 64 bit floating point number.
	/// </summary>
	Double,

	/// <summary>
	/// Sequence of unicode characters.
	/// </summary>
	String,

	/// <summary>
	/// Sequence of bytes.
	/// </summary>
	Bytes,

	/// <summary>
	/// Sequence of bytes with fixed length.
	/// </summary>
	Fixed,

	/// <summary>
	/// Complex type which contains multiple fields.
	/// </summary>
	Record
}