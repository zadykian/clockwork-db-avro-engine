using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
/// Schema representing an array.
/// </summary>
internal sealed class ArraySchema : TypeSchema
{
    /// <summary>
    /// Initializes a new instance of the <see cref="ArraySchema" /> class.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <param name="runtimeType">Type of the runtime.</param>
    /// <param name="attributes">The attributes.</param>
    private ArraySchema(
        TypeSchema item,
        Type runtimeType,
        IDictionary<string, string> attributes)
        : base(runtimeType, attributes)
    {
        ItemSchema = item ?? throw new ArgumentNullException(nameof(item));
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="ArraySchema"/> class.
    /// </summary>
    /// <param name="item">The item.</param>
    /// <param name="runtimeType">Type of the runtime.</param>
    internal ArraySchema(
        TypeSchema item,
        Type runtimeType)
        : this(item, runtimeType, new Dictionary<string, string>())
    {
    }

    /// <summary>
    ///     Gets the item schema.
    /// </summary>
    internal TypeSchema ItemSchema { get; }

    /// <summary>
    ///     Converts current not to JSON according to the avro specification.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="seenSchemas">The seen schemas.</param>
    internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
    {
        writer.WriteStartObject();
        writer.WriteProperty("type", "array");
        writer.WritePropertyName("items");
        ItemSchema.ToJson(writer, seenSchemas);
        writer.WriteEndObject();
    }

    /// <summary>
    /// Gets the type of the schema as string.
    /// </summary>
    internal override AvroType Type => AvroType.Array;
}