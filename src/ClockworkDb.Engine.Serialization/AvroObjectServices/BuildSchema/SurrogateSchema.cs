using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
/// Node for surrogate types.
/// </summary>
internal sealed class SurrogateSchema : TypeSchema
{
    private readonly Type surrogateType;
    private readonly TypeSchema surrogateSchema;

    internal SurrogateSchema(Type originalType, Type surrogateType, TypeSchema surrogateSchema)
        : this(originalType, surrogateType, new Dictionary<string, string>(), surrogateSchema)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SurrogateSchema" /> class.
    /// </summary>
    /// <param name="originalType">Type of the original.</param>
    /// <param name="surrogateType">Type of the surrogate.</param>
    /// <param name="attributes">The attributes.</param>
    /// <param name="surrogateSchema">The surrogate schema.</param>
    internal SurrogateSchema(
        Type originalType,
        Type surrogateType,
        IDictionary<string, string> attributes,
        TypeSchema surrogateSchema)
        : base(originalType, attributes)
    {
        if (originalType == null)
        {
            throw new ArgumentNullException(nameof(originalType));
        }
        if (surrogateType == null)
        {
            throw new ArgumentNullException(nameof(surrogateType));
        }
        if (surrogateSchema == null)
        {
            throw new ArgumentNullException(nameof(surrogateSchema));
        }

        this.surrogateType = surrogateType;
        this.surrogateSchema = surrogateSchema;
    }

    /// <summary>
    /// Gets the type of the original.
    /// </summary>
    internal Type SurrogateType => surrogateType;

    /// <summary>
    /// Gets the surrogate schema.
    /// </summary>
    internal TypeSchema Surrogate => surrogateSchema;

    /// <summary>
    /// Converts current not to JSON according to the avro specification.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="seenSchemas">The seen schemas.</param>
    internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
    {
        surrogateSchema.ToJson(writer, seenSchemas);
    }

    /// <summary>
    /// Gets the type of the schema as string.
    /// </summary>
    internal override AvroType Type => surrogateSchema.Type;
}