using System.Collections.ObjectModel;
using System.Reflection;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
///  Sort order.
/// </summary>
internal enum SortOrder
{
    /// <summary>
    /// The ascending order.
    /// </summary>
    Ascending,

    /// <summary>
    /// The descending order.
    /// </summary>
    Descending,

    /// <summary>
    /// The order is ignored.
    /// </summary>
    Ignore
}

/// <summary>
///     Class representing a field of the record.
///     For more information please see <a href="http://avro.apache.org/docs/current/spec.html#schema_record">the specification</a>.
/// </summary>
internal sealed class RecordField : Schema
{
    private readonly NamedEntityAttributes namedEntityAttributes;
    private readonly TypeSchema typeSchema;
    private readonly SortOrder order;
    private readonly bool hasDefaultValue;
    private readonly object defaultValue;
    private readonly int position;
    private MemberInfo info;

    /// <summary>
    /// Initializes a new instance of the <see cref="RecordField" /> class.
    /// </summary>
    /// <param name="namedEntityAttributes">The named entity attributes.</param>
    /// <param name="typeSchema">The type schema.</param>
    /// <param name="order">The order.</param>
    /// <param name="hasDefaultValue">Whether the field has a default value or not.</param>
    /// <param name="defaultValue">The default value.</param>
    /// <param name="info">The info.</param>
    /// <param name="position">The position of the field in the schema.</param>
    internal RecordField(
        NamedEntityAttributes namedEntityAttributes,
        TypeSchema typeSchema,
        SortOrder order,
        bool hasDefaultValue,
        object defaultValue,
        MemberInfo info,
        int position)
        : this(namedEntityAttributes, typeSchema, order, hasDefaultValue, defaultValue, info, position, new Dictionary<string, string>())
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="RecordField" /> class.
    /// </summary>
    /// <param name="namedEntityAttributes">The named entity attributes.</param>
    /// <param name="typeSchema">The type schema.</param>
    /// <param name="order">The order.</param>
    /// <param name="hasDefaultValue">Whether the field has a default value or not.</param>
    /// <param name="defaultValue">The default value.</param>
    /// <param name="info">The info.</param>
    /// <param name="position">The position of the field in the schema.</param>
    /// <param name="attributes">The attributes.</param>
    internal RecordField(
        NamedEntityAttributes namedEntityAttributes,
        TypeSchema typeSchema,
        SortOrder order,
        bool hasDefaultValue,
        object defaultValue,
        MemberInfo info,
        int position,
        Dictionary<string, string> attributes)
        : base(attributes)
    {
        this.namedEntityAttributes = namedEntityAttributes;
        this.typeSchema = typeSchema;
        this.order = order;
        this.hasDefaultValue = hasDefaultValue;
        this.defaultValue = defaultValue;
        this.info = info;
        this.position = position;

        ShouldBeSkipped = false;
        UseDefaultValue = false;
    }

    /// <summary>
    ///     Gets the full name.
    /// </summary>
    internal string FullName => namedEntityAttributes.Name.FullName;

    /// <summary>
    ///     Gets the name.
    /// </summary>
    internal string Name => namedEntityAttributes.Name.Name;

    /// <summary>
    ///     Gets the namespace.
    /// </summary>
    internal string Namespace => namedEntityAttributes.Name.Namespace;

    /// <summary>
    ///     Gets the aliases.
    /// </summary>
    internal ReadOnlyCollection<string> Aliases => namedEntityAttributes.Aliases.AsReadOnly();

    /// <summary>
    ///     Gets the doc.
    /// </summary>
    internal string Doc => namedEntityAttributes.Doc;

    /// <summary>
    ///     Gets the type schema.
    /// </summary>
    internal TypeSchema TypeSchema => typeSchema;

    /// <summary>
    ///     Gets the sort order.
    /// </summary>
    internal SortOrder Order => order;

    /// <summary>
    ///     Gets a value indicating whether the field has a default value or not.
    /// </summary>
    internal bool HasDefaultValue => hasDefaultValue;

    /// <summary>
    ///     Gets the default value.
    /// </summary>
    internal object DefaultValue => defaultValue;

    /// <summary>
    /// Gets the position.
    /// </summary>
    internal int Position => position;

    /// <summary>
    /// Gets or sets the member info.
    /// </summary>
    internal MemberInfo MemberInfo
    {
        get => info;
        set => info = value;
    }

    internal NamedEntityAttributes NamedEntityAttributes => namedEntityAttributes;

    /// <summary>
    ///     Converts current not to JSON according to the avro specification.
    /// </summary>
    /// <param name="writer">The writer.</param>
    /// <param name="seenSchemas">The seen schemas.</param>
    internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
    {
        writer.WriteStartObject();
        writer.WriteProperty("name", Name);
        writer.WriteOptionalProperty("namespace", Namespace);
        writer.WriteOptionalProperty("doc", Doc);
        writer.WriteOptionalProperty("aliases", Aliases);
        writer.WritePropertyName("type");
        TypeSchema.ToJson(writer, seenSchemas);
        writer.WriteOptionalProperty("default", defaultValue, hasDefaultValue);

        writer.WriteEndObject();
    }

    /// <summary>
    /// Gets or sets a value indicating whether the field should be skipped or not.
    /// </summary>
    internal bool ShouldBeSkipped
    {
        get; set;
    }

    /// <summary>
    /// Gets or sets a value indicating whether use default value should be used.
    /// </summary>
    /// <value>
    ///   <c>true</c> if [use default value]; otherwise, <c>false</c>.
    /// </value>
    internal bool UseDefaultValue
    {
        get; set;
    }
}