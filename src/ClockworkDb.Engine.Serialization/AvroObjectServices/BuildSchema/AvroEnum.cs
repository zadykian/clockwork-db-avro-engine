using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
/// Represents Avro enumeration.
/// </summary>
[SuppressMessage("Microsoft.Naming", "CA1711:IdentifiersShouldNotHaveIncorrectSuffix", Justification = "Represents Avro enumeration.")]
internal sealed class AvroEnum
{
    private readonly EnumSchema schema;
    private int value;

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroEnum"/> class.
    /// </summary>
    /// <param name="schema">The schema.</param>
    internal AvroEnum(Schema schema)
    {
        this.schema = schema as EnumSchema;
        if (this.schema == null)
        {
            throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "Enum schema expected."), nameof(schema));
        }
    }

    /// <summary>
    /// Gets or sets the value.
    /// </summary>
    internal string Value
    {
        get => schema.GetSymbolByValue(value);

        set => this.value = schema.GetValueBySymbol(value);
    }

    /// <summary>
    /// Gets or sets the integer value.
    /// </summary>
    /// <value>
    /// The integer value.
    /// </value>
    internal int IntegerValue
    {
        get => value;

        set => this.value = value;
    }
}