namespace ClockworkDb.Engine.Serialization.Infrastructure.Attributes;

/// <summary>
/// Used to determine type alternatives for field or property.
/// </summary>
[AttributeUsage(AttributeTargets.Field | AttributeTargets.Property)]
internal sealed class AvroUnionAttribute : Attribute
{
    private readonly Type[] typeAlternatives;

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroUnionAttribute"/> class.
    /// </summary>
    /// <param name="typeAlternatives">
    /// The type alternatives.
    /// </param>
    internal AvroUnionAttribute(params Type[] typeAlternatives)
    {
        this.typeAlternatives = typeAlternatives;
    }

    /// <summary>
    /// Gets the type alternatives.
    /// </summary>
    /// <value>
    /// The type alternatives.
    /// </value>
    internal IEnumerable<Type> TypeAlternatives => typeAlternatives;
}