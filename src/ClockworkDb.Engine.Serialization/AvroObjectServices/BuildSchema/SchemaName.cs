using System.Globalization;
using System.Runtime.Serialization;
using System.Text.RegularExpressions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
/// Represents a name of the schema.
/// </summary>
internal sealed class SchemaName : IComparable<SchemaName>, IEquatable<SchemaName>
{
    private static readonly Regex NamePattern = new("^[A-Za-z_][A-Za-z0-9_]*$");
    private static readonly Regex NamespacePattern = new("^([A-Za-z_][A-Za-z0-9_]*)?(?:\\.[A-Za-z_][A-Za-z0-9_]*)*$");

    private readonly string name;
    private readonly string @namespace;

    /// <summary>
    ///     Initializes a new instance of the <see cref="SchemaName" /> class.
    /// </summary>
    /// <param name="name">The name.</param>
    internal SchemaName(string name) : this(name, string.Empty)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="SchemaName" /> class.
    /// </summary>
    /// <param name="name">The name.</param>
    /// <param name="namespace">The namespace.</param>
    /// <exception cref="System.ArgumentException">Thrown when <paramref name="name"/> is empty or null.</exception>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when any argument is invalid.</exception>
    internal SchemaName(string name, string @namespace)
    {
        if (string.IsNullOrEmpty(name))
        {
            throw new ArgumentException(
                string.Format(CultureInfo.InvariantCulture, "Name is not allowed to be null or empty."), nameof(name));
        }

        this.@namespace = @namespace ?? string.Empty;

        int lastDot = name.LastIndexOf('.');
        if (lastDot == name.Length - 1)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Invalid name specified '{0}'.", name));
        }

        if (lastDot != -1)
        {
            this.name = name.Substring(lastDot + 1, name.Length - lastDot - 1);
            this.@namespace = name.Substring(0, lastDot);
        }
        else
        {
            this.name = name;
        }

        CheckNameAndNamespace();
    }

    /// <summary>
    ///     Gets the name.
    /// </summary>
    internal string Name => name;

    /// <summary>
    ///     Gets the namespace.
    /// </summary>
    internal string Namespace => @namespace;

    /// <summary>
    ///     Gets the full name.
    /// </summary>
    internal string FullName => string.IsNullOrEmpty(@namespace) ? name : @namespace + "." + name;

    /// <summary>
    ///     Compares the current object with another object of the same type.
    /// </summary>
    /// <param name="other">An object to compare with this object.</param>
    /// <returns>
    ///     A 32-bit signed integer that indicates the relative order of the objects being compared.
    ///     The return value has the following meanings: Value Meaning Less than zero This object is less than the
    ///     <paramref
    ///         name="other" />
    ///     parameter.
    ///     Zero This object is equal to <paramref name="other" />.
    ///     Greater than zero This object is greater than <paramref name="other" />.
    /// </returns>
    public int CompareTo(SchemaName other)
    {
        if (other == null)
        {
            return 1;
        }

        return string.Compare(FullName, other.FullName, StringComparison.Ordinal);
    }

    /// <summary>
    ///     Indicates whether the current object is equal to another object of the same type.
    /// </summary>
    /// <param name="other">An object to compare with this object.</param>
    /// <returns>
    ///     True if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
    /// </returns>
    public bool Equals(SchemaName other)
    {
        return CompareTo(other) == 0;
    }

    /// <summary>
    ///     Determines whether the specified <see cref="System.Object" /> is equal to this instance.
    /// </summary>
    /// <param name="obj">
    ///     The <see cref="System.Object" /> to compare with this instance.
    /// </param>
    /// <returns>
    ///     <c>true</c> if the specified <see cref="System.Object" /> is equal to this instance; otherwise, <c>false</c>.
    /// </returns>
    public override bool Equals(object obj)
    {
        return CompareTo(obj as SchemaName) == 0;
    }

    /// <summary>
    ///     Returns a hash code for this instance.
    /// </summary>
    /// <returns>
    ///     A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
    /// </returns>
    public override int GetHashCode()
    {
        return FullName.GetHashCode();
    }

    private void CheckNameAndNamespace()
    {
        if (!NamePattern.IsMatch(name))
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Name '{0}' contains invalid characters.", name));
        }

        if (!NamespacePattern.IsMatch(@namespace))
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Namespace '{0}' contains invalid characters.", @namespace));
        }
    }
}