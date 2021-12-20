namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
///     Specifies Avro serializer settings.
/// </summary>
internal sealed class AvroSerializerSettings : IEquatable<AvroSerializerSettings>
{
    /// <summary>
    ///     Initializes a new instance of the <see cref="AvroSerializerSettings" /> class.
    /// </summary>
    internal AvroSerializerSettings( bool includeOnlyDataContractMembers = false)
    {
        GenerateDeserializer = true;
        GenerateSerializer = true;
        Resolver = new AvroContractResolver(includeOnlyDataContractMembers: includeOnlyDataContractMembers);
        MaxItemsInSchemaTree = 1024;
        UsePosixTime = false;
        KnownTypes = new List<Type>();
    }

    /// <summary>
    ///     Gets or sets a value indicating whether to generate a serializer.
    /// </summary>
    /// <value>
    ///     <c>True</c> if the serializer should be generated; otherwise, <c>false</c>.
    /// </value>
    internal bool GenerateSerializer { get; set; }

    /// <summary>
    ///     Gets or sets a value indicating whether to generate a deserializer.
    /// </summary>
    /// <value>
    ///     <c>True</c> if the deserializer should be generated; otherwise, <c>false</c>.
    /// </value>
    internal bool GenerateDeserializer { get; set; }

    /// <summary>
    ///     Gets or sets a contract resolver.
    /// </summary>
    internal AvroContractResolver Resolver { get; set; }

    /// <summary>
    /// Gets or sets a serialization surrogate.
    /// </summary>
    internal IAvroSurrogate Surrogate { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether DateTime values will be serialized in the Posix format (as a number
    /// of seconds passed from the start of the Unix epoch) or as a number of ticks.
    /// </summary>
    /// <value>
    ///   <c>True</c> if to use Posix format; otherwise, <c>false</c>.
    /// </value>
    internal bool UsePosixTime { get; set; }

    /// <summary>
    ///     Gets or sets the maximum number of items in the schema tree.
    /// </summary>
    /// <value>
    ///     The maximum number of items in the schema tree.
    /// </value>
    internal int MaxItemsInSchemaTree { get; set; }

    /// <summary>
    ///     Gets or sets the known types.
    /// </summary>
    internal IEnumerable<Type> KnownTypes { get; set; }

    /// <summary>
    /// Gets or sets a value indicating whether to use a cache of precompiled serializers.
    /// </summary>
    /// <value>
    ///   <c>True</c> if to use the cache; otherwise, <c>false</c>.
    /// </value>
    internal bool UseCache { get; set; }

    /// <summary>
    ///     Indicates whether the current object is equal to another object of the same type.
    /// </summary>
    /// <param name="other">An object to compare with this object.</param>
    /// <returns>
    ///     True if the current object is equal to the <paramref name="other" /> parameter; otherwise, false.
    /// </returns>
    public bool Equals(AvroSerializerSettings other)
    {
        if (other == null)
        {
            return false;
        }

        return GenerateSerializer == other.GenerateSerializer
               && GenerateDeserializer == other.GenerateDeserializer
               && UsePosixTime == other.UsePosixTime
               && MaxItemsInSchemaTree == other.MaxItemsInSchemaTree
               && Surrogate == other.Surrogate
               && UseCache == other.UseCache
               && Resolver.Equals(other.Resolver)
               && KnownTypes.SequenceEqual(other.KnownTypes);
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
        return Equals(obj as AvroSerializerSettings);
    }

    /// <summary>
    ///     Returns a hash code for this instance.
    /// </summary>
    /// <returns>
    ///     A hash code for this instance, suitable for use in hashing algorithms and data structures like a hash table.
    /// </returns>
    public override int GetHashCode()
    {
        unchecked
        {
            var hashcode = 83;
            hashcode = (hashcode * 89) + GenerateSerializer.GetHashCode();
            hashcode = (hashcode * 89) + GenerateDeserializer.GetHashCode();
            hashcode = (hashcode * 89) + UsePosixTime.GetHashCode();
            hashcode = (hashcode * 89) + MaxItemsInSchemaTree.GetHashCode();
            hashcode = (hashcode * 89) + (Resolver != null ? Resolver.GetHashCode() : 0);
            hashcode = (hashcode * 89) + (Surrogate != null ? Surrogate.GetHashCode() : 0);
            hashcode = (hashcode * 89) + UseCache.GetHashCode();
            if (KnownTypes != null)
            {
                hashcode = (hashcode * 89) + KnownTypes.Count();
                foreach (var knownType in KnownTypes)
                {
                    hashcode = (hashcode * 89) + (knownType != null ? knownType.GetHashCode() : 0);
                }
            }
            return hashcode;
        }
    }
}