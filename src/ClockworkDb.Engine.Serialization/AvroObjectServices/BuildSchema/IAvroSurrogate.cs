namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
///     Provides the methods needed to substitute one type for another by the IAvroSerializer
///     during serialization and deserialization of C# types.
/// </summary>
internal interface IAvroSurrogate
{
    /// <summary>
    /// During serialization deserialization returns a type that substitutes the specified type.
    /// </summary>
    /// <param name="type">The CLR type <see cref="T:System.Type" /> to substitute.</param>
    /// <returns>
    /// The <see cref="T:System.Type" /> to substitute for the <paramref name="type" /> value. This type must be serializable by the <see cref="T:System.Runtime.Serialization.DataContractSerializer" />. For example, it must be marked with the <see cref="T:System.Runtime.Serialization.DataContractAttribute" /> attribute or other mechanisms that the serializer recognizes.
    /// </returns>
    Type GetSurrogateType(Type type);

    /// <summary>
    /// During deserialization, returns an object that is a substitute for the specified object.
    /// </summary>
    /// <param name="obj">The deserialized object to be substituted.</param>
    /// <param name="targetType">The <see cref="T:System.Type" /> that the substituted object should be assigned to.</param>
    /// <returns>
    /// The substituted deserialized object. This object must be of a type that is serializable by the <see cref="Microsoft.Hadoop.Avro.IAvroSerializer{T}" />. For example, it must be marked with the <see cref="T:System.Runtime.Serialization.DataContractAttribute" /> attribute or other mechanisms that the serializer recognizes.
    /// </returns>
    object GetDeserializedObject(object obj, Type targetType);

    /// <summary>
    /// During serialization returns an object that substitutes the specified object.
    /// </summary>
    /// <param name="obj">The object to substitute.</param>
    /// <param name="targetType">The <see cref="T:System.Type" /> that the substituted object should be assigned to.</param>
    /// <returns>
    /// The substituted object that will be serialized. The object must be serializable by the <see cref="Microsoft.Hadoop.Avro.IAvroSerializer{T}" />. For example, it must be marked with the <see cref="T:System.Runtime.Serialization.DataContractAttribute" /> attribute or other mechanisms that the serializer recognizes.
    /// </returns>
    object GetObjectToSerialize(object obj, Type targetType);
}