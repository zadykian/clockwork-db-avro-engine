

using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

namespace ClockworkDb.Engine.Serialization;

public static partial class AvroConvert
{
    /// <summary>
    /// Generates schema for given .NET Type
    /// </summary>
    public static string GenerateSchema(Type type)
    {
        var reader = new ReflectionSchemaBuilder(new AvroSerializerSettings()).BuildSchema(type);

        return reader.ToString();
    }


    /// <summary>
    /// Generates schema for given .NET Type
    /// <paramref name="includeOnlyDataContractMembers"/> indicates if only classes with DataContractAttribute and properties marked with DataMemberAttribute should be returned
    /// </summary>
    public static string GenerateSchema(Type type, bool includeOnlyDataContractMembers)
    {
        var builder = new ReflectionSchemaBuilder(new AvroSerializerSettings(includeOnlyDataContractMembers));
        var schema = builder.BuildSchema(type);

        return schema.ToString();
    }
}