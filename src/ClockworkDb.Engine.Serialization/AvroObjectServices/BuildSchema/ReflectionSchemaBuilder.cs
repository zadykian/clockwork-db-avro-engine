using System.Globalization;
using System.Reflection;
using System.Runtime.Serialization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Attributes;
using ClockworkDb.Engine.Serialization.Infrastructure.Extensions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
///     This class creates an avro schema given a c# type.
/// </summary>
internal sealed class ReflectionSchemaBuilder
{
    private static readonly Dictionary<Type, Func<Type, LogicalTypeSchema>> typeToAvroLogicalSchemaMap =
        new()
        {
            { typeof(decimal), type => new DecimalSchema(type) },
            { typeof(Guid), type => new UuidSchema(type) },
            // { typeof(DateTime), type => new TimestampMillisecondsSchema(type) },
            // { typeof(DateTimeOffset), type => new TimestampMillisecondsSchema(type) },
            { typeof(TimeSpan), type => new DurationSchema(type) },
        };


    private static readonly Dictionary<Type, Func<Type, PrimitiveTypeSchema>> typeToAvroPrimitiveSchemaMap =
        new()
        {
            { typeof(AvroNull), type => new NullSchema(type) },
            { typeof(char), type => new IntSchema(type) },
            { typeof(byte), type => new IntSchema(type) },
            { typeof(sbyte), type => new IntSchema(type) },
            { typeof(short), type => new IntSchema(type) },
            { typeof(ushort), type => new IntSchema(type) },
            { typeof(uint), type => new IntSchema(type) },
            { typeof(int), type => new IntSchema(type) },
            { typeof(bool), type => new BooleanSchema() },
            { typeof(long), type => new LongSchema(type) },
            { typeof(ulong), type => new LongSchema(type) },
            { typeof(float), type => new FloatSchema() },
            { typeof(double), type => new DoubleSchema() },
            { typeof(string), type => new StringSchema(type) },
            { typeof(Uri), type => new StringSchema(type) },
            { typeof(byte[]), type => new BytesSchema() },
            { typeof(decimal), type => new StringSchema(type) },
            { typeof(DateTime), type => new LongSchema(type) }
        };

    private readonly AvroSerializerSettings settings;
    private readonly HashSet<Type> knownTypes;

    /// <summary>
    ///     Initializes a new instance of the <see cref="ReflectionSchemaBuilder" /> class.
    /// </summary>
    /// <param name="settings">The settings.</param>
    internal ReflectionSchemaBuilder(AvroSerializerSettings settings = null)
    {
        if (settings == null)
        {
            settings = new AvroSerializerSettings();
        }

        this.settings = settings;
        knownTypes = new HashSet<Type>(this.settings.KnownTypes);
    }

    /// <summary>
    ///     Creates a schema definition.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <returns>
    ///     New instance of schema definition.
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="type"/> parameter is null.</exception>
    internal TypeSchema BuildSchema(Type type)
    {
        if (type == null)
        {
            return new NullSchema();
        }

        AvroContractResolver resolver = settings.Resolver;
        knownTypes.UnionWith(resolver.GetKnownTypes(type) ?? new List<Type>());
        return CreateSchema(false, type, new Dictionary<string, NamedSchema>(), 0);
    }

    private TypeSchema CreateSchema(bool forceNullable,
        Type type,
        Dictionary<string, NamedSchema> schemas,
        uint currentDepth,
        Type prioritizedType = null,
        MemberInfo memberInfo = null)
    {
        if (currentDepth == settings.MaxItemsInSchemaTree)
        {
            throw new SerializationException(string.Format(CultureInfo.InvariantCulture, "Maximum depth of object graph reached."));
        }

        var surrogate = settings.Surrogate;
        if (surrogate != null)
        {
            var surrogateType = surrogate.GetSurrogateType(type);
            if (surrogateType == null || surrogateType.IsUnsupported())
            {
                throw new SerializationException(
                    string.Format(CultureInfo.InvariantCulture, "Type '{0}' is not supported.", surrogateType ?? type));
            }

            if (type != surrogateType)
            {
                return new SurrogateSchema(
                    type,
                    surrogateType,
                    new Dictionary<string, string>(),
                    CreateSchema(forceNullable, surrogateType, schemas, currentDepth + 1));
            }
        }

        var typeInfo = settings.Resolver.ResolveType(type, memberInfo);
        if (typeInfo == null)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Unexpected type info returned for type '{0}'.", type));
        }

        return typeInfo.Nullable || forceNullable
            ? CreateNullableSchema(type, schemas, currentDepth, prioritizedType, memberInfo)
            : CreateNotNullableSchema(type, schemas, currentDepth, memberInfo);
    }

    private TypeSchema CreateNullableSchema(Type type, Dictionary<string, NamedSchema> schemas, uint currentDepth, Type prioritizedType, MemberInfo info)
    {
        var typeSchemas = new List<TypeSchema> { new NullSchema(type) };
        var notNullableType = type;
        var underlyingType = Nullable.GetUnderlyingType(type);
        if (underlyingType != null)
        {
            notNullableType = underlyingType;
        }

        var notNullableSchema = CreateNotNullableSchema(notNullableType, schemas, currentDepth, info);
        if (notNullableSchema is UnionSchema unionSchema)
        {
            typeSchemas.AddRange(unionSchema.Schemas);
        }
        else
        {
            typeSchemas.Add(notNullableSchema);
        }

        typeSchemas = typeSchemas.OrderBy(x =>
                prioritizedType == null && x.Type.ToString() != "Null"
                || prioritizedType != null && x.Type.ToString() == "Null")
            .ToList();

        return new UnionSchema(typeSchemas, type);
    }

    /// <summary>
    /// Creates the avro schema for the specified type.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <param name="schemas">The schemas seen so far.</param>
    /// <param name="currentDepth">The current depth.</param>
    /// <returns>
    /// New instance of schema.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when maximum depth of object graph is reached.</exception>
    private TypeSchema CreateNotNullableSchema(Type type, Dictionary<string, NamedSchema> schemas, uint currentDepth, MemberInfo info)
    {
        //Logical
        TypeSchema schema = TryBuildLogicalTypeSchema(type, info);
        if (schema != null)
        {
            return schema;
        }

        //Primitive
        schema = TryBuildPrimitiveTypeSchema(type);
        if (schema != null)
        {
            return schema;
        }

        //Others
        if (type.IsInterface() || type.IsAbstract() || HasApplicableKnownType(type))
        {
            return BuildKnownTypeSchema(type, schemas, currentDepth, info);
        }

        return BuildComplexTypeSchema(type, schemas, currentDepth, info);
    }

    private static TypeSchema TryBuildPrimitiveTypeSchema(Type type)
    {
        if (!typeToAvroPrimitiveSchemaMap.ContainsKey(type))
        {
            return null;
        }
        return typeToAvroPrimitiveSchemaMap[type](type);
    }

    private static TypeSchema TryBuildLogicalTypeSchema(Type type, MemberInfo info = null)
    {
        if (type == typeof(decimal))
        {
            return BuildDecimalTypeSchema(type, info);
        }

        if (!typeToAvroLogicalSchemaMap.ContainsKey(type))
        {
            return null;
        }

        return typeToAvroLogicalSchemaMap[type](type);
    }

    private static TypeSchema BuildDecimalTypeSchema(Type type, MemberInfo info)
    {
        var decimalAttribute = info?.GetCustomAttributes(false).OfType<AvroDecimalAttribute>().FirstOrDefault();
        if (decimalAttribute != null)
        {
            return new DecimalSchema(type, decimalAttribute.Precision, decimalAttribute.Scale);
        }

        return new DecimalSchema(type);
    }

    /// <summary>
    ///     Generates the schema for a complex type.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <param name="schemas">The schemas.</param>
    /// <param name="currentDepth">The current depth.</param>
    /// <returns>
    ///     New instance of schema.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when <paramref name="type"/> is not supported.</exception>
    private TypeSchema BuildComplexTypeSchema(Type type, Dictionary<string, NamedSchema> schemas, uint currentDepth, MemberInfo info)
    {
        if (type.IsEnum())
        {
            return BuildEnumTypeSchema(type, schemas);
        }

        if (type.IsArray)
        {
            return BuildArrayTypeSchema(type, schemas, currentDepth);
        }

        // Dictionary
        Type dictionaryType = type
            .GetAllInterfaces()
            .SingleOrDefault(t => t.IsGenericType() && t.GetGenericTypeDefinition() == typeof(IDictionary<,>));

        if (dictionaryType != null
            && (dictionaryType.GetGenericArguments()[0] == typeof(string)
                || dictionaryType.GetGenericArguments()[0] == typeof(Uri)))
        {
            return new MapSchema(
                CreateNotNullableSchema(dictionaryType.GetGenericArguments()[0], schemas, currentDepth + 1, info),
                CreateSchema(false, dictionaryType.GetGenericArguments()[1], schemas, currentDepth + 1),
                type);
        }

        // Enumerable
        Type enumerableType = type
            .GetAllInterfaces()
            .SingleOrDefault(t => t.IsGenericType() && t.GetGenericTypeDefinition() == typeof(IEnumerable<>));
        if (enumerableType != null)
        {
            var itemType = enumerableType.GetGenericArguments()[0];
            return new ArraySchema(CreateSchema(false, itemType, schemas, currentDepth + 1), type);
        }

        //Nullable
        var nullable = Nullable.GetUnderlyingType(type);
        if (nullable != null)
        {
            return new NullableSchema(
                type,
                new Dictionary<string, string>(),
                CreateSchema(false, nullable, schemas, currentDepth + 1));
        }

        // Others
        if (type.IsClass() || type.IsValueType())
        {
            return BuildRecordTypeSchema(type, schemas, currentDepth);
        }

        throw new SerializationException(
            string.Format(CultureInfo.InvariantCulture, "Type '{0}' is not supported.", type));
    }

    /// <summary>
    ///     Builds the enumeration type schema.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <param name="schemas">The schemas.</param>
    /// <returns>Enum schema.</returns>
    private TypeSchema BuildEnumTypeSchema(Type type, Dictionary<string, NamedSchema> schemas)
    {
        if (type.IsFlagEnum())
        {
            return new LongSchema(type);
        }

        NamedSchema schema;
        if (schemas.TryGetValue(type.ToString(), out schema))
        {
            return schema;
        }

        var attributes = GetNamedEntityAttributesFrom(type);
        var result = new EnumSchema(attributes, type);
        schemas.Add(type.ToString(), result);
        return result;
    }

    private NamedEntityAttributes GetNamedEntityAttributesFrom(Type type)
    {
        AvroContractResolver resolver = settings.Resolver;
        TypeSerializationInfo typeInfo = resolver.ResolveType(type);
        var name = new SchemaName(typeInfo.Name, typeInfo.Namespace);
        var aliases = typeInfo
            .Aliases
            .Select(alias => string.IsNullOrEmpty(name.Namespace) || alias.Contains(".") ? alias : name.Namespace + "." + alias)
            .ToList();
        return new NamedEntityAttributes(name, aliases, typeInfo.Doc);
    }

    /// <summary>
    ///     Generates the array type schema.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <param name="schemas">The schemas.</param>
    /// <param name="currentDepth">The current depth.</param>
    /// <returns>
    ///     A new instance of schema.
    /// </returns>
    private TypeSchema BuildArrayTypeSchema(Type type, Dictionary<string, NamedSchema> schemas, uint currentDepth)
    {
        Type element = type.GetElementType();
        TypeSchema elementSchema = CreateSchema(false, element, schemas, currentDepth + 1);
        return new ArraySchema(elementSchema, type);
    }

    /// <summary>
    /// Generates the record type schema.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <param name="schemas">The schemas.</param>
    /// <param name="currentDepth">The current depth.</param>
    /// <returns>
    /// Instance of schema.
    /// </returns>
    private TypeSchema BuildRecordTypeSchema(Type type, Dictionary<string, NamedSchema> schemas, uint currentDepth)
    {
        if (schemas.TryGetValue(type.ToString(), out var schema))
        {
            return schema;
        }

        var attr = GetNamedEntityAttributesFrom(type);
        AvroContractResolver resolver = settings.Resolver;
        var record = new RecordSchema(
            attr,
            type);
        schemas.Add(type.ToString(), record);

        var members = resolver.ResolveMembers(type);
        AddRecordFields(members, schemas, currentDepth, record);
        return record;
    }

    private TypeSchema BuildKnownTypeSchema(Type type, Dictionary<string, NamedSchema> schemas, uint currentDepth, MemberInfo info)
    {
        var applicable = GetApplicableKnownTypes(type).ToList();
        if (applicable.Count == 0)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Could not find any matching known type for '{0}'.", type));
        }

        var knownTypeSchemas = new List<TypeSchema>(applicable.Count);
        applicable.ForEach(t => knownTypeSchemas.Add(BuildComplexTypeSchema(t, schemas, currentDepth, info)));
        return new UnionSchema(knownTypeSchemas, type);
    }

    private bool HasApplicableKnownType(Type type)
    {
        return GetApplicableKnownTypes(type).Count(t => t != type) != 0;
    }

    private IEnumerable<Type> GetApplicableKnownTypes(Type type)
    {
        var allKnownTypes = new HashSet<Type>(knownTypes)
        {
            type
        };
        return allKnownTypes.Where(t => t.CanBeKnownTypeOf(type));
    }

    private TypeSchema TryBuildUnionSchema(Type memberType, MemberInfo memberInfo, Dictionary<string, NamedSchema> schemas, uint currentDepth)
    {

        var attribute = memberInfo.GetCustomAttributes(false).OfType<AvroUnionAttribute>().FirstOrDefault();
        if (attribute == null)
        {
            return null;
        }

        var result = attribute.TypeAlternatives.ToList();
        if (memberType != typeof(object) && !memberType.IsAbstract() && !memberType.IsInterface())
        {
            result.Add(memberType);
        }


        return new UnionSchema(result.Select(type => CreateNotNullableSchema(type, schemas, currentDepth + 1, memberInfo)).ToList(), memberType);
    }

    private FixedSchema TryBuildFixedSchema(Type memberType, MemberInfo memberInfo, NamedSchema parentSchema)
    {
        var result = memberInfo.GetCustomAttributes(false).OfType<AvroFixedAttribute>().FirstOrDefault();
        if (result == null)
        {
            return null;
        }

        if (memberType != typeof(byte[]))
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "'{0}' can be set only to members of type byte[].", typeof(AvroFixedAttribute)));
        }

        var schemaNamespace = string.IsNullOrEmpty(result.Namespace) && !result.Name.Contains(".") && parentSchema != null
            ? parentSchema.Namespace
            : result.Namespace;

        return new FixedSchema(
            new NamedEntityAttributes(new SchemaName(result.Name, schemaNamespace), new List<string>(), string.Empty),
            result.Size,
            memberType);
    }

    private void AddRecordFields(
        IEnumerable<MemberSerializationInfo> members,
        Dictionary<string, NamedSchema> schemas,
        uint currentDepth,
        RecordSchema record)
    {
        int index = 0;
        foreach (MemberSerializationInfo info in members)
        {
            var property = info.MemberInfo as PropertyInfo;
            var field = info.MemberInfo as FieldInfo;

            Type memberType;
            if (property != null)
            {
                memberType = property.PropertyType;
            }
            else if (field != null)
            {
                memberType = field.FieldType;
            }
            else
            {
                throw new SerializationException(
                    string.Format(
                        CultureInfo.InvariantCulture,
                        "Type member '{0}' is not supported.",
                        info.MemberInfo.GetType().Name));
            }

            TypeSchema fieldSchema = TryBuildUnionSchema(memberType, info.MemberInfo, schemas, currentDepth)
                                     ?? TryBuildFixedSchema(memberType, info.MemberInfo, record)
                                     ?? CreateSchema(info.Nullable, memberType, schemas, currentDepth + 1, info.DefaultValue?.GetType(), info.MemberInfo);



            var aliases = info
                .Aliases
                .ToList();
            var recordField = new RecordField(
                new NamedEntityAttributes(new SchemaName(info.Name), aliases, info.Doc),
                fieldSchema,
                SortOrder.Ascending,
                info.HasDefaultValue,
                info.DefaultValue,
                info.MemberInfo,
                index++);
            record.AddField(recordField);
        }
    }
}