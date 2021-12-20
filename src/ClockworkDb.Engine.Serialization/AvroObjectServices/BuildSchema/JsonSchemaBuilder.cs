using System.Globalization;
using System.Runtime.Serialization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json.Linq;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
///     Class responsible for building the internal representation of the schema given a JSON string.
/// </summary>
internal sealed class JsonSchemaBuilder
{
    private static readonly Dictionary<string, Func<PrimitiveTypeSchema>> primitiveRuntimeType
        = new(comparer: StringComparer.InvariantCultureIgnoreCase)
        {
            { "null", () => new NullSchema() },
            { "boolean", () => new BooleanSchema() },
            { "int", () => new IntSchema() },
            { "long", () => new LongSchema() },
            { "float", () => new FloatSchema() },
            { "double", () => new DoubleSchema() },
            { "bytes", () => new BytesSchema() },
            { "string", () => new StringSchema() },
        };

    private static readonly Dictionary<string, SortOrder> sortValue = new()
    {
        { SortOrder.Ascending.ToString().ToUpperInvariant(), SortOrder.Ascending },
        { SortOrder.Descending.ToString().ToUpperInvariant(), SortOrder.Descending },
        { SortOrder.Ignore.ToString().ToUpperInvariant(), SortOrder.Ignore }
    };

    /// <summary>
    ///     Parses the JSON schema.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <returns>Schema internal representation as a tree of nodes.</returns>
    /// <exception cref="System.ArgumentException">Thrown when <paramref name="schema"/> is null or empty.</exception>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when <paramref name="schema"/> is invalid schema.</exception>
    internal TypeSchema BuildSchema(string schema)
    {
        if (string.IsNullOrEmpty(schema))
        {
            throw new ArgumentNullException(nameof(schema));
        }

        JToken token = JToken.Parse(schema);
        if (token == null)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "'{0}' is invalid JSON.", schema));
        }

        return Parse(token, null, new Dictionary<string, NamedSchema>());
    }

    /// <summary>
    /// Parses the specified token.
    /// </summary>
    /// <param name="token">The token.</param>
    /// <param name="parent">The parent schema.</param>
    /// <param name="namedSchemas">The schemas.</param>
    /// <returns>
    /// Schema internal representation.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when JSON schema type is not supported.</exception>
    private TypeSchema Parse(JToken token, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas)
    {
        if (token.Type == JTokenType.Object)
        {
            return ParseJsonObject(token as JObject, parent, namedSchemas);
        }

        if (token.Type == JTokenType.String)
        {
            var t = (string)token;
            if (namedSchemas.ContainsKey(t))
            {
                return namedSchemas[t];
            }

            if (parent != null && namedSchemas.ContainsKey(parent.Namespace + "." + t))
            {
                return namedSchemas[parent.Namespace + "." + t];
            }

            // Primitive.
            return ParsePrimitiveTypeFromString(t);
        }

        if (token.Type == JTokenType.Array)
        {
            return ParseUnionType(token as JArray, parent, namedSchemas);
        }

        throw new SerializationException(
            string.Format(CultureInfo.InvariantCulture, "Unexpected Json schema type '{0}'.", token));
    }

    /// <summary>
    /// Parses the JSON object.
    /// </summary>
    /// <param name="token">The object.</param>
    /// <param name="parent">The parent schema.</param>
    /// <param name="namedSchemas">The schemas.</param>
    /// <returns>
    /// Schema internal representation.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when JSON schema type is invalid.</exception>
    private TypeSchema ParseJsonObject(JObject token, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas)
    {
        JToken tokenType = token[Token.Type];
        if (tokenType.Type == JTokenType.String)
        {
            var typeString = token.RequiredProperty<string>(Token.Type);
            Enum.TryParse(typeString, true, out AvroType type);

            var logicalType = token.OptionalProperty<string>(Token.LogicalType);
            if (logicalType != null)
            {
                return ParseLogicalType(token, parent, namedSchemas, logicalType);
            }

            switch (type)
            {
                case AvroType.Record:
                    return ParseRecordType(token, parent, namedSchemas);
                case AvroType.Enum:
                    return ParseEnumType(token, parent, namedSchemas);
                case AvroType.Array:
                    return ParseArrayType(token, parent, namedSchemas);
                case AvroType.Map:
                    return ParseMapType(token, parent, namedSchemas);
                case AvroType.Fixed:
                    return ParseFixedType(token, parent);
                default:
                {
                    if (primitiveRuntimeType.ContainsKey(type.ToString()))
                    {
                        return ParsePrimitiveTypeFromObject(token);
                    }

                    throw new SerializationException(
                        string.Format(CultureInfo.InvariantCulture, "Invalid type specified: '{0}'.", type));
                }
            }
        }

        if (tokenType.Type == JTokenType.Array)
        {
            return ParseUnionType(tokenType as JArray, parent, namedSchemas);
        }

        throw new SerializationException(
            string.Format(CultureInfo.InvariantCulture, "Invalid type specified: '{0}'.", tokenType));
    }

    /// <summary>
    /// Parses a union token.
    /// </summary>
    /// <param name="unionToken">The union token.</param>
    /// <param name="parent">The parent.</param>
    /// <param name="namedSchemas">The named schemas.</param>
    /// <returns>
    /// Schema internal representation.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when union schema type is invalid.</exception>
    private TypeSchema ParseUnionType(JArray unionToken, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas)
    {
        var types = new HashSet<string>();
        var schemas = new List<TypeSchema>();
        foreach (var typeAlternative in unionToken.Children())
        {
            var schema = Parse(typeAlternative, parent, namedSchemas);
            if (schema.Type == AvroType.Union)
            {
                throw new SerializationException(
                    string.Format(CultureInfo.InvariantCulture, "Union schemas cannot be nested:'{0}'.", unionToken));
            }

            if (types.Contains(schema.Type.ToString()))
            {
                throw new SerializationException(
                    string.Format(CultureInfo.InvariantCulture, "Unions cannot contains schemas of the same type: '{0}'.", schema.Type));
            }

            types.Add(schema.Type.ToString());
            schemas.Add(schema);
        }

        return new UnionSchema(schemas, typeof(object));
    }

    /// <summary>
    /// Parses a JSON object representing an Avro enumeration to a <see cref="EnumSchema"/>.
    /// </summary>
    /// <param name="enumeration">The JSON token that represents the enumeration.</param>
    /// <param name="parent">The parent schema.</param>
    /// <param name="namedSchemas">The named schemas.</param>
    /// <returns>
    /// Instance of <see cref="TypeSchema" /> containing IR of the enumeration.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when <paramref name="enumeration"/> contains invalid symbols.</exception>
    private TypeSchema ParseEnumType(JObject enumeration, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas)
    {
        var name = enumeration.RequiredProperty<string>(Token.Name);
        var nspace = GetNamespace(enumeration, parent, name);
        var enumName = new SchemaName(name, nspace);

        var doc = enumeration.OptionalProperty<string>(Token.Doc);
        var aliases = GetAliases(enumeration);
        var attributes = new NamedEntityAttributes(enumName, aliases, doc);

        List<string> symbols = enumeration.OptionalArrayProperty(
            Token.Symbols,
            (symbol, index) =>
            {
                if (symbol.Type != JTokenType.String)
                {
                    throw new SerializationException(
                        string.Format(CultureInfo.InvariantCulture, "Expected an enum symbol of type string however the type of the symbol is '{0}'.", symbol.Type));
                }
                return (string)symbol;
            });

        Dictionary<string, string> customAttributes = enumeration.GetAttributesNotIn(StandardProperties.Enumeration);
        var result = new EnumSchema(attributes, typeof(AvroEnum), customAttributes);
        namedSchemas.Add(result.FullName, result);
        symbols.ForEach(result.AddSymbol);
        return result;
    }

    /// <summary>
    /// Parses a JSON object representing an Avro array.
    /// </summary>
    /// <param name="array">JSON representing the array.</param>
    /// <param name="parent">The parent.</param>
    /// <param name="namedSchemas">The named schemas.</param>
    /// <returns>
    /// A corresponding schema.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when no 'items' property is found in <paramref name="array" />.</exception>
    private TypeSchema ParseArrayType(JObject array, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas)
    {
        var itemType = array[Token.Items];
        if (itemType == null)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Property 'items' cannot be found inside the array '{0}'.", array));
        }

        var elementSchema = Parse(itemType, parent, namedSchemas);
        return new ArraySchema(elementSchema, typeof(Array));
    }

    /// <summary>
    /// Parses a JSON object representing an Avro map.
    /// </summary>
    /// <param name="map">JSON representing the map.</param>
    /// <param name="parent">The parent.</param>
    /// <param name="namedSchemas">The named schemas.</param>
    /// <returns>
    /// A corresponding schema.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when 'values' property is not found in <paramref name="map" />.</exception>
    private TypeSchema ParseMapType(JObject map, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas)
    {
        var valueType = map[Token.Values];
        if (valueType == null)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Property 'values' cannot be found inside the map '{0}'.", map));
        }

        var valueSchema = Parse(valueType, parent, namedSchemas);
        return new MapSchema(new StringSchema(), valueSchema, typeof(Dictionary<string, object>));
    }

    private TypeSchema ParseLogicalType(JObject token, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas, string logicalType)
    {
        TypeSchema result;
        switch (logicalType)
        {
            case LogicalTypeSchema.LogicalTypeEnum.Uuid:
                result = new UuidSchema();
                break;
            case LogicalTypeSchema.LogicalTypeEnum.Decimal:
                var scale = token.OptionalProperty<int>(nameof(DecimalSchema.Scale).ToLower());
                var precision = token.RequiredProperty<int>(nameof(DecimalSchema.Precision).ToLower());
                result = new DecimalSchema(typeof(decimal), precision, scale);
                break;
            case LogicalTypeSchema.LogicalTypeEnum.Duration:
                result = new DurationSchema();
                break;
            case LogicalTypeSchema.LogicalTypeEnum.Date:
                result = new DateSchema();
                break;
            default:
                throw new SerializationException(
                    string.Format(CultureInfo.InvariantCulture, "Unknown LogicalType schema :'{0}'.", logicalType));
        }

        return result;
    }

    /// <summary>
    /// Parses the record type.
    /// </summary>
    /// <param name="record">The record.</param>
    /// <param name="parent">The parent schema.</param>
    /// <param name="namedSchemas">The named schemas.</param>
    /// <returns>
    /// Schema internal representation.
    /// </returns>
    /// <exception cref="System.Runtime.Serialization.SerializationException">Thrown when <paramref name="record"/> can not be parsed properly.</exception>
    private TypeSchema ParseRecordType(JObject record, NamedSchema parent, Dictionary<string, NamedSchema> namedSchemas)
    {
        var name = record.RequiredProperty<string>(Token.Name);
        var nspace = GetNamespace(record, parent, name);
        var recordName = new SchemaName(name, nspace);

        var doc = record.OptionalProperty<string>(Token.Doc);
        var aliases = GetAliases(record);
        var attributes = new NamedEntityAttributes(recordName, aliases, doc);

        Dictionary<string, string> customAttributes = record.GetAttributesNotIn(StandardProperties.Record);
        var result = new RecordSchema(attributes, typeof(AvroRecord), customAttributes);
        namedSchemas.Add(result.FullName, result);

        List<RecordField> fields = record.OptionalArrayProperty(
            Token.Fields,
            (field, index) =>
            {
                if (field.Type != JTokenType.Object)
                {
                    throw new SerializationException(
                        string.Format(CultureInfo.InvariantCulture, "Property 'fields' has invalid value '{0}'.", field));
                }
                return ParseRecordField(field as JObject, result, namedSchemas, index);
            });

        fields.ForEach(result.AddField);
        return result;
    }

    /// <summary>
    /// Parses the record field.
    /// </summary>
    private RecordField ParseRecordField(
        JObject field,
        NamedSchema parent,
        Dictionary<string, NamedSchema> namedSchemas,
        int position)
    {
        var name = field.RequiredProperty<string>(Token.Name);
        var doc = field.OptionalProperty<string>(Token.Doc);
        var order = field.OptionalProperty<string>(Token.Order);
        var aliases = GetAliases(field);
        var fieldType = field[Token.Type];
        if (fieldType == null)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Record field schema '{0}' has no type.", field));
        }

        TypeSchema type = Parse(fieldType, parent, namedSchemas);
        object defaultValue = null;
        bool hasDefaultValue = field[Token.Default] != null;
        if (hasDefaultValue)
        {
            var objectParser = new JsonObjectParser();
            defaultValue = objectParser.Parse(type, field[Token.Default].ToString());
        }

        var orderValue = SortOrder.Ascending;
        if (!string.IsNullOrEmpty(order))
        {
            if (!sortValue.ContainsKey(order.ToUpperInvariant()))
            {
                throw new SerializationException(
                    string.Format(CultureInfo.InvariantCulture, "Invalid sort order of the field '{0}'.", order));
            }
            orderValue = sortValue[order.ToUpperInvariant()];
        }

        var fieldName = new SchemaName(name);
        var attributes = new NamedEntityAttributes(fieldName, aliases, doc);

        return new RecordField(attributes, type, orderValue, hasDefaultValue, defaultValue, null, position);
    }

    /// <summary>
    ///     Parses the primitive type schema.
    /// </summary>
    /// <param name="token">The token.</param>
    /// <returns>Schema internal representation.</returns>
    private TypeSchema ParsePrimitiveTypeFromString(string token)
    {
        return CreatePrimitiveTypeSchema(token, new Dictionary<string, string>());
    }

    /// <summary>
    ///     Parses the primitive type schema.
    /// </summary>
    /// <param name="token">The token.</param>
    /// <param name="usingTypeName">Will use this type name for creating the primitive type.</param>
    /// <returns>Schema internal representation.</returns>
    private static TypeSchema ParsePrimitiveTypeFromObject(JObject token, string usingTypeName = null)
    {
        usingTypeName ??= token.RequiredProperty<string>(Token.Type);
        var customAttributes = token.GetAttributesNotIn(StandardProperties.Primitive);
        return CreatePrimitiveTypeSchema(usingTypeName, customAttributes);
    }

    /// <summary>
    ///     Creates the primitive type schema.
    /// </summary>
    /// <param name="type">The type.</param>
    /// <param name="attributes">The attributes.</param>
    /// <returns>Schema internal representation.</returns>
    private static TypeSchema CreatePrimitiveTypeSchema(string type, Dictionary<string, string> attributes)
    {
        var result = primitiveRuntimeType[type]();
        foreach (var (key, value) in attributes)
        {
            result.AddAttribute(key, value);
        }
        return result;
    }

    private static FixedSchema ParseFixedType(JObject type, NamedSchema parent)
    {
        var name = type.RequiredProperty<string>(Token.Name);
        var @namespace = GetNamespace(type, parent, name);
        var fixedName = new SchemaName(name, @namespace);

        var size = type.RequiredProperty<int>(Token.Size);
        if (size <= 0)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Only positive size of fixed values allowed: '{0}'.", size));
        }

        var aliases = GetAliases(type);
        var attributes = new NamedEntityAttributes(fixedName, aliases, string.Empty);

        var customAttributes = type.GetAttributesNotIn(StandardProperties.Record);
        var result = new FixedSchema(attributes, size, typeof(byte[]), customAttributes);
        return result;
    }

    private static string GetNamespace(JToken type, NamedSchema parentSchema, string name)
    {
        var @namespace = type.OptionalProperty<string>(Token.Namespace);
        if (string.IsNullOrEmpty(@namespace) && !name.Contains('.') && parentSchema != null)
        {
            @namespace = parentSchema.Namespace;
        }
        return @namespace;
    }

    private static IEnumerable<string> GetAliases(JToken type)
        => type.OptionalArrayProperty(
            Token.Aliases,
            (alias, _) =>
            {
                if (alias.Type != JTokenType.String)
                {
                    throw new SerializationException(
                        string.Format(CultureInfo.InvariantCulture, "Property 'aliases' has invalid value '{0}'.", alias));
                }

                var result = (string)alias;
                if (string.IsNullOrEmpty(result))
                {
                    throw new SerializationException(
                        string.Format(CultureInfo.InvariantCulture, "Alias is not allowed to be null or empty."));
                }

                return result;
            });
}