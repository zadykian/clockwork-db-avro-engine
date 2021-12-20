using System.ComponentModel;
using System.Globalization;
using System.Runtime.Serialization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
/// This class is responsible for parsing a JSON string according to a given JSON 
/// schema and returning the corresponding C# value as object.
/// </summary>
internal sealed class JsonObjectParser
{
    private readonly Dictionary<Type, Func<string, object>> parsersWithoutSchema;
    private readonly Dictionary<Type, Func<TypeSchema, string, object>> parsersWithSchema;

    internal JsonObjectParser()
    {
        parsersWithoutSchema = new Dictionary<Type, Func<string, object>>
        {
            { typeof(BooleanSchema), json => ConvertTo<bool>(json) },
            { typeof(IntSchema), json => ConvertTo<int>(json) },
            { typeof(LongSchema), json => ConvertTo<long>(json) },
            { typeof(FloatSchema), json => ConvertTo<float>(json) },
            { typeof(DoubleSchema), json => ConvertTo<double>(json) },
            { typeof(StringSchema), json => json },
            { typeof(BytesSchema), ConvertToBytes },
            { typeof(NullSchema), ParseNull }
        };

        parsersWithSchema = new Dictionary<Type, Func<TypeSchema, string, object>>
        {
            { typeof(EnumSchema), ParseEnum },
            { typeof(ArraySchema), ParseArray },
            { typeof(UnionSchema), ParseUnion },
            { typeof(MapSchema), ParseMap },
            { typeof(RecordSchema), ParseRecord },
            { typeof(FixedSchema), ParseFixed },
        };
    }

    /// <summary>
    /// Parses a JSON string according to given schema and returns the corresponding object.
    /// </summary>
    /// <param name="schema">The schema.</param>
    /// <param name="json">The JSON object.</param>
    /// <returns>The object.</returns>
    internal object Parse(TypeSchema schema, string json)
    {
        if (parsersWithoutSchema.ContainsKey(schema.GetType()))
        {
            return parsersWithoutSchema[schema.GetType()](json);
        }

        if (parsersWithSchema.ContainsKey(schema.GetType()))
        {
            return parsersWithSchema[schema.GetType()](schema, json);
        }

        throw new SerializationException(
            string.Format(CultureInfo.InvariantCulture, "Unknown schema type '{0}'.", schema.GetType()));
    }

    private object ParseNull(string json)
    {
        if (!string.IsNullOrEmpty(json))
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "'{0}' is not valid. Null is expected.", json));
        }
        return null;
    }

    private AvroEnum ParseEnum(TypeSchema schema, string jsonObject)
    {
        var enumSchema = (EnumSchema)schema;
        if (!enumSchema.Symbols.Contains(jsonObject))
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "'{0}' is not a valid enum value.", jsonObject));
        }

        return new AvroEnum(schema) { Value = jsonObject };
    }

    private object[] ParseArray(TypeSchema schema, string jsonObject)
    {
        var arraySchema = (ArraySchema)schema;
        return JArray
            .Parse(jsonObject)
            .Select(i => Parse(arraySchema.ItemSchema, i.ToString()))
            .ToArray();
    }

    private object ParseUnion(TypeSchema schema, string jsonObject)
    {
        var unionSchema = (UnionSchema)schema;
        return Parse(unionSchema.Schemas[0], jsonObject);
    }

    private IDictionary<string, object> ParseMap(TypeSchema schema, string jsonObject)
    {
        var mapSchema = (MapSchema)schema;
        return JsonConvert
            .DeserializeObject<Dictionary<string, JToken>>(jsonObject)
            .Select(d => new { d.Key, Value = Parse(mapSchema.ValueSchema, d.Value.ToString()) })
            .ToDictionary(o => o.Key, o => o.Value);
    }

    private AvroRecord ParseRecord(TypeSchema schema, string jsonObject)
    {
        var recordSchema = (RecordSchema)schema;
        var result = new AvroRecord(recordSchema);
        var data = JsonConvert.DeserializeObject<Dictionary<string, JToken>>(jsonObject);

        foreach (var datum in data)
        {
            var matchedRecord = recordSchema.Fields.FirstOrDefault(field => field.Name == datum.Key);
            if (matchedRecord == null)
            {
                throw new SerializationException(
                    string.Format(CultureInfo.InvariantCulture,
                        "Could not set default value because JSON object contains fields that do not exist in the schema."));
            }
            result[matchedRecord.Name] = Parse(matchedRecord.TypeSchema, datum.Value.ToString());
        }

        return result;
    }

    private byte[] ParseFixed(TypeSchema schema, string jsonObject)
    {
        var fixedSchema = (FixedSchema)schema;
        var result = ConvertToBytes(jsonObject);

        if (result.Length != fixedSchema.Size)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "'{0}' size does not match the size of fixed schema node.", jsonObject));
        }

        return result;
    }

    private static byte[] ConvertToBytes(string jsonObject)
    {
        var result = new List<byte>();

        for (var i = 0; i < jsonObject.Length; i += char.IsSurrogatePair(jsonObject, i) ? 2 : 1)
        {
            var codepoint = char.ConvertToUtf32(jsonObject, i);

            if (codepoint > 255)
            {
                throw new SerializationException(string.Format(CultureInfo.InvariantCulture, "'{0}' contains invalid characters.", jsonObject));
            }

            result.Add((byte)codepoint);
        }

        return result.ToArray();
    }

    private static T ConvertTo<T>(string jsonObject)
    {
        // https://github.com/dotnet/corefx/pull/8093
        var converter = TypeDescriptor.GetConverter(typeof(T));
        try
        {
            return (T)converter.ConvertFromString(jsonObject);
        }
        catch (Exception e)
        {
            throw new SerializationException(
                string.Format(CultureInfo.InvariantCulture, "Could not parse '{0}' as '{1}'.", jsonObject, typeof(T)),
                e);
        }
    }
}