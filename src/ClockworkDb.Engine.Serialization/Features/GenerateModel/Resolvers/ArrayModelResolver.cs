using ClockworkDb.Engine.Serialization.Features.GenerateModel.Models;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;
using Newtonsoft.Json.Linq;

namespace ClockworkDb.Engine.Serialization.Features.GenerateModel.Resolvers;

internal class ArrayModelResolver
{
    internal AvroField ResolveArray(JObject typeObj)
    {
        var avroField = new AvroField();

        // If this is an array of a specific class that's being defined in this area of the json
        if (typeObj["items"] is JObject && ((JObject)typeObj["items"])["type"].ToString() == "record")
        {
            avroField.FieldType = ((JObject)typeObj["items"])["name"] + "[]";
            avroField.Namespace = ((JObject)typeObj["items"])["namespace"]?.ToString();
        }
        else if (typeObj["items"] is JValue value)
        {
            avroField.FieldType = value + "[]";
        }
        else
        {
            throw new InvalidAvroObjectException($"{typeObj}");
        }

        return avroField;
    }
}