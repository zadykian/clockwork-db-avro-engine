

using ClockworkDb.Engine.Serialization.Features.GenerateModel.Models;
using Newtonsoft.Json.Linq;

namespace ClockworkDb.Engine.Serialization.Features.GenerateModel.Resolvers;

internal class EnumModelResolver
{
    internal void ResolveEnum(JToken propValue, AvroModel model)
    {
        var result = new AvroEnum();

        var name = propValue["name"].ToString().Split('.').Last();
        var symbols = (JArray)propValue["symbols"];

        result.EnumName = name;
        result.Symbols = symbols.Select(s => s.ToString()).ToList();

        model.Enums.Add(result);
    }
}