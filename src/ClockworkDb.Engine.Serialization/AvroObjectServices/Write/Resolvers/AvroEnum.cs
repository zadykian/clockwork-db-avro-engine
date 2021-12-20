using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class AvroEnum
{
    internal Encoder.WriteItem Resolve(EnumSchema schema)
    {
        return (value, e) =>
        {
            if (!schema.Symbols.Contains(value.ToString()))
            {
                throw new AvroException(
                    $"[Enum] Provided value is not of the enum [{schema.Name}] members");
            }

            e.WriteEnum(schema.GetValueBySymbol(value.ToString()));
        };
    }
}