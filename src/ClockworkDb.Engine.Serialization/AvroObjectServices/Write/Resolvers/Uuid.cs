using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.Features;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class Uuid
{
    internal Encoder.WriteItem Resolve(UuidSchema schema)
    {
        return (value, encoder) =>
        {
            if (!(value is Guid))
            {
                throw new AvroTypeMismatchException($"[Uuid] required to write against [Guid] of [string] schema but found [{value.GetType()}]" );
            }

            encoder.WriteString(((Guid)value).ToString());
        };
    }
}