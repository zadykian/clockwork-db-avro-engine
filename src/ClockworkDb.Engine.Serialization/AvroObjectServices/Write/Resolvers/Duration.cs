using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class Duration
{
    internal Encoder.WriteItem Resolve(DurationSchema schema)
    {
        return (value, encoder) =>
        {
            if (!(value is TimeSpan))
            {
                throw new AvroTypeMismatchException($"[Duration] required to write against [TimeSpan] of [fixed] schema but found [{value.GetType()}]");
            }

            byte[] bytes = (byte[])schema.ConvertToBaseValue(value, schema);

            encoder.WriteFixed(bytes);
        };
    }
}