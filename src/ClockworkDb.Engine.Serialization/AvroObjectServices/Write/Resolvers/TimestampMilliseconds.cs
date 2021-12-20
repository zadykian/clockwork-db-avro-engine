using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.Features.Serialize;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class TimestampMilliseconds
{
    internal Encoder.WriteItem Resolve(TimestampMillisecondsSchema schema)
    {
        return (value, encoder) =>
        {
            if (!(schema.BaseTypeSchema is LongSchema))
            {
                throw new AvroTypeMismatchException($"[TimestampMilliseconds] required to write against [long] of [Long] schema but found [{schema.BaseTypeSchema}]");
            }

            var bytesValue = (long)schema.ConvertToBaseValue(value, schema);
            encoder.WriteLong(bytesValue);
        };
    }
}