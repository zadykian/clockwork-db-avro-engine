using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.Features;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class AvroDecimal
{
    internal Encoder.WriteItem Resolve(DecimalSchema schema)
    {
        return (value, encoder) =>
        {
            if (!(schema.BaseTypeSchema is BytesSchema))
            {
                throw new AvroTypeMismatchException($"[Decimal] required to write against [decimal] of [Bytes] schema but found [{schema.BaseTypeSchema}]");
            }

            var bytesValue = (byte[])schema.ConvertToBaseValue(value, schema);
            encoder.WriteBytes(bytesValue);
        };
    }
}