using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class Fixed
{
    internal Encoder.WriteItem Resolve(FixedSchema es)
    {
        return (value, encoder) =>
        {
            if (!(value is Fixed) || !((FixedModel)value).Schema.Equals(es))
            {
                throw new AvroTypeMismatchException("[GenericFixed] required to write against [Fixed] schema but found " + value.GetType());
            }

            FixedModel ba = (FixedModel)value;
            encoder.WriteFixed(ba.Value);
        };
    }
}