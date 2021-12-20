using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class Long
{
    internal void Resolve(object value, IWriter encoder)
    {
        if (!(value is long))
        {
            throw new AvroTypeMismatchException("[Long] required to write against [Long] schema but found " + value.GetType());
        }

        encoder.WriteLong((long)value);
    }
}