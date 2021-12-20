using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class Null
{
    internal void Resolve(object value, IWriter encoder)
    {
        if (value != null)
        {
            throw new AvroTypeMismatchException("[Null] required to write against [Null] schema but found " + value.GetType());
        }
        encoder.WriteNull();
    }
}