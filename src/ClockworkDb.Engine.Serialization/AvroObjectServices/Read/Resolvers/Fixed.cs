using System;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers
{
    internal partial class Resolver
    {
        protected virtual object ResolveFixed(FixedSchema writerSchema, TypeSchema readerSchema, IReader d, Type type)
        {
            FixedSchema rs = (FixedSchema)readerSchema;

            FixedModel ru = new FixedModel(rs);
            byte[] bb = ((FixedModel)ru).Value;
            d.ReadFixed(bb);

            if (type == typeof(Guid))
            {
                return new Guid(ru.Value);
            }

            return ru.Value;
        }
    }
}