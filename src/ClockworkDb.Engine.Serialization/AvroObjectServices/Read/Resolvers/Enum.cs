using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    protected virtual object ResolveEnum(EnumSchema writerSchema, TypeSchema readerSchema, IReader d, Type type)
    {
        if (Nullable.GetUnderlyingType(type) != null)
        {
            type = Nullable.GetUnderlyingType(type);
        }

        int position = d.ReadEnum();
        string value = writerSchema.Symbols[position];
        return Enum.Parse(type, value);
    }
}