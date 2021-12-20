namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    internal object ResolveString(Type type, IReader reader)
    {
        var value = reader.ReadString();

        switch (type)
        {
            case Type _ when type == typeof(string):
                return value;
            case Type _ when type == typeof(decimal):
                return decimal.Parse(value);
            case Type _ when type == typeof(DateTimeOffset):
            case Type _ when type == typeof(DateTimeOffset?):
                return DateTimeOffset.Parse(value);
            case Type _ when type == typeof(Uri):
                return new Uri(value);
        }

        return value;
    }
}