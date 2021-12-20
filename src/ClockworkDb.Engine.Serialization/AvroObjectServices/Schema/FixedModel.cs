using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

internal class FixedModel
{
    protected readonly byte[] Value;
    private FixedSchema schema;

    internal FixedSchema Schema
    {
        get => schema;

        set
        {
            if (!(value is FixedSchema))
                throw new AvroException("Schema " + value.Name + " in set is not FixedSchema");

            if ((value as FixedSchema).Size != _value.Length)
                throw new AvroException("Schema " + value.Name + " Size " + (value as FixedSchema).Size + "is not equal to bytes length " + _value.Length);

            schema = value;
        }
    }

    internal FixedModel(FixedSchema schema)
    {
        _value = new byte[schema.Size];
        Schema = schema;
    }

    internal FixedModel(FixedSchema schema, byte[] value)
    {
        _value = new byte[schema.Size];
        Schema = schema;
        Value = value;
    }

    protected FixedModel(uint size)
    {
        _value = new byte[size];
    }

    internal byte[] Value
    {
        get => _value;
        set
        {
            if (value.Length == _value.Length)
            {
                Array.Copy(value, _value, value.Length);
                return;
            }
            throw new AvroException("Invalid length for fixed: " + value.Length + ", (" + Schema + ")");
        }
    }

    public override bool Equals(object obj)
    {
        if (this == obj) return true;
        if (obj == null || !(obj is FixedModel that)) return false;
        if (!that.Schema.Equals(Schema)) return false;
        return !_value.Where((t, i) => _value[i] != that._value[i]).Any();
    }

    public override int GetHashCode()
    {
        return Schema.GetHashCode() + _value.Sum(b => 23 * b);
    }
}