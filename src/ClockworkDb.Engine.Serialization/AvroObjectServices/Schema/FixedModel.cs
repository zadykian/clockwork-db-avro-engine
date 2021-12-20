using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

internal class FixedModel
{
	private readonly byte[] value;
	private FixedSchema schema;

	internal FixedSchema Schema
	{
		get => schema;

		// ReSharper disable once PropertyCanBeMadeInitOnly.Local
		private set
		{
			if (value == null)
				throw new AvroException("Schema in set is not FixedSchema");

			if (value.Size != this.value.Length)
				throw new AvroException("Schema " + value.Name + " Size " + value.Size +
				                        "is not equal to bytes length " + this.value.Length);

			schema = value;
		}
	}

	internal FixedModel(FixedSchema schema)
	{
		value = new byte[schema.Size];
		Schema = schema;
	}

	internal FixedModel(FixedSchema schema, byte[] value)
	{
		this.value = new byte[schema.Size];
		Schema = schema;
		Value = value;
	}

	protected FixedModel(uint size)
	{
		value = new byte[size];
	}

	internal byte[] Value
	{
		get => value;
		// ReSharper disable once PropertyCanBeMadeInitOnly.Local
		private set
		{
			if (value.Length != this.value.Length)
			{
				throw new AvroException("Invalid length for fixed: " + value.Length + ", (" + Schema + ")");
			}

			Array.Copy(value, this.value, value.Length);
		}
	}

	public override bool Equals(object obj)
	{
		if (this == obj) return true;
		if (obj is not FixedModel that) return false;
		if (!that.Schema.Equals(Schema)) return false;
		return !value.Where((_, i) => value[i] != that.value[i]).Any();
	}

	public override int GetHashCode()
	{
		return Schema.GetHashCode() + value.Sum(b => 23 * b);
	}
}