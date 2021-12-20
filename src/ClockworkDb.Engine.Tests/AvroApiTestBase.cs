// ReSharper disable NotAccessedPositionalProperty.Global
// ReSharper disable MemberCanBePrivate.Global
// ReSharper disable UnusedAutoPropertyAccessor.Global
// ReSharper disable AutoPropertyCanBeMadeGetOnly.Global

using System;

namespace ClockworkDb.Engine.Tests;

/// <summary>
/// Base class for Avro API tests.
/// </summary>
public abstract class AvroApiTestBase
{
	private protected class SimpleTestEntity : IEquatable<SimpleTestEntity>
	{
		public SimpleTestEntity()
		{
		}
		
		public SimpleTestEntity(long id, string info)
		{
			Id = id;
			Info = info;
		}

		public long? Id { get; set; }
		
		public string? Info { get; set; }

		public bool Equals(SimpleTestEntity? other)
		{
			if (ReferenceEquals(null, other)) return false;
			if (ReferenceEquals(this, other)) return true;
			return Id == other.Id && Info == other.Info;
		}

		public override bool Equals(object? obj)
		{
			if (ReferenceEquals(null, obj)) return false;
			if (ReferenceEquals(this, obj)) return true;
			if (obj.GetType() != this.GetType()) return false;
			return Equals((SimpleTestEntity)obj);
		}

		public override int GetHashCode()
		{
			return HashCode.Combine(Id, Info);
		}
	}
}