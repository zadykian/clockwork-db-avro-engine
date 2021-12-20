using ClockworkDb.Engine.Serialization;
using NUnit.Framework;

namespace ClockworkDb.Engine.Tests;

/// <summary>
/// AVRO schema generation tests.
/// </summary>
public class AvroApiSchemaTests : TestBase
{
	[Test]
	public void GenerateSchemaForSimpleEntityTest()
	{
		var schema = ApacheAvroApi.GenerateSchema(typeof(SimpleTestEntity));
		Assert.IsFalse(string.IsNullOrWhiteSpace(schema));
	}
}