using System.Text.RegularExpressions;
using ClockworkDb.Engine.Serialization;
using NUnit.Framework;

namespace ClockworkDb.Engine.Tests;

/// <summary>
/// AVRO schema generation tests.
/// </summary>
public class AvroApiSchemaTests : AvroApiTestBase
{
	[Test]
	public void GenerateSchemaForSimpleEntityTest()
	{
		const string expectedSchema = @"
			{
				""type"": ""record"",
				""name"": ""SimpleTestEntity"",
				""namespace"": ""ClockworkDb.Engine.Tests"",
				""fields"": [
					{
						""name"": ""Id"",
						""type"": [""null"", ""long""]
					},
					{
						""name"": ""Info"", 
						""type"": [""null"", ""string""]
					}
				]
			}";

		var actualSchema = ApacheAvroApi.GenerateSchema(typeof(SimpleTestEntity));

		Assert.AreEqual(
			expected: Regex.Replace(expectedSchema, @"\s+", string.Empty),
			actual:   actualSchema);
	}
}