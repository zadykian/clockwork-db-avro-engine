using System.Threading.Tasks;
using ClockworkDb.Engine.Serialization;
using NUnit.Framework;
using SolTechnology.Avro;

namespace ClockworkDb.Engine.Tests;

/// <summary>
/// <see cref="ApacheAvroApi"/> serialization tests.
/// </summary>
public class AvroApiSerializationTests : AvroApiTestBase
{
	[Test]
	public void SerializationCompletesTest()
	{
		var simpleEntity = new SimpleTestEntity(10, "some-info");
		var bytesSequence = ApacheAvroApi.Serialize(simpleEntity);
		Assert.IsNotEmpty(bytesSequence);
	}
	
	[Test]
	public async ValueTask DeserializationTest()
	{
		var simpleEntity = new SimpleTestEntity(10, "some-info");
		var bytesSequence = ApacheAvroApi.Serialize(simpleEntity);
		var deserialized = await ApacheAvroApi.DeserializeAsync<SimpleTestEntity>(bytesSequence);
		Assert.AreEqual(simpleEntity, deserialized);
	}

	[Test]
	public void AvroConvertTest()
	{
		var simpleEntity = new SimpleTestEntity(10, "some-info");
		var bytesSequence = AvroConvert.Serialize(simpleEntity);
		var deserialized = AvroConvert.Deserialize<SimpleTestEntity>(bytesSequence);
		Assert.AreEqual(simpleEntity, deserialized);
	}
}