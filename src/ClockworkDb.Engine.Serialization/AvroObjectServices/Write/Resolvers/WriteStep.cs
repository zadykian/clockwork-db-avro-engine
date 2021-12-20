using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.Features.Serialize;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class WriteStep
{
	internal Encoder.WriteItem? WriteField { get; set; }
	internal RecordField? Field { get; set; }
}