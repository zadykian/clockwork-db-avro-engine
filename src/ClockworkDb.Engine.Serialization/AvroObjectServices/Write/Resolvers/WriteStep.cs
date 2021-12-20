using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class WriteStep
{
	internal Encoder.WriteItem? WriteField { get; set; }
	internal RecordField? Field { get; set; }
}