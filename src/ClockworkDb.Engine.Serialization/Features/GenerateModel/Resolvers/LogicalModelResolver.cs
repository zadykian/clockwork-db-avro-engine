//
//
// using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
// using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;
// using Newtonsoft.Json.Linq;
//
// namespace ClockworkDb.Engine.Serialization.Features.GenerateModel.Resolvers;
//
// internal class LogicalModelResolver
// {
//     internal string ResolveLogical(JObject typeObj)
//     {
//         string logicalType = typeObj["logicalType"].ToString();
//
//         switch (logicalType)
//         {
//             case LogicalTypeSchema.LogicalTypeEnum.Date:
//             case LogicalTypeSchema.LogicalTypeEnum.TimestampMicroseconds:
//             case LogicalTypeSchema.LogicalTypeEnum.TimestampMilliseconds:
//                 return "DateTime";
//             case LogicalTypeSchema.LogicalTypeEnum.Decimal:
//                 return "decimal";
//             case LogicalTypeSchema.LogicalTypeEnum.Duration:
//             case LogicalTypeSchema.LogicalTypeEnum.TimeMicrosecond:
//             case LogicalTypeSchema.LogicalTypeEnum.TimeMilliseconds:
//                 return "TimeSpan";
//             case LogicalTypeSchema.LogicalTypeEnum.Uuid:
//                 return "Guid";
//             default:
//                 throw new InvalidAvroObjectException($"Unidentified logicalType {logicalType}");
//         }
//     }
// }