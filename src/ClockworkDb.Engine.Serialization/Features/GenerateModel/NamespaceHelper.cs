//
//
// using ClockworkDb.Engine.Serialization.Features.GenerateModel.Models;
// using Newtonsoft.Json.Linq;
//
// namespace ClockworkDb.Engine.Serialization.Features.GenerateModel;
//
// internal class NamespaceHelper
// {
//     internal void EnsureUniqueNames(AvroModel model)
//     {
//         foreach (IGrouping<string, AvroClass> avroClasses in model.Classes.GroupBy(c => c.ClassName))
//         {
//             if (avroClasses.Count() == 1)
//             {
//                 continue;
//             }
//
//             foreach (var avroClass in avroClasses)
//             {
//                 foreach (var avroField in model.Classes
//                              .SelectMany(c => c.Fields)
//                              .Where(f => (f.FieldType == avroClass.ClassName ||
//                                           f.FieldType == avroClass.ClassName + "[]" ||
//                                           f.FieldType == avroClass.ClassName + "?") &&
//                                          f.Namespace == avroClass.ClassNamespace))
//                 {
//                     avroField.FieldType = avroField.Namespace + avroField.FieldType;
//                 }
//
//                 avroClass.ClassName = avroClass.ClassNamespace + avroClass.ClassName;
//             }
//         }
//     }
//
//     internal string ExtractNamespace(JObject typeObj, string longName, string shortName)
//     {
//         string @namespace = "";
//         if (typeObj.ContainsKey("namespace"))
//         {
//             @namespace = typeObj["namespace"].ToString();
//         }
//         else
//         {
//             int place = longName.LastIndexOf(shortName, StringComparison.InvariantCulture);
//             if (place >= 0)
//             {
//                 @namespace = longName.Remove(place, shortName.Length);
//             }
//         }
//
//         @namespace = @namespace.Replace(".", "");
//
//         return @namespace;
//     }
// }

// todo: remove