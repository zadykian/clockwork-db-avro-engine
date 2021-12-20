using System.Runtime.Serialization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using FastMember;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
    private readonly Dictionary<int, Dictionary<string, Func<object>>> readStepsDictionary = new();
    private readonly Dictionary<int, TypeAccessor> accessorDictionary = new();

    protected virtual object ResolveRecord(RecordSchema writerSchema, RecordSchema readerSchema, IReader dec, Type type)
    {
        object result = FormatterServices.GetUninitializedObject(type);
        var typeHash = type.GetHashCode();

        TypeAccessor accessor;
        Dictionary<string, Func<object>> readSteps;

        if (!accessorDictionary.ContainsKey(typeHash))
        {
            accessor = TypeAccessor.Create(type);
            readSteps = new Dictionary<string, Func<object>>();

            foreach (RecordField wf in writerSchema.Fields)
            {
                if (readerSchema.TryGetField(wf.Name, out var rf))
                {
                    string name = rf.Aliases.FirstOrDefault() ?? wf.Name;

                    var members = accessor.GetMembers();
                    var memberInfo = members.FirstOrDefault(n => n.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
                    if (memberInfo == null)
                    {
                        continue;
                    }

                    Func<object> func = () => Resolve(wf.TypeSchema, rf.TypeSchema, dec, memberInfo.Type) ?? wf.DefaultValue;
                    accessor[result, memberInfo.Name] = func.Invoke();

                    readSteps.Add(memberInfo.Name, func);

                }
                else
                    _skipper.Skip(wf.TypeSchema, dec);
            }

            readStepsDictionary.Add(typeHash, readSteps);
            accessorDictionary.Add(typeHash, accessor);
        }
        else
        {
            accessor = accessorDictionary[typeHash];
            readSteps = readStepsDictionary[typeHash];

            foreach (var readStep in readSteps)
            {
                accessor[result, readStep.Key] = readStep.Value.Invoke();
            }
        }

        return result;
    }
}