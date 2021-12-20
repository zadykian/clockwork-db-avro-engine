using System.Runtime.Serialization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;
using FastMember;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Read.Resolvers;

internal partial class Resolver
{
	private readonly Dictionary<int, Dictionary<string, Func<object>>> readStepsDictionary = new();
	private readonly Dictionary<int, TypeAccessor> accessorDictionary = new();

	private object ResolveRecord(
		RecordSchema currentWriterSchema,
		RecordSchema currentReaderSchema,
		IReader dec,
		Type type)
	{
		var result = FormatterServices.GetUninitializedObject(type);
		var typeHash = type.GetHashCode();

		TypeAccessor accessor;
		Dictionary<string, Func<object>> readSteps;

		if (!accessorDictionary.ContainsKey(typeHash))
		{
			accessor = TypeAccessor.Create(type);
			readSteps = new Dictionary<string, Func<object>>();

			foreach (RecordField wf in currentWriterSchema.Fields)
			{
				if (currentReaderSchema.TryGetField(wf.Name, out var rf))
				{
					string name = rf.Aliases.FirstOrDefault() ?? wf.Name;

					var members = accessor.GetMembers();
					var memberInfo = members.FirstOrDefault(n =>
						n.Name.Equals(name, StringComparison.InvariantCultureIgnoreCase));
					if (memberInfo == null)
					{
						continue;
					}

					Func<object> func = () =>
						Resolve(wf.TypeSchema, rf.TypeSchema, dec, memberInfo.Type) ?? wf.DefaultValue;
					accessor[result, memberInfo.Name] = func.Invoke();

					readSteps.Add(memberInfo.Name, func);
				}
				else
					Skipper.Skip(wf.TypeSchema, dec);
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

	private static class Skipper
	{
		public static void Skip(TypeSchema schema, IReader d)
		{
			switch (schema.Type)
			{
				case AvroType.Null:
					d.SkipNull();
					break;
				case AvroType.Boolean:
					d.SkipBoolean();
					break;
				case AvroType.Int:
					d.SkipInt();
					break;
				case AvroType.Long:
					d.SkipLong();
					break;
				case AvroType.Float:
					d.SkipFloat();
					break;
				case AvroType.Double:
					d.SkipDouble();
					break;
				case AvroType.String:
					d.SkipString();
					break;
				case AvroType.Bytes:
					d.SkipBytes();
					break;
				case AvroType.Record:
					foreach (var field in ((RecordSchema)schema).Fields)
					{
						Skip(field.TypeSchema, d);
					}

					break;
				case AvroType.Enum:
					d.SkipEnum();
					break;
				case AvroType.Fixed:
					d.SkipFixed(((FixedSchema)schema).Size);
					break;
				case AvroType.Array:
				{
					TypeSchema s = ((ArraySchema)schema).ItemSchema;
					for (long n = d.ReadArrayStart(); n != 0; n = d.ReadArrayNext())
					{
						for (long i = 0; i < n; i++) Skip(s, d);
					}
				}
					break;
				case AvroType.Map:
				{
					TypeSchema s = ((MapSchema)schema).ValueSchema;
					for (long n = d.ReadMapStart(); n != 0; n = d.ReadMapNext())
					{
						for (long i = 0; i < n; i++)
						{
							d.SkipString();
							Skip(s, d);
						}
					}
				}
					break;
				case AvroType.Union:
					Skip(((UnionSchema)schema).Schemas[d.ReadUnionIndex()], d);
					break;
				case AvroType.Logical:
					Skip(((LogicalTypeSchema)schema).BaseTypeSchema, d);
					break;
				case AvroType.Error:
					break;
				default:
					throw new AvroException("Unknown schema type: " + schema);
			}
		}
	}
}