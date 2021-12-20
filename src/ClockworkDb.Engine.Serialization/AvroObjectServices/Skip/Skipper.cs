﻿using ClockworkDb.Engine.Serialization.AvroObjectServices.Read;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Skip;

internal static class Skipper
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