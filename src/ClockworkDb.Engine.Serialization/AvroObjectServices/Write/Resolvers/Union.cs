using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Features;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers;

internal class Union
{
    internal Encoder.WriteItem Resolve(UnionSchema unionSchema)
    {
        var branchSchemas = unionSchema.Schemas.ToArray();
        var branchWriters = new Encoder.WriteItem[branchSchemas.Length];
        int branchIndex = 0;
        foreach (var branch in branchSchemas)
        {
            branchWriters[branchIndex++] = Resolver.ResolveWriter(branch);
        }


        return (v, e) => WriteUnion(unionSchema, branchSchemas, branchWriters, v, e);
    }

    private bool UnionBranchMatches(TypeSchema sc, object obj)
    {
        if (obj == null && sc.Type != AvroType.Null) return false;
        switch (sc.Type)
        {
            case AvroType.Null:
                return obj == null;
            case AvroType.Boolean:
                return obj is bool;
            case AvroType.Int:
                return obj is int;
            case AvroType.Long:
                return obj is long;
            case AvroType.Float:
                return obj is float;
            case AvroType.Double:
                return obj is double;
            case AvroType.Bytes:
                return obj is byte[];
            case AvroType.String:
                return true;
            case AvroType.Error:
            case AvroType.Record:
                return true;
            case AvroType.Enum:
                return obj is System.Enum;
            case AvroType.Array:
                return !(obj is byte[]);
            case AvroType.Map:
                return true;
            case AvroType.Union:
                return false; // Union directly within another union not allowed!
            case AvroType.Fixed:
                //return obj is GenericFixed && (obj as GenericFixed)._schema.Equals(s);
                return obj is FixedModel &&
                       (obj as FixedModel)!.Schema.FullName.Equals((sc as FixedSchema)!.FullName);
            case AvroType.Logical:
                // return (sc as LogicalTypeSchema).IsInstanceOfLogicalType(obj);
                return true;
            default:
                throw new AvroException("Unknown schema type: " + sc.Type);
        }
    }

    private void WriteUnion(UnionSchema unionSchema, TypeSchema[] branchSchemas, Encoder.WriteItem[] branchWriters, object value, IWriter encoder)
    {
        int index = ResolveUnion(unionSchema, branchSchemas, value);
        encoder.WriteUnionIndex(index);
        branchWriters[index](value, encoder);
    }

    private int ResolveUnion(UnionSchema us, TypeSchema[] branchSchemas, object obj)
    {
        for (int i = 0; i < branchSchemas.Length; i++)
        {
            if (UnionBranchMatches(branchSchemas[i], obj)) return i;
        }

        throw new AvroException("Cannot find a match for " + obj.GetType() + " in " + us);
    }
}