using System.Diagnostics.CodeAnalysis;
using System.Globalization;
using System.Runtime.Serialization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
/// Matches the schema of writer to the schema of writer.
/// </summary>
internal sealed class EvolutionSchemaBuilder
{
    private readonly Dictionary<TypeSchema, TypeSchema> visited = new();

    internal TypeSchema Build(TypeSchema w, TypeSchema r)
    {
        visited.Clear();
        return BuildDynamic(w, r);
    }

    /// <summary>
    /// Implements double dispatch.
    /// </summary>
    /// <param name="w">The writer schema.</param>
    /// <param name="r">The reader schema.</param>
    /// <returns>True if match.</returns>
    private TypeSchema BuildDynamic(TypeSchema w, TypeSchema r)
    {
        return this.BuildCore((dynamic)w, (dynamic)r);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(IntSchema w, IntSchema r)
    {
        return new IntSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(BooleanSchema w, BooleanSchema r)
    {
        return new BooleanSchema();
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(BytesSchema w, BytesSchema r)
    {
        return new BytesSchema();
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(DoubleSchema w, DoubleSchema r)
    {
        return new DoubleSchema();
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(FloatSchema w, FloatSchema r)
    {
        return new FloatSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(LongSchema w, LongSchema r)
    {
        return new LongSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(StringSchema w, StringSchema r)
    {
        return new StringSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(IntSchema w, LongSchema r)
    {
        return new IntSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(IntSchema w, FloatSchema r)
    {
        return new IntSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(IntSchema w, DoubleSchema r)
    {
        return new IntSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(LongSchema w, FloatSchema r)
    {
        return new LongSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(LongSchema w, DoubleSchema r)
    {
        return new LongSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(FloatSchema w, DoubleSchema r)
    {
        return new FloatSchema(r.RuntimeType);
    }

    private TypeSchema BuildCore(ArraySchema w, ArraySchema r)
    {
        TypeSchema itemSchema = BuildDynamic(w.ItemSchema, r.ItemSchema);
        return itemSchema != null
            ? new ArraySchema(itemSchema, r.RuntimeType)
            : null;
    }

    private TypeSchema BuildCore(MapSchema w, MapSchema r)
    {
        TypeSchema valueSchema = BuildDynamic(w.ValueSchema, r.ValueSchema);
        TypeSchema keySchema = BuildDynamic(w.KeySchema, r.KeySchema);
        return valueSchema != null && keySchema != null
            ? new MapSchema(keySchema, valueSchema, r.RuntimeType)
            : null;
    }

    private TypeSchema BuildCore(EnumSchema w, EnumSchema r)
    {
        bool match = DoNamesMatch(w, r)
                     && w.Symbols.Select((s, i) => i < r.Symbols.Count && r.Symbols[i] == s).All(m => m);
        if (!match)
        {
            return null;
        }

        if (visited.ContainsKey(w))
        {
            return visited[w];
        }

        var attr = new NamedEntityAttributes(new SchemaName(w.Name, w.Namespace), w.Aliases, w.Doc);
        var schema = new EnumSchema(attr, r.RuntimeType);
        r.Symbols.Where(s => !schema.Symbols.Contains(s)).ToList().ForEach(schema.AddSymbol);
        visited.Add(w, schema);
        return schema;
    }

    private TypeSchema BuildCore(FixedSchema w, FixedSchema r)
    {
        bool match = DoNamesMatch(w, r) && w.Size == r.Size;
        if (!match)
        {
            return null;
        }

        if (visited.ContainsKey(w))
        {
            return visited[w];
        }

        var attr = new NamedEntityAttributes(new SchemaName(w.Name, w.Namespace), w.Aliases, w.Doc);
        var schema = new FixedSchema(attr, w.Size, r.RuntimeType);
        visited.Add(w, schema);
        return schema;
    }

    private TypeSchema BuildCore(RecordSchema w, RecordSchema r)
    {
        if (!DoNamesMatch(w, r))
        {
            return null;
        }

        if (visited.ContainsKey(w))
        {
            return visited[w];
        }

        var schema = new RecordSchema(
            new NamedEntityAttributes(new SchemaName(w.Name, w.Namespace), w.Aliases, w.Doc),
            r.RuntimeType);

        visited.Add(w, schema);

        var fields = BuildWriterFields(w, r);
        fields.AddRange(BuildReaderFields(w, r, fields.Count));
        fields.Sort((f1, f2) => f1.Position.CompareTo(f2.Position));
        fields.ForEach(schema.AddField);
        return schema;
    }

    private List<RecordField> BuildReaderFields(RecordSchema w, RecordSchema r, int startPosition)
    {
        var readerFieldsWithDefault = r.Fields.Where(field => field.HasDefaultValue);
        var fieldsToAdd = new List<RecordField>();
        foreach (var readerField in readerFieldsWithDefault)
        {
            if (!w.Fields.Any(f => DoNamesMatch(f, readerField)))
            {
                var newField = new RecordField(
                    readerField.NamedEntityAttributes,
                    readerField.TypeSchema,
                    readerField.Order,
                    readerField.HasDefaultValue,
                    readerField.DefaultValue,
                    readerField.MemberInfo,
                    startPosition++)
                {
                    UseDefaultValue = true
                };

                fieldsToAdd.Add(newField);
            }
        }

        if (r.RuntimeType == typeof(AvroRecord) &&
            r.Fields.Any(rf => !rf.HasDefaultValue && !w.Fields.Any(wf => DoNamesMatch(wf, rf))))
        {
            throw new SerializationException(
                string.Format(
                    CultureInfo.InvariantCulture,
                    "Fields without default values found in type '{0}'. Not corresponding writer fields found.",
                    r.RuntimeType));
        }
        return fieldsToAdd;
    }

    /// <summary>
    /// Matches if the writer's record contains a field with a name not present in the reader's record, the writer's value for that field is ignored.
    /// </summary>
    /// <param name="w">The writer schema.</param>
    /// <param name="r">The reader schema.</param>
    /// <returns>True if match.</returns>
    private List<RecordField> BuildWriterFields(RecordSchema w, RecordSchema r)
    {
        var fields = new List<RecordField>();
        var writerFields = w.Fields.OrderBy(f => f.FullName);
        foreach (var writerField in writerFields)
        {
            writerField.ShouldBeSkipped = true;

            RecordField readerField = r.Fields.SingleOrDefault(f => DoNamesMatch(writerField, f));
            RecordField newField = null;
            if (readerField != null)
            {
                var schema = BuildDynamic(writerField.TypeSchema, readerField.TypeSchema);
                if (schema == null)
                {
                    throw new SerializationException(
                        string.Format(
                            CultureInfo.InvariantCulture,
                            "Field '{0}' in type '{1}' does not match the reader field.",
                            writerField.Name,
                            w.RuntimeType));
                }

                newField = new RecordField(
                    writerField.NamedEntityAttributes,
                    schema,
                    writerField.Order,
                    writerField.HasDefaultValue,
                    writerField.DefaultValue,
                    readerField.MemberInfo,
                    writerField.Position)
                {
                    ShouldBeSkipped = false
                };
            }
            else
            {
                newField = new RecordField(
                    writerField.NamedEntityAttributes,
                    writerField.TypeSchema,
                    writerField.Order,
                    writerField.HasDefaultValue,
                    writerField.DefaultValue,
                    writerField.MemberInfo,
                    writerField.Position)
                {
                    ShouldBeSkipped = true
                };
            }

            fields.Add(newField);
        }
        return fields;
    }

    private TypeSchema BuildCore(UnionSchema w, UnionSchema r)
    {
        var unionSchemas = new List<TypeSchema>();
        foreach (var writerSchema in w.Schemas)
        {
            TypeSchema schema = null;
            foreach (var readerSchema in r.Schemas)
            {
                schema = BuildDynamic(writerSchema, readerSchema);
                if (schema != null)
                {
                    break;
                }
            }

            if (schema == null)
            {
                return null;
            }

            unionSchemas.Add(schema);
        }
        return new UnionSchema(unionSchemas, r.RuntimeType);
    }

    /// <summary>
    ///  If reader's is a union, but writer's is not the first schema in the reader's union 
    ///  that matches the writer's schema is recursively resolved against it. If none match, an error is signalled.
    /// </summary>
    /// <param name="w">The writer schema.</param>
    /// <param name="r">The reader schema.</param>
    /// <returns>True if match.</returns>
    private TypeSchema BuildCore(TypeSchema w, UnionSchema r)
    {
        return r.Schemas.Select(rs => BuildDynamic(w, rs)).SingleOrDefault(s => s != null);
    }

    /// <summary>
    ///  If writer's is a union, but reader's is not then
    ///  if the reader's schema matches the selected writer's schema, it is recursively resolved against it. If they do not match, an error is signalled.
    /// </summary>
    /// <param name="w">The writer schema.</param>
    /// <param name="r">The reader schema.</param>
    /// <returns>True if match.</returns>
    private TypeSchema BuildCore(UnionSchema w, TypeSchema r)
    {
        var schemas = new List<TypeSchema>();
        TypeSchema schemaToReplace = null;
        TypeSchema newSchema = null;
        foreach (var ws in w.Schemas)
        {
            newSchema = BuildDynamic(ws, r);
            if (newSchema != null)
            {
                schemaToReplace = ws;
                break;
            }
        }

        if (newSchema == null)
        {
            throw new SerializationException("Cannot match the union schema.");
        }

        foreach (var s in w.Schemas)
        {
            schemas.Add(s == schemaToReplace ? newSchema : s);
        }

        return new UnionSchema(schemas, newSchema.RuntimeType);
    }

    private TypeSchema BuildCore(TypeSchema w, NullableSchema r)
    {
        var schema = BuildDynamic(w, r.ValueSchema);
        return schema != null
            ? new NullableSchema(r.RuntimeType, schema)
            : null;
    }

    private TypeSchema BuildCore(TypeSchema w, SurrogateSchema r)
    {
        var schema = BuildDynamic(w, r.Surrogate);
        return schema != null
            ? new SurrogateSchema(r.RuntimeType, r.SurrogateType, r.Surrogate)
            : null;
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters", Justification = "Double dispatch.")]
    private TypeSchema BuildCore(NullSchema w, NullSchema r)
    {
        return new NullSchema(r.RuntimeType);
    }

    [SuppressMessage("Microsoft.Usage", "CA1801:ReviewUnusedParameters",
        Justification = "The method is used for double dispatch.")]
    private TypeSchema BuildCore(object w, object r)
    {
        return null;
    }

    private bool DoNamesMatch(NamedSchema w, NamedSchema r)
    {
        return r.FullName == w.FullName || r.Aliases.Contains(w.FullName);
    }

    private bool DoNamesMatch(RecordField w, RecordField r)
    {
        return r.FullName == w.FullName || r.Aliases.Contains(w.FullName);
    }
}