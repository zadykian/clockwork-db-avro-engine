﻿using System.Dynamic;
using System.Globalization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;

/// <summary>
/// Represents an Avro generic record. It can be considered as a set of name-value pairs.
/// Please, use the <see cref="Microsoft.Hadoop.Avro.AvroSerializer.CreateGeneric"/> method to create the corresponding <see cref="Microsoft.Hadoop.Avro.IAvroSerializer{T}"/>.
/// </summary>
internal sealed class AvroRecord : DynamicObject
{
    private readonly object[] values;
    private readonly RecordSchema schema;

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroRecord"/> class.
    /// </summary>
    /// <param name="schema">The schema.</param>
    internal AvroRecord(Schema schema)
    {
        this.schema = schema as RecordSchema;
        if (this.schema == null)
        {
            throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "Expected record schema."), nameof(schema));
        }
        values = new object[this.schema.Fields.Count];
    }

    /// <summary>
    /// Gets the schema of the record.
    /// </summary>
    internal RecordSchema Schema => schema;

    /// <summary>
    /// Gets or sets the field value with the specified name.
    /// </summary>
    /// <value>
    /// The field value.
    /// </value>
    /// <param name="name">The name.</param>
    /// <returns>Field value.</returns>
    /// <exception cref="System.ArgumentException">Thrown when field value is invalid.</exception>
    /// <exception cref="System.ArgumentOutOfRangeException">Thrown when field value is out of range.</exception>
    internal object this[string name]
    {
        get
        {
            if (string.IsNullOrEmpty(name))
            {
                throw new ArgumentException("Value cannot be null or empty");
            }

            RecordField field = GetField(name);
            if (field == null)
            {
                throw new ArgumentOutOfRangeException(string.Format(CultureInfo.InvariantCulture, "Field with name '{0}' cannot be found.", name));
            }

            return values[field.Position];
        }

        set
        {
            RecordField field = GetField(name);
            if (field == null)
            {
                throw new ArgumentOutOfRangeException(string.Format(CultureInfo.InvariantCulture, "Field with name '{0}' cannot be found.", name));
            }

            values[field.Position] = value;
        }
    }

    /// <summary>
    /// Gets the field value.
    /// </summary>
    /// <typeparam name="T">The type of field.</typeparam>
    /// <param name="name">The name of field.</param>
    /// <returns>Field value.</returns>
    internal T GetField<T>(string name)
    {
        object result = this[name];
        return (T)result;
    }

    /// <summary>
    /// Gets or sets the field value with the specified position.
    /// </summary>
    /// <value>
    /// The field value.
    /// </value>
    /// <param name="position">The position.</param>
    /// <returns>Field value.</returns>
    internal object this[int position]
    {
        get => values[position];

        set => values[position] = value;
    }

    /// <summary>
    /// Provides the implementation for operations that get member values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" /> class can override this method to specify dynamic behavior for operations such as getting a value for a property.
    /// </summary>
    /// <param name="binder">Provides information about the object that called the dynamic operation. The binder.Name property provides the name of the member on which the dynamic operation is performed. For example, for the Console.WriteLine(sampleObject.SampleProperty) statement, where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, binder.Name returns "SampleProperty". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
    /// <param name="result">The result of the get operation. For example, if the method is called for a property, you can assign the property value to <paramref name="result" />.</param>
    /// <returns>
    /// True if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a run-time exception is thrown.).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="binder"/> is null.</exception>
    public override bool TryGetMember(
        GetMemberBinder binder,
        out object result)
    {
        if (binder == null)
        {
            throw new ArgumentNullException(nameof(binder));
        }

        result = null;

        RecordField field = GetField(binder.Name);
        if (field == null)
        {
            return false;
        }
        result = values[field.Position];
        return true;
    }

    /// <summary>
    /// Provides the implementation for operations that set member values. Classes derived from the <see cref="T:System.Dynamic.DynamicObject" /> class can override this method to specify dynamic behavior for operations such as setting a value for a property.
    /// </summary>
    /// <param name="binder">Provides information about the object that called the dynamic operation. The binder.Name property provides the name of the member to which the value is being assigned. For example, for the statement sampleObject.SampleProperty = "Test", where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, binder.Name returns "SampleProperty". The binder.IgnoreCase property specifies whether the member name is case-sensitive.</param>
    /// <param name="value">The value to set to the member. For example, for sampleObject.SampleProperty = "Test", where sampleObject is an instance of the class derived from the <see cref="T:System.Dynamic.DynamicObject" /> class, the <paramref name="value" /> is "Test".</param>
    /// <returns>
    /// True if the operation is successful; otherwise, false. If this method returns false, the run-time binder of the language determines the behavior. (In most cases, a language-specific run-time exception is thrown.).
    /// </returns>
    /// <exception cref="System.ArgumentNullException">Thrown when <paramref name="binder"/> is null.</exception>
    public override bool TrySetMember(SetMemberBinder binder, object value)
    {
        if (binder == null)
        {
            throw new ArgumentNullException(nameof(binder));
        }

        RecordField field = GetField(binder.Name);
        if (field == null)
        {
            return false;
        }
        values[field.Position] = value;
        return true;
    }

    /// <summary>
    /// Gets the record field.
    /// </summary>
    /// <param name="fieldName">Name of the field.</param>
    /// <returns>Record field.</returns>
    private RecordField GetField(string fieldName)
    {
        RecordField field;
        return schema.TryGetField(fieldName, out field)
            ? field
            : null;
    }
}