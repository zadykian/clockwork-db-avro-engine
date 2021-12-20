using System.Numerics;
using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;
using Newtonsoft.Json;
using NamedSchema = ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract.NamedSchema;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

internal sealed class DecimalSchema : LogicalTypeSchema
{
    internal override AvroType Type => AvroType.Logical;

    internal override TypeSchema BaseTypeSchema { get; set; }
    internal int Precision { get; set; }
    internal int Scale { get; set; }

    internal override string LogicalTypeName => "decimal";

    public DecimalSchema() : this(typeof(decimal))
    {
    }
    public DecimalSchema(Type runtimeType) : this(runtimeType, 29, 14)  //Default C# values
    {
    }

    public DecimalSchema(Type runtimeType, int precision, int scale) : base(runtimeType)
    {
        BaseTypeSchema = new BytesSchema();

        if (precision <= 0)
            throw new AvroException("Property [Precision] of [Decimal] schema has to be greater than 0");

        if (scale < 0)
            throw new AvroException("Property [Scale] of [Decimal] schema has to be greater equal 0");

        if (scale > precision)
            throw new AvroException("Property [Scale] of [Decimal] schema has to be lesser equal [Precision]");

        Scale = scale;
        Precision = precision;
    }

    internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
    {
        writer.WriteStartObject();
        writer.WriteProperty("type", BaseTypeSchema.Type.ToString().ToLowerInvariant());
        writer.WriteProperty("logicalType", LogicalTypeName);
        writer.WriteProperty("precision", Precision);
        writer.WriteProperty("scale", Scale);
        writer.WriteEndObject();
    }

    internal object ConvertToBaseValue(object logicalValue, DecimalSchema schema)
    {
        var avroDecimal = new AvroDecimalValue((decimal)logicalValue);
        var logicalScale = Scale;
        var scale = avroDecimal.Scale;

        //Resize value to match schema Scale property
        int sizeDiff = logicalScale - scale;
        if(sizeDiff < 0)
        {
            throw new AvroException(
                $@"Decimal Scale for value [{logicalValue}] is equal to [{scale}]. This exceeds default setting [{logicalScale}].
Consider adding following attribute to your property:

[AvroDecimal(Precision = 28, Scale = {scale})]
");
        }

        string trailingZeros = new string('0', sizeDiff);
        var logicalValueString = logicalValue.ToString();

        string valueWithTrailingZeros;
        if (logicalValueString.Contains(avroDecimal.SeparatorCharacter))
        {
            valueWithTrailingZeros = $"{logicalValue}{trailingZeros}";
        }
        else
        {
            valueWithTrailingZeros = $"{logicalValue}{avroDecimal.SeparatorCharacter}{trailingZeros}";
        }

        avroDecimal = new AvroDecimalValue(valueWithTrailingZeros);

        var buffer = avroDecimal.UnscaledValue.ToByteArray();
        Array.Reverse(buffer);

        return AvroType.Bytes == schema.BaseTypeSchema.Type
            ? (object)buffer
            : (object)new FixedModel(
                (FixedSchema)schema.BaseTypeSchema,
                GetDecimalFixedByteArray(buffer, ((FixedSchema)schema.BaseTypeSchema).Size,
                    avroDecimal.Sign < 0 ? (byte)0xFF : (byte)0x00));
    }

    internal override object ConvertToLogicalValue(object baseValue, LogicalTypeSchema schema, Type type)
    {
        var buffer = AvroType.Bytes == schema.BaseTypeSchema.Type
            ? (byte[])baseValue
            : ((FixedModel)baseValue).Value;

        Array.Reverse(buffer);
        var avroDecimal = new AvroDecimalValue(new BigInteger(buffer), Scale);


        return AvroDecimalValue.ToDecimal(avroDecimal);
    }

    private static byte[] GetDecimalFixedByteArray(byte[] sourceBuffer, int size, byte fillValue)
    {
        var paddedBuffer = new byte[size];

        var offset = size - sourceBuffer.Length;

        for (var idx = 0; idx < size; idx++)
        {
            paddedBuffer[idx] = idx < offset ? fillValue : sourceBuffer[idx - offset];
        }

        return paddedBuffer;
    }


}