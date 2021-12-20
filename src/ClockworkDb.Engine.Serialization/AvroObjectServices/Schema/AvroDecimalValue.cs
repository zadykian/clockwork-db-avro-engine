using System.Globalization;
using System.Numerics;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

internal readonly struct AvroDecimalValue : IConvertible, IFormattable, IComparable, IComparable<AvroDecimalValue>,
    IEquatable<AvroDecimalValue>
{
    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given double.
    /// </summary>
    /// <param name="value">The double value.</param>
    internal AvroDecimalValue(double value)
        : this((decimal)value)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given float.
    /// </summary>
    /// <param name="value">The float value.</param>
    internal AvroDecimalValue(float value)
        : this((decimal)value)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given decimal.
    /// </summary>
    /// <param name="value">The decimal value.</param>
    internal AvroDecimalValue(decimal value)
    {
        var bytes = GetBytesFromDecimal(value);

        var unscaledValueBytes = new byte[12];
        Array.Copy(bytes, unscaledValueBytes, unscaledValueBytes.Length);

        var unscaledValue = new BigInteger(unscaledValueBytes);
        var scale = bytes[14];

        if (bytes[15] == 128)
            unscaledValue *= BigInteger.MinusOne;

        UnscaledValue = unscaledValue;
        Scale = scale;
        SeparatorCharacter = CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator.ToCharArray()[0];
    }

    internal AvroDecimalValue(string value)
    {
        SeparatorCharacter = CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator.ToCharArray()[0];

        var unscaledValue = string.Join("", value.Split(SeparatorCharacter));
        UnscaledValue = BigInteger.Parse(unscaledValue);

        var indexOfSeparatorCharacter = value.IndexOf(SeparatorCharacter);
        var scale = value.Length - indexOfSeparatorCharacter - 1;
        Scale = scale;
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given int.
    /// </summary>
    /// <param name="value">The int value.</param>
    internal AvroDecimalValue(int value)
        : this(new BigInteger(value), 0)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given long.
    /// </summary>
    /// <param name="value">The long value.</param>
    internal AvroDecimalValue(long value)
        : this(new BigInteger(value), 0)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given unsigned int.
    /// </summary>
    /// <param name="value">The unsigned int value.</param>
    public AvroDecimalValue(uint value)
        : this(new BigInteger(value), 0)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given unsigned long.
    /// </summary>
    /// <param name="value">The unsigned long value.</param>
    public AvroDecimalValue(ulong value)
        : this(new BigInteger(value), 0)
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="AvroDecimalValue"/> class from a given <see cref="BigInteger"/>
    /// and a scale.
    /// </summary>
    /// <param name="unscaledValue">The double value.</param>
    /// <param name="scale">The scale.</param>
    internal AvroDecimalValue(BigInteger unscaledValue, int scale)
    {
        UnscaledValue = unscaledValue;
        Scale = scale;
        SeparatorCharacter = CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator.ToCharArray()[0];
    }

    /// <summary>
    /// Gets the unscaled integer value represented by the current <see cref="AvroDecimalValue"/>.
    /// </summary>
    internal BigInteger UnscaledValue { get; }

    /// <summary>
    /// Gets the scale of the current <see cref="AvroDecimalValue"/>.
    /// </summary>
    internal int Scale { get; }

    internal char SeparatorCharacter { get; }

    /// <summary>
    /// Gets the sign of the current <see cref="AvroDecimalValue"/>.
    /// </summary>
    internal int Sign => UnscaledValue.Sign;

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a string.
    /// </summary>
    /// <returns>A string representation of the numeric value.</returns>
    public override string ToString()
    {
        var number = UnscaledValue.ToString(CultureInfo.CurrentCulture);

        if (Scale > 0)
            return number.Insert(number.Length - Scale,
                CultureInfo.CurrentCulture.NumberFormat.NumberDecimalSeparator);

        return number;
    }

    public static bool operator ==(AvroDecimalValue left, AvroDecimalValue right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(AvroDecimalValue left, AvroDecimalValue right)
    {
        return !left.Equals(right);
    }

    public static bool operator >(AvroDecimalValue left, AvroDecimalValue right)
    {
        return left.CompareTo(right) > 0;
    }

    public static bool operator >=(AvroDecimalValue left, AvroDecimalValue right)
    {
        return left.CompareTo(right) >= 0;
    }

    public static bool operator <(AvroDecimalValue left, AvroDecimalValue right)
    {
        return left.CompareTo(right) < 0;
    }

    public static bool operator <=(AvroDecimalValue left, AvroDecimalValue right)
    {
        return left.CompareTo(right) <= 0;
    }

    public static bool operator ==(AvroDecimalValue left, decimal right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(AvroDecimalValue left, decimal right)
    {
        return !left.Equals(right);
    }

    public static bool operator >(AvroDecimalValue left, decimal right)
    {
        return left.CompareTo(right) > 0;
    }

    public static bool operator >=(AvroDecimalValue left, decimal right)
    {
        return left.CompareTo(right) >= 0;
    }

    public static bool operator <(AvroDecimalValue left, decimal right)
    {
        return left.CompareTo(right) < 0;
    }

    public static bool operator <=(AvroDecimalValue left, decimal right)
    {
        return left.CompareTo(right) <= 0;
    }

    public static bool operator ==(decimal left, AvroDecimalValue right)
    {
        return left.Equals(right);
    }

    public static bool operator !=(decimal left, AvroDecimalValue right)
    {
        return !left.Equals(right);
    }

    public static bool operator >(decimal left, AvroDecimalValue right)
    {
        return left.CompareTo(right) > 0;
    }

    public static bool operator >=(decimal left, AvroDecimalValue right)
    {
        return left.CompareTo(right) >= 0;
    }

    public static bool operator <(decimal left, AvroDecimalValue right)
    {
        return left.CompareTo(right) < 0;
    }

    public static bool operator <=(decimal left, AvroDecimalValue right)
    {
        return left.CompareTo(right) <= 0;
    }

    public static explicit operator byte(AvroDecimalValue value)
    {
        return ToByte(value);
    }

    /// <summary>
    /// Creates a byte from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A byte.</returns>
    public static byte ToByte(AvroDecimalValue value)
    {
        return value.ToType<byte>();
    }

    public static explicit operator sbyte(AvroDecimalValue value)
    {
        return ToSByte(value);
    }

    /// <summary>
    /// Creates a signed byte from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A signed byte.</returns>
    public static sbyte ToSByte(AvroDecimalValue value)
    {
        return value.ToType<sbyte>();
    }

    public static explicit operator short(AvroDecimalValue value)
    {
        return ToInt16(value);
    }

    /// <summary>
    /// Creates a short from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A short.</returns>
    public static short ToInt16(AvroDecimalValue value)
    {
        return value.ToType<short>();
    }

    public static explicit operator int(AvroDecimalValue value)
    {
        return ToInt32(value);
    }

    /// <summary>
    /// Creates an int from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>An int.</returns>
    public static int ToInt32(AvroDecimalValue value)
    {
        return value.ToType<int>();
    }

    public static explicit operator long(AvroDecimalValue value)
    {
        return ToInt64(value);
    }

    /// <summary>
    /// Creates a long from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A long.</returns>
    public static long ToInt64(AvroDecimalValue value)
    {
        return value.ToType<long>();
    }

    public static explicit operator ushort(AvroDecimalValue value)
    {
        return ToUInt16(value);
    }

    /// <summary>
    /// Creates an unsigned short from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>An unsigned short.</returns>
    public static ushort ToUInt16(AvroDecimalValue value)
    {
        return value.ToType<ushort>();
    }

    public static explicit operator uint(AvroDecimalValue value)
    {
        return ToUInt32(value);
    }

    /// <summary>
    /// Creates an unsigned int from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>An unsigned int.</returns>
    public static uint ToUInt32(AvroDecimalValue value)
    {
        return value.ToType<uint>();
    }

    public static explicit operator ulong(AvroDecimalValue value)
    {
        return ToUInt64(value);
    }

    /// <summary>
    /// Creates an unsigned long from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>An unsigned long.</returns>
    public static ulong ToUInt64(AvroDecimalValue value)
    {
        return value.ToType<ulong>();
    }

    public static explicit operator float(AvroDecimalValue value)
    {
        return ToSingle(value);
    }

    /// <summary>
    /// Creates a double from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A double.</returns>
    public static float ToSingle(AvroDecimalValue value)
    {
        return value.ToType<float>();
    }

    public static explicit operator double(AvroDecimalValue value)
    {
        return ToDouble(value);
    }

    /// <summary>
    /// Creates a double from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A double.</returns>
    public static double ToDouble(AvroDecimalValue value)
    {
        return value.ToType<double>();
    }

    public static explicit operator decimal(AvroDecimalValue value)
    {
        return ToDecimal(value);
    }

    /// <summary>
    /// Creates a decimal from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A decimal.</returns>
    public static decimal ToDecimal(AvroDecimalValue value)
    {
        return value.ToType<decimal>();
    }

    public static explicit operator BigInteger(AvroDecimalValue value)
    {
        return ToBigInteger(value);
    }

    /// <summary>
    /// Creates a <see cref="BigInteger"/> from a given <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="value">The <see cref="AvroDecimalValue"/>.</param>
    /// <returns>A <see cref="BigInteger"/>.</returns>
    public static BigInteger ToBigInteger(AvroDecimalValue value)
    {
        var scaleDivisor = BigInteger.Pow(new BigInteger(10), value.Scale);
        var scaledValue = BigInteger.Divide(value.UnscaledValue, scaleDivisor);
        return scaledValue;
    }

    public static implicit operator AvroDecimalValue(byte value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(sbyte value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(short value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(int value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(long value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(ushort value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(uint value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(ulong value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(float value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(double value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(decimal value)
    {
        return new AvroDecimalValue(value);
    }

    public static implicit operator AvroDecimalValue(BigInteger value)
    {
        return new AvroDecimalValue(value, 0);
    }

    /// <summary>
    /// Converts the numeric value of the current <see cref="AvroDecimalValue"/> to a given type.
    /// </summary>
    /// <typeparam name="T">The type to which the value of the current <see cref="AvroDecimalValue"/> should be converted.</typeparam>
    /// <returns>A value of type <typeparamref name="T"/> converted from the current <see cref="AvroDecimalValue"/>.</returns>
    public T ToType<T>()
        where T : struct
    {
        return (T)((IConvertible)this).ToType(typeof(T), null);
    }

    /// <summary>
    /// Converts the numeric value of the current <see cref="AvroDecimalValue"/> to a given type.
    /// </summary>
    /// <param name="conversionType">The type to which the value of the current <see cref="AvroDecimalValue"/> should be converted.</param>
    /// <param name="provider">An System.IFormatProvider interface implementation that supplies culture-specific formatting information.</param>
    /// <returns></returns>
    object IConvertible.ToType(Type conversionType, IFormatProvider provider)
    {
        var scaleDivisor = BigInteger.Pow(new BigInteger(10), Scale);
        var remainder = BigInteger.Remainder(UnscaledValue, scaleDivisor);
        var scaledValue = BigInteger.Divide(UnscaledValue, scaleDivisor);

        if (scaledValue > new BigInteger(Decimal.MaxValue))
            throw new ArgumentOutOfRangeException("value",
                "The value " + UnscaledValue + " cannot fit into " + conversionType.Name + ".");

        var leftOfDecimal = (decimal)scaledValue;
        var rightOfDecimal = ((decimal)remainder) / ((decimal)scaleDivisor);

        var value = leftOfDecimal + rightOfDecimal;
        return Convert.ChangeType(value, conversionType, provider);
    }

    /// <summary>
    /// Returns a value that indicates whether the current <see cref="AvroDecimalValue"/> and a specified object
    /// have the same value.
    /// </summary>
    /// <param name="obj">The object to compare.</param>
    /// <returns>true if the obj argument is an <see cref="AvroDecimalValue"/> object, and its value
    /// is equal to the value of the current <see cref="AvroDecimalValue"/> instance; otherwise false.
    /// </returns>
    public override bool Equals(object obj)
    {
        return (obj is AvroDecimalValue) && Equals((AvroDecimalValue)obj);
    }

    /// <summary>
    /// Returns the hash code for the current <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <returns>The hash code.</returns>
    public override int GetHashCode()
    {
        return UnscaledValue.GetHashCode() ^ Scale.GetHashCode();
    }

    /// <summary>
    /// Returns the <see cref="TypeCode"/> for the current <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <returns><see cref="TypeCode.Object"/>.</returns>
    TypeCode IConvertible.GetTypeCode()
    {
        return TypeCode.Object;
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a boolean.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>true or false, which reflects the value of the current <see cref="AvroDecimalValue"/>.</returns>
    bool IConvertible.ToBoolean(IFormatProvider provider)
    {
        return Convert.ToBoolean(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a byte.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A byte.</returns>
    byte IConvertible.ToByte(IFormatProvider provider)
    {
        return Convert.ToByte(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a char.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>This method always throws an <see cref="InvalidCastException"/>.</returns>
    char IConvertible.ToChar(IFormatProvider provider)
    {
        throw new InvalidCastException("Cannot cast BigDecimal to Char");
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a <see cref="DateTime"/>.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>This method always throws an <see cref="InvalidCastException"/>.</returns>
    DateTime IConvertible.ToDateTime(IFormatProvider provider)
    {
        throw new InvalidCastException("Cannot cast BigDecimal to DateTime");
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a decimal.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A decimal.</returns>
    decimal IConvertible.ToDecimal(IFormatProvider provider)
    {
        return Convert.ToDecimal(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a double.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A double.</returns>
    double IConvertible.ToDouble(IFormatProvider provider)
    {
        return Convert.ToDouble(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a short.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A short.</returns>
    short IConvertible.ToInt16(IFormatProvider provider)
    {
        return Convert.ToInt16(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to an int.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>An int.</returns>
    int IConvertible.ToInt32(IFormatProvider provider)
    {
        return Convert.ToInt32(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a long.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A long.</returns>
    long IConvertible.ToInt64(IFormatProvider provider)
    {
        return Convert.ToInt64(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a signed byte.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A signed byte.</returns>
    sbyte IConvertible.ToSByte(IFormatProvider provider)
    {
        return Convert.ToSByte(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a float.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A float.</returns>
    float IConvertible.ToSingle(IFormatProvider provider)
    {
        return Convert.ToSingle(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a string.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>A string.</returns>
    string IConvertible.ToString(IFormatProvider provider)
    {
        return Convert.ToString(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to an unsigned short.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>An unsigned short.</returns>
    ushort IConvertible.ToUInt16(IFormatProvider provider)
    {
        return Convert.ToUInt16(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to an unsigned int.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>An unsigned int.</returns>
    uint IConvertible.ToUInt32(IFormatProvider provider)
    {
        return Convert.ToUInt32(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to an unsigned long.
    /// </summary>
    /// <param name="provider">The format provider.</param>
    /// <returns>An unsigned long.</returns>
    ulong IConvertible.ToUInt64(IFormatProvider provider)
    {
        return Convert.ToUInt64(this, provider);
    }

    /// <summary>
    /// Converts the current <see cref="AvroDecimalValue"/> to a string.
    /// </summary>
    /// <param name="format"></param>
    /// <param name="formatProvider">The format provider.</param>
    /// <returns>A string representation of the numeric value.</returns>
    public string ToString(string format, IFormatProvider formatProvider)
    {
        return ToString();
    }

    /// <summary>
    /// Compares the value of the current <see cref="AvroDecimalValue"/> to the value of another object.
    /// </summary>
    /// <param name="obj">The object to compare.</param>
    /// <returns>A value that indicates the relative order of the objects being compared.</returns>
    public int CompareTo(object obj)
    {
        if (obj == null)
            return 1;

        if (!(obj is AvroDecimalValue))
            throw new ArgumentException("Compare to object must be a BigDecimal", nameof(obj));

        return CompareTo((AvroDecimalValue)obj);
    }

    /// <summary>
    /// Compares the value of the current <see cref="AvroDecimalValue"/> to the value of another
    /// <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="other">The <see cref="AvroDecimalValue"/> to compare.</param>
    /// <returns>A value that indicates the relative order of the <see cref="AvroDecimalValue"/>
    /// instances being compared.</returns>
    public int CompareTo(AvroDecimalValue other)
    {
        var unscaledValueCompare = UnscaledValue.CompareTo(other.UnscaledValue);
        var scaleCompare = Scale.CompareTo(other.Scale);

        // if both are the same value, return the value
        if (unscaledValueCompare == scaleCompare)
            return unscaledValueCompare;

        // if the scales are both the same return unscaled value
        if (scaleCompare == 0)
            return unscaledValueCompare;

        var scaledValue = BigInteger.Divide(UnscaledValue, BigInteger.Pow(new BigInteger(10), Scale));
        var otherScaledValue =
            BigInteger.Divide(other.UnscaledValue, BigInteger.Pow(new BigInteger(10), other.Scale));

        return scaledValue.CompareTo(otherScaledValue);
    }

    /// <summary>
    /// Returns a value that indicates whether the current <see cref="AvroDecimalValue"/> has the same
    /// value as another <see cref="AvroDecimalValue"/>.
    /// </summary>
    /// <param name="other">The <see cref="AvroDecimalValue"/> to compare.</param>
    /// <returns>true if the current <see cref="AvroDecimalValue"/> has the same value as <paramref name="other"/>;
    /// otherwise false.</returns>
    public bool Equals(AvroDecimalValue other)
    {
        return Scale == other.Scale && UnscaledValue == other.UnscaledValue;
    }

    private static byte[] GetBytesFromDecimal(decimal d)
    {
        byte[] bytes = new byte[16];

        int[] bits = decimal.GetBits(d);
        int lo = bits[0];
        int mid = bits[1];
        int hi = bits[2];
        int flags = bits[3];

        bytes[0] = (byte)lo;
        bytes[1] = (byte)(lo >> 8);
        bytes[2] = (byte)(lo >> 0x10);
        bytes[3] = (byte)(lo >> 0x18);
        bytes[4] = (byte)mid;
        bytes[5] = (byte)(mid >> 8);
        bytes[6] = (byte)(mid >> 0x10);
        bytes[7] = (byte)(mid >> 0x18);
        bytes[8] = (byte)hi;
        bytes[9] = (byte)(hi >> 8);
        bytes[10] = (byte)(hi >> 0x10);
        bytes[11] = (byte)(hi >> 0x18);
        bytes[12] = (byte)flags;
        bytes[13] = (byte)(flags >> 8);
        bytes[14] = (byte)(flags >> 0x10);
        bytes[15] = (byte)(flags >> 0x18);

        return bytes;
    }
}