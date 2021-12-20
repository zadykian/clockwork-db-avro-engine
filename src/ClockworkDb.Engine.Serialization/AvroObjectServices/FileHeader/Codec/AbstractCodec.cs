namespace ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader.Codec;

internal abstract class AbstractCodec
{
    internal abstract string Name { get; }

    internal abstract byte[] Decompress(byte[] compressedData);

    internal abstract byte[] Compress(byte[] uncompressedData);

    internal static AbstractCodec CreateCodec(CodecType codecType)
    {
        switch (codecType)
        {
            case CodecType.Deflate:
                return new DeflateCodec();
            case CodecType.Snappy:
                return new SnappyCodec();
            case CodecType.GZip:
                return new GZipCodec();
            case CodecType.Brotli:
                return new BrotliCodec();
            default:
                return new NullCodec();
        }
    }

    internal static AbstractCodec CreateCodecFromString(string codecName)
    {
        var parsedSuccessfully = Enum.TryParse<CodecType>(codecName, true, out var codecType);
        return parsedSuccessfully ? CreateCodec(codecType) : new NullCodec();
    }
}