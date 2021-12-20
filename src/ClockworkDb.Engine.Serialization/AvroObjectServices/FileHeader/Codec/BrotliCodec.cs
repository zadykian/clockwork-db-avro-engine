

using BrotliSharpLib;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader.Codec;

internal class BrotliCodec : AbstractCodec
{
    internal override string Name { get; } = CodecType.Brotli.ToString().ToLower();
    internal override byte[] Decompress(byte[] compressedData)
    {
        return Brotli.DecompressBuffer(compressedData, 0, compressedData.Length);
    }

    internal override byte[] Compress(byte[] uncompressedData)
    {
        return Brotli.CompressBuffer(uncompressedData, 0, uncompressedData.Length, 4);
    }
}