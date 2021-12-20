

using IronSnappy;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader.Codec;

internal class SnappyCodec : AbstractCodec
{
    internal override string Name { get; } = CodecType.Snappy.ToString().ToLower();

    internal override byte[] Compress(byte[] uncompressedData)
    {
        var compressedData = Snappy.Encode(uncompressedData);
        uint checksumUint = Crc32.Get(compressedData);
        byte[] checksumBytes = BitConverter.GetBytes(checksumUint);

        byte[] result = compressedData.Concat(checksumBytes).ToArray();
        return result;
    }

    internal override byte[] Decompress(byte[] compressedData)
    {
        byte[] dataToDecompress = new byte[compressedData.Length - 4]; // last 4 bytes are CRC
        Array.Copy(compressedData, dataToDecompress, dataToDecompress.Length);

        return Snappy.Decode(dataToDecompress);
    }
}