

using System.IO.Compression;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader.Codec;

internal class GZipCodec : AbstractCodec
{
    internal override string Name { get; } = "gzip";
    internal override byte[] Decompress(byte[] compressedData)
    {
        using (var compressedStream = new MemoryStream(compressedData))
        using (var zipStream = new GZipStream(compressedStream, CompressionMode.Decompress))
        using (var resultStream = new MemoryStream())
        {
            zipStream.CopyTo(resultStream);
            return resultStream.ToArray();
        }
    }

    internal override byte[] Compress(byte[] uncompressedData)
    {
        using (var compressedStream = new MemoryStream())
        using (var zipStream = new GZipStream(compressedStream, CompressionMode.Compress))
        {
            zipStream.Write(uncompressedData, 0, uncompressedData.Length);
            zipStream.Close();
            return compressedStream.ToArray();
        }
    }
}