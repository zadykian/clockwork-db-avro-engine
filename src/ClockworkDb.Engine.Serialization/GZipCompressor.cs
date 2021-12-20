using System.IO.Compression;

namespace ClockworkDb.Engine.Serialization;

/// <summary>
/// GZip compressor/decompressor.
/// </summary>
internal static class GZipCompressor
{
    /// <summary>
    /// Decompress sequence of bytes.
    /// </summary>
    public static byte[] Decompress(byte[] compressedData)
    {
        using var compressedStream = new MemoryStream(compressedData);
        using var zipStream = new GZipStream(compressedStream, CompressionMode.Decompress);
        using var resultStream = new MemoryStream();
        zipStream.CopyTo(resultStream);
        return resultStream.ToArray();
    }

    /// <summary>
    /// Compress sequence of bytes.
    /// </summary>
    public static byte[] Compress(byte[] uncompressedData)
    {
        using var compressedStream = new MemoryStream();
        using var zipStream = new GZipStream(compressedStream, CompressionMode.Compress);
        zipStream.Write(uncompressedData, 0, uncompressedData.Length);
        zipStream.Close();
        return compressedStream.ToArray();
    }
}