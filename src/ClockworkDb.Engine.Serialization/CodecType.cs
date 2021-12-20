

namespace ClockworkDb.Engine.Serialization;

public enum CodecType
{
    Null = 0,
    Deflate = 1,
    Snappy = 2,
    GZip = 3,
    Brotli = 4
}