using ClockworkDb.Engine.Serialization.AvroObjectServices.FileHeader;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write;

/// <summary>
/// Write leaf values.
/// </summary>
internal partial class Writer
{
    internal void WriteHeader(Header header)
    {
        WriteFixed(DataFileConstants.AvroHeader);

        // Write metadata 
        int size = header.GetMetadataSize();
        WriteInt(size);

        foreach (KeyValuePair<string, byte[]> metaPair in header.GetRawMetadata())
        {
            WriteString(metaPair.Key);
            WriteBytes(metaPair.Value);
        }
        WriteMapEnd();


        // Write sync data
        WriteFixed(header.SyncData);
    }

    internal void WriteDataBlock(byte[] data, byte[] syncData, int blockCount)
    {
        // write count 
        WriteLong(blockCount);

        // write data 
        WriteBytes(data);

        // write sync marker 
        WriteFixed(syncData);
    }
}