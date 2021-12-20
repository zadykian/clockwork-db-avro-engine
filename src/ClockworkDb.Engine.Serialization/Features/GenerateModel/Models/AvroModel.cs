namespace ClockworkDb.Engine.Serialization.Features.GenerateModel.Models;

internal class AvroModel
{
    internal List<AvroClass> Classes { get; set; } = new();
    internal List<AvroEnum> Enums { get; set; } = new();
}

internal class AvroEnum
{
    public string EnumName { get; set; }
    public List<string> Symbols { get; set; } = new();
}

internal class AvroClass
{
    public string ClassName { get; set; }
    public string ClassNamespace { get; set; }
    public List<AvroField> Fields { get; set; } = new();
}

internal class AvroField
{
    public string FieldType { get; set; }
    public string Name { get; set; }
    public string Namespace { get; set; }
}