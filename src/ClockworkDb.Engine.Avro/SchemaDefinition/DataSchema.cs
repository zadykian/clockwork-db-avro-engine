using System.Text.Json;
using ClockworkDb.Engine.Avro.SchemaObjects;

namespace ClockworkDb.Engine.Avro.SchemaDefinition;

/// <inheritdoc />
internal sealed class DataSchema : IDataSchema
{
	/// <summary>
	/// Path to engine's working directory.
	/// </summary>
	private static string WorkingDirectoryPath => Path.Combine(Environment.CurrentDirectory, "clockwork-db");

	private static string PathToSchemaFile => Path.Combine(WorkingDirectoryPath, "meta", "schema.json");
	
	/// <inheritdoc />
	async ValueTask IDataSchema.InitializeAsync()
	{
		if (File.Exists(PathToSchemaFile))
		{
			return;
		}

		await File.WriteAllTextAsync(PathToSchemaFile, "{}");
	}

	/// <inheritdoc />
	async ValueTask IDataSchema.AddTypeAsync(SchemaTypeInfo typeInfo)
	{
		var allTypes = await LoadAllAsync()
			.ToDictionaryAsync(info => info.TypeName, info => info);

		if (allTypes.TryGetValue(typeInfo.TypeName, out var existingTypeInfo))
		{
			throw Error.TypeAlreadyDeclared(existingTypeInfo);
		}

		var updated = allTypes
			.Select(pair => pair.Value)
			.Append(typeInfo)
			.ToArray();

		var serialized = JsonSerializer.Serialize(updated);
		await File.WriteAllTextAsync(PathToSchemaFile, serialized);
	}

	/// <inheritdoc />
	async ValueTask IDataSchema.UpdateTypeAsync(SchemaTypeInfo typeInfo)
	{
		var allTypes = await LoadAllAsync()
			.ToDictionaryAsync(info => info.TypeName, info => info);

		if (!allTypes.TryGetValue(typeInfo.TypeName, out var existingTypeInfo))
		{
			throw new ArgumentException("Type doesn't declared in schema.", nameof(typeInfo));
		}

		if (typeInfo.Equals(existingTypeInfo))
		{
			return;
		}

		var declaredTypeInfos = allTypes
			.Select(pair => pair.Value)
			.ToList();

		declaredTypeInfos.Remove(existingTypeInfo);
		declaredTypeInfos.Add(typeInfo);

		var serialized = JsonSerializer.Serialize(declaredTypeInfos);
		await File.WriteAllTextAsync(PathToSchemaFile, serialized);
	}

	/// <inheritdoc />
	public async IAsyncEnumerable<SchemaTypeInfo> LoadAllAsync()
	{
		var serialized = await File.ReadAllTextAsync(PathToSchemaFile);
		var typeInfos = JsonSerializer.Deserialize<IEnumerable<SchemaTypeInfo>>(serialized)!;
		foreach (var typeInfo in typeInfos) yield return typeInfo;
	}

	/// <inheritdoc />
	async ValueTask<SchemaTypeInfo> IDataSchema.LoadAsync(string typeName)
	{
		var allTypes = await LoadAllAsync()
			.ToDictionaryAsync(info => info.TypeName, info => info);

		if (!allTypes.TryGetValue(typeName, out var schemaTypeInfo))
		{
			throw new ArgumentException("Type doesn't declared in schema.", nameof(typeName));
		}

		return schemaTypeInfo;
	}
}