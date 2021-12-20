// Copyright (c) Microsoft Corporation
// All rights reserved.
// 
// Licensed under the Apache License, Version 2.0 (the "License"); you may not
// use this file except in compliance with the License.  You may obtain a copy
// of the License at http://www.apache.org/licenses/LICENSE-2.0
// 
// THIS CODE IS PROVIDED *AS IS* BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, EITHER EXPRESS OR IMPLIED, INCLUDING WITHOUT LIMITATION ANY IMPLIED
// WARRANTIES OR CONDITIONS OF TITLE, FITNESS FOR A PARTICULAR PURPOSE,
// MERCHANTABLITY OR NON-INFRINGEMENT.
// 
// See the Apache Version 2.0 License for specific language governing
// permissions and limitations under the License.

/** Modifications copyright(C) 2020 Adrian Struga�a **/

using System.Collections.ObjectModel;
using System.Globalization;
using ClockworkDb.Engine.Serialization.AvroObjectServices.BuildSchema;
using ClockworkDb.Engine.Serialization.AvroObjectServices.Schema.Abstract;
using ClockworkDb.Engine.Serialization.Infrastructure.Extensions;
using Newtonsoft.Json;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Schema;

/// <summary>
///     Schema representing an enumeration.
///     For more details please see <a href="http://avro.apache.org/docs/current/spec.html#Enums"> the specification</a>.
/// </summary>
internal sealed class EnumSchema : NamedSchema
{
    private readonly List<string> symbols;
    private readonly List<long> avroToCSharpValueMapping;
    private readonly Dictionary<string, int> symbolToValue;
    private readonly Dictionary<int, string> valueToSymbol;

    /// <summary>
    /// Initializes a new instance of the <see cref="EnumSchema"/> class.
    /// </summary>
    /// <param name="namedEntityAttributes">The named entity attributes.</param>
    /// <param name="runtimeType">Type of the runtime.</param>
    internal EnumSchema(NamedEntityAttributes namedEntityAttributes, Type runtimeType)
        : this(namedEntityAttributes, runtimeType, new Dictionary<string, string>())
    {
    }

    /// <summary>
    /// Initializes a new instance of the <see cref="EnumSchema" /> class.
    /// </summary>
    /// <param name="namedEntityAttributes">The named schema attributes.</param>
    /// <param name="runtimeType">Type of the runtime.</param>
    /// <param name="attributes">The attributes.</param>
    internal EnumSchema(
        NamedEntityAttributes namedEntityAttributes,
        Type runtimeType,
        Dictionary<string, string> attributes)
        : base(namedEntityAttributes, runtimeType, attributes)
    {
        if (runtimeType == null)
        {
            throw new ArgumentNullException(nameof(runtimeType));
        }

        symbols = new List<string>();
        symbolToValue = new Dictionary<string, int>();
        valueToSymbol = new Dictionary<int, string>();
        avroToCSharpValueMapping = new List<long>();

        if (runtimeType.IsEnum())
        {
            symbols = Enum.GetNames(runtimeType).ToList();
            Array values = Enum.GetValues(runtimeType);
            for (int i = 0; i < symbols.Count; i++)
            {
                int v = Convert.ToInt32(values.GetValue(i), CultureInfo.InvariantCulture);
                avroToCSharpValueMapping.Add(Convert.ToInt64(values.GetValue(i), CultureInfo.InvariantCulture));
                symbolToValue.Add(symbols[i], v);
                valueToSymbol.Add(v, symbols[i]);
            }
        }
    }
        
    internal ReadOnlyCollection<string> Symbols => new(symbols);

    internal int GetValueBySymbol(string symbol)
    {
        return symbolToValue[symbol];
    }

    internal string GetSymbolByValue(int value)
    {
        return valueToSymbol[value];
    }

    internal long[] AvroToCSharpValueMapping => avroToCSharpValueMapping.ToArray();

    internal override void ToJsonSafe(JsonTextWriter writer, HashSet<NamedSchema> seenSchemas)
    {
        if (seenSchemas.Contains(this))
        {
            writer.WriteValue(FullName);
            return;
        }

        seenSchemas.Add(this);
        writer.WriteStartObject();
        writer.WriteProperty("type", "enum");
        writer.WriteProperty("name", Name);
        writer.WriteOptionalProperty("namespace", Namespace);
        writer.WriteOptionalProperty("doc", Doc);
        writer.WritePropertyName("symbols");
        writer.WriteStartArray();
        symbols.ForEach(writer.WriteValue);
        writer.WriteEndArray();
        writer.WriteEndObject();
    }

    internal void AddSymbol(string symbol)
    {
        if (string.IsNullOrEmpty(symbol))
        {
            throw new ArgumentException(string.Format(CultureInfo.InvariantCulture, "Symbol should not be null."));
        }

        symbols.Add(symbol);
        symbolToValue.Add(symbol, symbolToValue.Count);
        valueToSymbol.Add(valueToSymbol.Count, symbol);

        if (avroToCSharpValueMapping.Any())
        {
            avroToCSharpValueMapping.Add(avroToCSharpValueMapping.Last() + 1);
        }
        else
        {
            avroToCSharpValueMapping.Add(0);
        }
    }

    internal override AvroType Type { get; } = AvroType.Enum;
}