﻿#region license
/**Copyright (c) 2020 Adrian Strugała
*
* Licensed under the Apache License, Version 2.0 (the "License");
* you may not use this file except in compliance with the License.
* You may obtain a copy of the License at
*
* https://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/
#endregion

using System;
using ClockworkDb.Engine.Serialization.Infrastructure.Exceptions;

namespace ClockworkDb.Engine.Serialization.AvroObjectServices.Write.Resolvers
{
    internal class String
    {
        internal void Resolve(object value, IWriter encoder)
        {
            if (value == null)
            {
                value = string.Empty;
            }

            if (!(value is string))
            {
                try
                {
                    value = value.ToString();
                }
                catch (Exception)
                {
                    throw new AvroTypeMismatchException("[String] required to write against [String] schema but found " + value.GetType());
                }
            }

            encoder.WriteString((string)value);
        }
    }
}