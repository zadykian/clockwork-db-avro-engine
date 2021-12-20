﻿using System;

namespace ClockworkDb.Engine.Serialization.Infrastructure.Extensions
{
    internal static class DateTimeExtensions
    {
        internal static DateTime UnixEpochDateTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
    }
}