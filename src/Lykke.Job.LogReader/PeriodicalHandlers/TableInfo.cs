using System;
using AzureStorage;
using Lykke.Logs;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class TableInfo
    {
        public INoSQLTableStorage<LogEntity> Entity { get; set; }
        public DateTimeOffset Time { get; set; }
    }
}
