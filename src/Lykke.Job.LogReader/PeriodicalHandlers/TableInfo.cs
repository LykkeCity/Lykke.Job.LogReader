using System;
using AzureStorage;
using Lykke.Logs;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class TableInfo
    {
        public INoSQLTableStorage<LogEntity> Entity { get; set; }
        public DateTimeOffset Time { get; set; }
        public string LastRowKey { get; set; }
        public string Name { get; set; }
        public string Account { get; set; }
        public string ConnString { get; set; }
    }
}
