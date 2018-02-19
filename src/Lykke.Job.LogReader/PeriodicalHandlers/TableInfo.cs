using System;
using AzureStorage;
using Lykke.Logs;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class TableInfo
    {
        public INoSQLTableStorage<LogEntity> Entity { get; set; }
        public string PartitionKey { get; set; }
        public string LastRowKey { get; set; }
        public string Name { get; set; }
        public string Account { get; set; }
        public string ConnString { get; set; }
        public LoggingType LoggingType { get; set; }
    }

    public enum LoggingType
    {
        Default = 0,
        Sensitive = 1
    }
}
