using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Common;
using Common.Log;
using Lykke.Job.LogReader.Core.Settings.JobSettings;
using Lykke.Logs;
using Lykke.SettingsReader;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class AzureLogHandler : TimerPeriod
    {
        private readonly List<TableInfo> _tables;

        public AzureLogHandler(ILog log, ReaderSettings settings, IReloadingManager<DbSettings> dbsettings) :
            base(nameof(AzureLogHandler), (int)TimeSpan.FromSeconds(10).TotalMilliseconds, log)
        {
            _tables = new List<TableInfo>();
            foreach (var name in settings.LogTables)
            {
                var info = new TableInfo
                {
                    Entity = AzureTableStorage<LogEntity>.Create(dbsettings.ConnectionString(e => e.LogsConnString), name, log),
                    Time = DateTimeOffset.UtcNow
                };
                _tables.Add(info);
            }
        }

        public override async Task Execute()
        {
            foreach (var table in _tables)
            {
                await HandleTable(table);
            }

            await Task.CompletedTask;
        }

        private async Task HandleTable(TableInfo table)
        {
            var query = new TableQuery<LogEntity>()
                .Where(TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, table.Time));

            var data = await table.Entity.WhereAsync(query);

            if (data != null)
            {
                foreach (var logEntity in data.OrderBy(e => e.RowKey))
                {
                    table.Time = logEntity.Timestamp;
                    
                    var dto = new LogDto()
                    {
                        DateTime = logEntity.DateTime,
                        Level = logEntity.Level,
                        Version = logEntity.Version,
                        Component = logEntity.Component,
                        Process = logEntity.Process,
                        Context = logEntity.Context,
                        Type = logEntity.Type,
                        Stack = logEntity.Stack,
                        Msg = logEntity.Msg
                    };
                    


                    Console.WriteLine(dto.ToJson());
                }
            }
        }
    }
}
;
