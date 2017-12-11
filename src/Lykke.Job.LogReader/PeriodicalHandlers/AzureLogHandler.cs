using System;
using System.Collections.Generic;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Common;
using Common.Log;
using Lykke.Job.LogReader.Core.Settings.JobSettings;
using Lykke.Logs;
using Lykke.SettingsReader;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Table;
using NetStash.Log;

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
                    Time = DateTimeOffset.UtcNow,
                    Name = name
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
            var lastTime = table.Time;
            var pk = table.Time.ToString("yyyy-MM-dd");

            await CheckEvents(table, pk, lastTime);

            if (DateTime.Now.Date != lastTime.Date)
            {
                table.Time = new DateTimeOffset(lastTime.Date.AddDays(1));
                lastTime = table.Time;
                pk = table.Time.ToString("yyyy-MM-dd");
                await CheckEvents(table, pk, lastTime);
            }
        }

        private static async Task CheckEvents(TableInfo table, string pk, DateTimeOffset lastTime)
        {
            var query = new TableQuery<LogEntity>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk),
                        TableOperators.And,
                        TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, lastTime)
                    ));

            var data = await table.Entity.WhereAsync(query);

            if (data != null)
            {
                using (TcpClient client = new TcpClient("logstash.lykke-elk-dev.svc.cluster.local", 5043))
                using (StreamWriter writer = new StreamWriter(client.GetStream()))
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
                            Msg = logEntity.Msg,
                            Table = table.Name
                        };

                        var json = dto.ToJson();

                        await writer.WriteLineAsync(json);
                        //Console.WriteLine("{0}: {1}", dto.DateTime, dto.Msg);
                    }
                }
            }
        }
    }
}
;
