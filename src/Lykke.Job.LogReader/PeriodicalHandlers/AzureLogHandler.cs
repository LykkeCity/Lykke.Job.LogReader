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
using Microsoft.AspNetCore.Mvc.ViewFeatures.Internal;
using Microsoft.WindowsAzure.Storage.File;
using Microsoft.WindowsAzure.Storage.Table;
using NetStash.Log;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class AzureLogHandler : TimerPeriod
    {
        private readonly List<TableInfo> _tables;
        private readonly ILog _log;

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
            _log = log;
        }

        public override async Task Execute()
        {
            var count = 0;

            foreach (var table in _tables)
            {
                try
                {
                    count += await HandleTable(table);
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync(nameof(AzureLogHandler), nameof(Execute), table.Name, ex);
                }
            }

            Console.WriteLine($"{DateTime.UtcNow:s} End of iteration, count events: {count}");
        }

        private async Task<int> HandleTable(TableInfo table)
        {
            var lastTime = table.Time;
            var pk = table.Time.ToString("yyyy-MM-dd");
            var index = 0;

            index += await CheckEvents(table, pk, lastTime);

            if (DateTime.Now.Date != lastTime.Date)
            {
                table.Time = new DateTimeOffset(lastTime.Date.AddDays(1));
                lastTime = table.Time;
                pk = table.Time.ToString("yyyy-MM-dd");

                index += await CheckEvents(table, pk, lastTime);
            }

            return index;
        }

        private static async Task<int> CheckEvents(TableInfo table, string pk, DateTimeOffset lastTime)
        {
            var index = 0;

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

                        index++;
                    }
                }
            }

            return index;
        }
    }
}
;
