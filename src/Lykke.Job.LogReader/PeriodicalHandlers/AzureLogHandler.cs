﻿using System;
using System.Collections.Generic;
using System.Diagnostics;
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
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class AzureLogHandler : TimerPeriod
    {
        private List<TableInfo> _tables;
        private readonly ILog _log;
        private readonly ReaderSettings _settings;
        private readonly IReloadingManager<DbSettings> _dbsettings;
        private readonly List<string> _exclute;

        public AzureLogHandler(ILog log, ReaderSettings settings, IReloadingManager<DbSettings> dbsettings) :
            base(nameof(AzureLogHandler), (int)TimeSpan.FromSeconds(10).TotalMilliseconds, log)
        {
            _exclute = settings.ExcludeTables.ToList();
            _exclute.Add("LogReaderLog");
            _log = log;
            _settings = settings;
            _dbsettings = dbsettings;
        }

        public override async Task Execute()
        {
            if (_tables == null)
            {
                _tables = new List<TableInfo>();
#pragma warning disable 4014
                FindTables();
#pragma warning restore 4014
            }

            Console.WriteLine($"{DateTime.UtcNow:s} Begin of iteration");

            var count = 0;

            var tableList = _tables.ToArray();

            var countFromTables = await Task.WhenAll(tableList.Select(HandleTableAndWatch).ToArray());
            count = countFromTables.Sum();

            Console.WriteLine($"{DateTime.UtcNow:s} End of iteration, count events: {count}");
        }

        private async Task<int> HandleTableAndWatch(TableInfo table)
        {
            int count = 0;
            try
            {
                var sw = new Stopwatch();
                sw.Start();
                var countNew = await HandleTable(table);
                sw.Stop();
                if (countNew > 300 || sw.ElapsedMilliseconds > 5000)
                {
                    Console.WriteLine($"table {table.Name} ({table.Account}), count: {countNew}, time: {sw.ElapsedMilliseconds} ms");
                }
                count += countNew;
            }
            catch (Exception ex)
            {
                await _log.WriteErrorAsync(nameof(AzureLogHandler), "handle log table", $"{table.Name} ({table.Account})", ex);
            }
            return count;
        }

        private async Task FindTables()
        {
            await _dbsettings.Reload();

            await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(FindTables), $"Begin find log tables, count accounts: {_dbsettings.CurrentValue.ScanLogsConnString.Length}");

            foreach (var connString in _dbsettings.CurrentValue.ScanLogsConnString)
            {
                CloudStorageAccount account = CloudStorageAccount.Parse(connString);
                var accountName = account.Credentials.AccountName;

                try
                {
                    await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(FindTables), accountName, $"Start scan account: {accountName}");

                    var tableClient = account.CreateCloudTableClient();
                    var names = (await tableClient.ListTablesSegmentedAsync(null)).Select(e => e.Name)
                        .Where(e => !_exclute.Contains(e)).ToArray();

                    await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(FindTables), accountName, $"Find {names.Length} tables in subscribtion");

                    var countAdd = 0;

                    var countHandling = 0;
                    foreach (var name in names)
                    {
                        try
                        {
                            CloudTable table = tableClient.GetTableReference(name);
                            var operationGet = new TableQuery<LogEntity>().Take(1);
                            var row = (await table.ExecuteQuerySegmentedAsync(operationGet, null)).FirstOrDefault();
                            if (row != null && row.DateTime != DateTime.MinValue && row.Level != null && row.Msg != null)
                            {
                                if (_tables.All(e => e.Name != name || e.ConnString != connString))
                                {
                                    var info = new TableInfo
                                    {
                                        Entity = AzureTableStorage<LogEntity>.Create(new FakeReloadingManager(connString), name, _log),
                                        Time = DateTimeOffset.UtcNow,
                                        Name = name,
                                        Account = accountName,
                                        ConnString = connString
                                    };
                                    _tables.Add(info);
                                    countAdd++;
                                }
                            }
#if DEBUG
                            countHandling++;
#endif

                            Console.Write($"\rhandling: {countHandling} / {names.Length}                   ");
                        }
                        catch (Exception ex)
                        {
#if DEBUG
                            Console.WriteLine();
#endif
                            await _log.WriteErrorAsync(nameof(AzureLogHandler), nameof(FindTables), $"{accountName} - {name}", ex);
                        }
                    }

#if DEBUG
                    Console.WriteLine();
#endif

                    await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(FindTables), accountName, $"Add {countAdd} tables to handling");
                }
                catch (Exception ex)
                {
                    await _log.WriteErrorAsync(nameof(AzureLogHandler), nameof(FindTables), $"{accountName}", ex);
                }
            }
            
            await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(FindTables), $"Start handling {_tables.Count} tables");
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

        private async Task<int> CheckEvents(TableInfo table, string pk, DateTimeOffset lastTime)
        {
            var index = 0;

            var query = new TableQuery<LogEntity>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, pk),
                        TableOperators.And,
                        TableQuery.GenerateFilterConditionForDate("Timestamp", QueryComparisons.GreaterThan, lastTime)
                    ));

            IEnumerable<LogEntity> data;
            try
            {
                data = await table.Entity.WhereAsync(query);
            }
            catch (Exception e)
            {
                Console.WriteLine(e);
                throw;
            }
            

            if (data != null)
            {
                try
                {
                    using (TcpClient client = new TcpClient(_settings.LogStash.Host, _settings.LogStash.Port))
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
                                Table = table.Name,
                                Account = table.Account
                            };

                            var json = dto.ToJson();

                            await writer.WriteLineAsync(json);

                            index++;
                        }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine(ex);
                    throw;
                }
            }

            return index;
        }
    }

    public class FakeReloadingManager : IReloadingManager<string>
    {
        private readonly string _value;

        public FakeReloadingManager(string value)
        {
            _value = value;
        }

        public Task<string> Reload() => Task.FromResult(_value);
        public bool HasLoaded => true;
        public string CurrentValue => _value;
    }
}
;
