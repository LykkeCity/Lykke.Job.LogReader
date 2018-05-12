using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using System.Transactions;
using AzureStorage.Tables;
using Common;
using Common.Log;
using Lykke.Job.LogReader.Core.Settings.JobSettings;
using Lykke.Logs;
using Lykke.SettingsReader;
using Microsoft.CodeAnalysis.CSharp;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class AzureLogHandler : TimerPeriod
    {
        private List<TableInfo> _tables;
        private readonly ILog _log;
        private readonly ReaderSettings _settings;
        private readonly IReloadingManager<DbSettings> _dbsettings;
        private readonly List<string> _exclute;

        private bool _isConnect = false;
        private TcpClient _client;
        private StreamWriter _writer;

        public AzureLogHandler(ILog log, ReaderSettings settings, IReloadingManager<DbSettings> dbsettings) :
            base(nameof(AzureLogHandler), (int)TimeSpan.FromSeconds(1).TotalMilliseconds, log)
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

            await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(Execute), "Begin of iteration");

            var count = 0;

            var tableList = _tables.ToArray();

            var countFromTables = await Task.WhenAll(tableList.Select(HandleTableAndWatch).ToArray());
            count = countFromTables.Sum();

            await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(Execute), $"End of iteration, count events: {count}");
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
                if (countNew > 600 || sw.ElapsedMilliseconds > 10000)
                {
                    await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(HandleTableAndWatch), $"table {table.Name} ({table.Account}), count: {countNew}, time: {sw.ElapsedMilliseconds} ms");
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
#if DEBUG
                    var countHandling = 0;
#endif
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
                                        PartitionKey = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd"),
                                        LastRowKey = DateTime.UtcNow.ToString("HH:mm:ss.fffffff"),
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
                            Console.Write($"\rhandling: {countHandling} / {names.Length}                   ");
#endif


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
            var index = 0;

            var nowDate = DateTime.UtcNow.Date;

            var i = await CheckEvents(table);
            index += i;

            if (nowDate != DateTime.Parse(table.PartitionKey))
            {
                table.PartitionKey = DateTime.UtcNow.ToString("yyyy-MM-dd");
                table.LastRowKey = "00";

                i = await CheckEvents(table);
                index += i;
            }

            return index;
        }

        private async Task<int> CheckEvents(TableInfo table)
        {
            var index = 0;

            var query = new TableQuery<LogEntity>()
                .Where(
                    TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, table.PartitionKey),
                        TableOperators.And,
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, table.LastRowKey)
                    ));

            IEnumerable<LogEntity> data;
            try
            {
                data = await table.Entity.WhereAsync(query);
            }
            catch (Exception e)
            {
                await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(CheckEvents), e.ToString());
                throw;
            }

            if (data != null)
            {
                try
                {
                    foreach (var logEntity in data.OrderBy(e => e.Timestamp))
                    {
                        await SendData(table, logEntity);
                        table.LastRowKey = logEntity.RowKey;
                        index++;
                    }
                }
                catch (Exception ex)
                {
                    await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(CheckEvents), ex.ToString());
                    throw;
                }
            }

            return index;
        }

        private void PreparingContext(LogDto logEntity)
        {
            if (!_settings.ParseContextAsJson && (string.IsNullOrEmpty(logEntity.Context) || !logEntity.Context.StartsWith('{')))
            {
                return;
            }

            try
            {
                var ctx = JObject.Parse(logEntity.Context);
                logEntity.ContextData = ctx;
            }
            // ReSharper disable once EmptyGeneralCatchClause
            catch (Exception)
            {
            }
        }

        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private async Task SendData(TableInfo table, LogEntity logEntity)
        {
            while (true)
            {
                await _lock.WaitAsync();
                try
                {
                    if (!_isConnect || _writer == null)
                    {
                        _writer?.Dispose();
                        _client?.Dispose();

                        _client = new TcpClient(_settings.LogStash.Host, _settings.LogStash.Port);
                        _writer = new StreamWriter(_client.GetStream());
                        _isConnect = true;
                    }

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
                        AccountName = table.Account,
                        Env = logEntity.Env
                    };
                    PreparingContext(dto);

                    var json = dto.ToJson();

                    await _writer.WriteLineAsync(json);
                    return;
                }
                catch (Exception ex)
                {
                    await _log.WriteInfoAsync(nameof(AzureLogHandler), nameof(SendData), $"{_settings.LogStash.Host}:_settings.LogStash.Port", ex.ToString());
                    await Task.Delay(2000);
                    _isConnect = false;
                }
                finally
                {
                    _lock.Release();
                }
            }
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
