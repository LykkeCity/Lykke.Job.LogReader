using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Common;
using Common.Log;
using Lykke.Common.Log;
using Lykke.Job.LogReader.Core.Settings.JobSettings;
using Lykke.Logs;
using Lykke.SettingsReader;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class AzureLogHandler : TimerPeriod
    {
        private readonly ILogFactory _logFactory;
        private readonly ILog _log;
        private readonly ReaderSettings _settings;
        private readonly IReloadingManager<DbSettings> _dbsettings;
        private readonly List<string> _exclude;

        private List<TableInfo> _tables;
        private bool _isConnect = false;
        private TcpClient _client;
        private StreamWriter _writer;

        private static bool _inProgress = false;
        private static readonly SemaphoreSlim _tableReadsSemaphore = new SemaphoreSlim(8);

        public AzureLogHandler(ILogFactory logFactory, ReaderSettings settings, IReloadingManager<DbSettings> dbsettings) :
            base(TimeSpan.FromMinutes(1), logFactory, nameof(AzureLogHandler))
        {
            _logFactory = logFactory;
            _exclude = settings.ExcludeTables?.ToList() ?? new List<string>(1);
            _exclude.Add("LogReaderLog");
            _log = logFactory.CreateLog(this);
            _settings = settings;
            _dbsettings = dbsettings;
        }

        public IReadOnlyList<TableInfo> GetTableInfo()
        {
            return _tables.ToList();
        }

        public override async Task Execute()
        {
            if (_inProgress)
                return;

            try
            {
                _inProgress = true;
                if (_tables == null)
                {
                    _tables = new List<TableInfo>();
                    await FindTables();
                }

                _log.Info("Begin of iteration");

                var tableList = _tables.ToArray();

                var countFromTables = await Task.WhenAll(tableList.Select(HandleTableAndWatch).ToArray());
                var count = countFromTables.Sum();

                _log.Info($"End of iteration, count events: {count}");
            }
            finally
            {
                _inProgress = false;
            }
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
                    _log.Info($"table {table.Name} ({table.Account}), count: {countNew}, time: {sw.ElapsedMilliseconds} ms");
                }
                count += countNew;
            }
            catch (Exception ex)
            {
                _log.Error(ex, context: $"{table.Name} ({table.Account})");
            }
            return count;
        }

        private async Task FindTables()
        {
            await _dbsettings.Reload();

            _log.Info($"Begin find log tables, count accounts: {_dbsettings.CurrentValue.ScanLogsConnString.Length}");

            var connStrings = _dbsettings.CurrentValue.ScanLogsConnString;
            foreach (var connString in connStrings)
            {
                var account = CloudStorageAccount.Parse(connString);
                var accountName = account.Credentials.AccountName;

                try
                {
                    _log.Info($"Start scan account: {accountName}", context: accountName);

                    var tableClient = account.CreateCloudTableClient();
                    var tables = await tableClient.ListTablesSegmentedAsync(null);
                    var names = tables
                        .Select(e => e.Name)
                        .Where(x => (tables.Results.Count == 1 || x.ToLower().Contains("log")) && !_exclude.Contains(x))
                        .ToArray();

                    _log.Info($"Find {names.Length} tables in subscribtion", context: accountName);

                    var countAdd = 0;
#if DEBUG
                    var countHandling = 0;
#endif
                    foreach (var name in names)
                    {
                        try
                        {
                            var table = tableClient.GetTableReference(name);
                            var operationGet = new TableQuery<LogEntity>().Take(1);
                            var row = (await table.ExecuteQuerySegmentedAsync(operationGet, null)).FirstOrDefault();
                            if (row != null && row.DateTime != DateTime.MinValue && row.Level != null
                                && (row.Msg != null || row.Stack != null))
                            {
                                if (_tables.All(e => e.Name != name || e.ConnString != connString))
                                {
                                    var info = new TableInfo
                                    {
                                        Entity = AzureTableStorage<LogEntity>.Create(new FakeReloadingManager(connString), name, _logFactory),
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
                            _log.Error(ex, context: $"{accountName} - {name}");
                        }
                    }

#if DEBUG
                    Console.WriteLine();
#endif

                    _log.Info($"Add {countAdd} tables to handling", context: accountName);
                }
                catch (Exception ex)
                {
                    _log.Error(ex, context: $"{accountName}");
                }
            }

            _log.Info($"Start handling {_tables.Count} tables");
        }

        private async Task<int> HandleTable(TableInfo table)
        {
            var index = 0;

            var nowDate = DateTime.UtcNow.Date;

            await _tableReadsSemaphore.WaitAsync();
            try
            {
                var i = await CheckEvents(table);
                index += i;

                if (nowDate != DateTime.Parse(table.PartitionKey))
                {
                    table.PartitionKey = DateTime.UtcNow.ToString("yyyy-MM-dd");
                    table.LastRowKey = "00";

                    i = await CheckEvents(table);
                    index += i;
                }
            }
            finally
            {
                _tableReadsSemaphore.Release();
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
                _log.Info(e.ToString());
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
                    _log.Info(ex.ToString());
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
        private DateTime _lastConnectTime = DateTime.UtcNow;

        private async Task SendData(TableInfo table, LogEntity logEntity, bool stopIferror = false)
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
                        _lastConnectTime = DateTime.UtcNow;
                        _log.Info("New connection established");
                    }

                    await SendDataToSocket(_writer, table, logEntity);


                    if ((DateTime.UtcNow - _lastConnectTime).Minutes >= 10)
                    {
                        _log.Info("Force close connection (10 min)");
                        _isConnect = false;
                    }

                    return;
                }
                catch (Exception ex)
                {
                    _log.Info(ex.ToString(), context: $"{_settings.LogStash.Host}:_settings.LogStash.Port");
                    _isConnect = false;
                    if (stopIferror)
                        return;
                }
                finally
                {
                    _lock.Release();
                }

                if (!_isConnect)
                    await Task.Delay(2000);
            }
        }

        private async Task SendDataToSocket(StreamWriter writer, TableInfo table, LogEntity logEntity)
        {
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

            await writer.WriteLineAsync(json);
        }

        public async Task<string> LoadData(string account, string table, string partitionKey, DateTime fromTime, DateTime toTime)
        {
            var item = _tables.FirstOrDefault(e => e.Account == account && e.Name == table);
            if (item == null)
                return "not found table";

            var count = 0;

            using (var client = new TcpClient(_settings.LogStash.Host, _settings.LogStash.Port))
            using (var writer = new StreamWriter(client.GetStream()))
            {
                var time = fromTime;
                while (time <= toTime)
                {
                    var totome = time.AddSeconds(10);
                    var filter = TableQuery.CombineFilters(
                        TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey),
                        TableOperators.And,
                        TableQuery.CombineFilters(
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan,
                                time.ToString("HH:mm:ss.fffffff")),
                            TableOperators.And,
                            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.LessThanOrEqual,
                                totome.ToString("HH:mm:ss.fffffff"))
                        )
                    );
                    var query = new TableQuery<LogEntity>().Where(filter);

                    IEnumerable<LogEntity> data;
                    try
                    {
                        data = (await item.Entity.WhereAsync(query)).ToList();
                    }
                    catch (Exception e)
                    {
                        _log.Info(e.ToString());
                        return $"count: {count}, error on get: {e}";
                    }

                    if (data.Any())
                    {
                        try
                        {
                            _log.Info("Try send ====", context: data.Count().ToString());
                            Console.WriteLine($"Try send {data.Count()}");
                            foreach (var logEntity in data.OrderBy(e => e.DateTime))
                            {
                                await SendDataToSocket(writer, item, logEntity);
                                count++;
                            }
                        }
                        catch (Exception ex)
                        {
                            _log.Info(ex.ToString());
                            return $"count: {count}, erroron send: {ex}";
                        }
                    }

                    time = totome;
                    _log.Info($"send {count}, time {totome:HH:mm:ss.fffffff} =====", context: data.Count().ToString());
                }

                return $"count: {count}";
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

        public bool WasReloadedFrom(DateTime dateTime)
        {
            throw new NotImplementedException();
        }

        public bool HasLoaded => true;
        public string CurrentValue => _value;
    }
}
;
