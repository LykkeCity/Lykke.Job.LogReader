using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Globalization;
using System.IO;
using System.Linq;
using System.Net.Sockets;
using System.Threading;
using System.Threading.Tasks;
using AzureStorage.Tables;
using Common;
using Common.Log;
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
        private readonly ILog _log;
        private readonly ReaderSettings _settings;
        private readonly IReloadingManager<DbSettings> _dbsettings;
        private readonly List<string> _exclute;
        private readonly SemaphoreSlim _lock = new SemaphoreSlim(1, 1);

        private List<TableInfo> _tables;
        private bool _isConnect = false;
        private TcpClient _client;
        private StreamWriter _writer;

        public AzureLogHandler(ILog log, ReaderSettings settings, IReloadingManager<DbSettings> dbsettings)
            : base(nameof(AzureLogHandler), (int)TimeSpan.FromSeconds(1).TotalMilliseconds, log)
        {
            this._exclute = settings.ExcludeTables.ToList();
            this._exclute.Add("LogReaderLog");
            this._log = log;
            this._settings = settings;
            this._dbsettings = dbsettings;
        }

        public override async Task Execute()
        {
            if (this._tables == null)
            {
                this._tables = new List<TableInfo>();
#pragma warning disable 4014
                this.FindTables();
#pragma warning restore 4014
            }

            await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.Execute), "Begin of iteration").ConfigureAwait(false);

            var count = 0;

            var tableList = this._tables.ToArray();

            var countFromTables = await Task.WhenAll(tableList.Select(this.HandleTableAndWatch).ToArray()).ConfigureAwait(false);
            count = countFromTables.Sum();

            await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.Execute), $"End of iteration, count events: {count}").ConfigureAwait(false);
        }

        private async Task<int> HandleTableAndWatch(TableInfo table)
        {
            int count = 0;
            try
            {
                var sw = new Stopwatch();
                sw.Start();
                var countNew = await this.HandleTable(table).ConfigureAwait(false);
                sw.Stop();
                if (countNew > 600 || sw.ElapsedMilliseconds > 10000)
                {
                    await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.HandleTableAndWatch), $"table {table.Name} ({table.Account}), count: {countNew}, time: {sw.ElapsedMilliseconds} ms").ConfigureAwait(false);
                }

                count += countNew;
            }
            catch (Exception ex)
            {
                await this._log.WriteErrorAsync(nameof(AzureLogHandler), "handle log table", $"{table.Name} ({table.Account})", ex).ConfigureAwait(false);
            }

            return count;
        }

        private void AddAccountsByConnString(string[] connStrings, LoggingType loggingType, Dictionary<string, AccountSettings> accounts)
        {
            foreach (var connString in connStrings)
            {
                CloudStorageAccount account = CloudStorageAccount.Parse(connString);
                accounts[account.Credentials.AccountName] = new AccountSettings
                {
                    Account = account,
                    ConnString = connString,
                    LoggingType = loggingType
                };
            }
        }

        private async Task FindTables()
        {
            await this._dbsettings.Reload().ConfigureAwait(false);

            var accounts = new Dictionary<string, AccountSettings>();
            this.AddAccountsByConnString(this._dbsettings.CurrentValue.ScanLogsConnString, LoggingType.Default, accounts);
            this.AddAccountsByConnString(this._dbsettings.CurrentValue.ScanSensitiveLogsConnString, LoggingType.Sensitive, accounts);

            await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.FindTables), $"Begin find log tables, count accounts: {accounts.Count}").ConfigureAwait(false);

            foreach (var a in accounts)
            {
                var accountName = a.Key;
                var account = a.Value.Account;
                var connString = a.Value.ConnString;
                var loggingType = a.Value.LoggingType;

                try
                {
                    await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.FindTables), accountName, $"Start scan account: {accountName}").ConfigureAwait(false);

                    var tableClient = account.CreateCloudTableClient();
                    var names = (await tableClient.ListTablesSegmentedAsync(null).ConfigureAwait(false)).Select(e => e.Name)
                        .Where(e => !this._exclute.Contains(e)).ToArray();

                    await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.FindTables), accountName, $"Find {names.Length} tables in subscribtion").ConfigureAwait(false);

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
                            var row = (await table.ExecuteQuerySegmentedAsync(operationGet, null).ConfigureAwait(false)).FirstOrDefault();
                            if (row != null && row.DateTime != DateTime.MinValue && row.Level != null && row.Msg != null)
                            {
                                if (this._tables.All(e => string.Compare(e.Name, name, StringComparison.OrdinalIgnoreCase) != 0 || string.Compare(e.Account, accountName, StringComparison.OrdinalIgnoreCase) != 0))
                                {
                                    var info = new TableInfo
                                    {
                                        Entity = AzureTableStorage<LogEntity>.Create(new FakeReloadingManager(connString), name, this._log),
                                        PartitionKey = DateTimeOffset.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture),
                                        LastRowKey = DateTime.UtcNow.ToString("HH:mm:ss.fffffff", CultureInfo.InvariantCulture),
                                        Name = name,
                                        Account = accountName,
                                        ConnString = connString,
                                        LoggingType = loggingType,
                                    };
                                    this._tables.Add(info);
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
                            await this._log.WriteErrorAsync(nameof(AzureLogHandler), nameof(this.FindTables), $"{accountName} - {name}", ex).ConfigureAwait(false);
                        }
                    }

#if DEBUG
                    Console.WriteLine();
#endif

                    await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.FindTables), accountName, $"Add {countAdd} tables to handling").ConfigureAwait(false);
                }
                catch (Exception ex)
                {
                    await this._log.WriteErrorAsync(nameof(AzureLogHandler), nameof(this.FindTables), $"{accountName}", ex).ConfigureAwait(false);
                }
            }

            await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.FindTables), $"Start handling {this._tables.Count} tables").ConfigureAwait(false);
        }

        private async Task<int> HandleTable(TableInfo table)
        {
            var index = 0;

            var nowDate = DateTime.UtcNow.Date;

            var i = await this.CheckEvents(table).ConfigureAwait(false);
            index += i;

            if (nowDate != DateTime.Parse(table.PartitionKey, CultureInfo.InvariantCulture))
            {
                table.PartitionKey = DateTime.UtcNow.ToString("yyyy-MM-dd", CultureInfo.InvariantCulture);
                table.LastRowKey = "00";

                i = await this.CheckEvents(table).ConfigureAwait(false);
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
                        TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.GreaterThan, table.LastRowKey)));

            IEnumerable<LogEntity> data;
            try
            {
                data = await table.Entity.WhereAsync(query).ConfigureAwait(false);
            }
            catch (Exception e)
            {
                await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.CheckEvents), e.ToString()).ConfigureAwait(false);
                throw;
            }

            if (data != null)
            {
                try
                {
                    foreach (var logEntity in data.OrderBy(e => e.Timestamp))
                    {
                        await this.SendData(table, logEntity).ConfigureAwait(false);
                        table.LastRowKey = logEntity.RowKey;
                        index++;
                    }
                }
                catch (Exception ex)
                {
                    await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.CheckEvents), ex.ToString()).ConfigureAwait(false);
                    throw;
                }
            }

            return index;
        }

        private void PreparingContext(LogDto logEntity)
        {
            if (!this._settings.ParseContextAsJson && (string.IsNullOrEmpty(logEntity.Context) || !logEntity.Context.StartsWith('{')))
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

        private async Task SendData(TableInfo table, LogEntity logEntity)
        {
            while (true)
            {
                await this._lock.WaitAsync().ConfigureAwait(false);
                try
                {
                    if (!this._isConnect || this._writer == null)
                    {
                        this._writer?.Dispose();
                        this._client?.Dispose();

                        this._client = new TcpClient(this._settings.LogStash.Host, this._settings.LogStash.Port);
                        this._writer = new StreamWriter(this._client.GetStream());
                        this._isConnect = true;
                    }

                    var dto = new LogDto()
                    {
                        DateTime = logEntity.DateTime,
                        Level = logEntity.Level,
                        Version = logEntity.Version,
                        Component = logEntity.Component,
                        Process = table.LoggingType == LoggingType.Sensitive ? null : logEntity.Process,
                        Context = table.LoggingType == LoggingType.Sensitive ? null : logEntity.Context,
                        Type = logEntity.Type,
                        Stack = logEntity.Stack,
                        Msg = table.LoggingType == LoggingType.Sensitive ? null : logEntity.Msg,
                        Table = table.Name,
                        AccountName = table.Account
                    };
                    this.PreparingContext(dto);

                    var json = dto.ToJson();

                    await this._writer.WriteLineAsync(json).ConfigureAwait(false);
                    return;
                }
                catch (Exception ex)
                {
                    await this._log.WriteInfoAsync(nameof(AzureLogHandler), nameof(this.SendData), $"{this._settings.LogStash.Host}:_settings.LogStash.Port", ex.ToString()).ConfigureAwait(false);
                    await Task.Delay(2000).ConfigureAwait(false);
                    this._isConnect = false;
                }
                finally
                {
                    this._lock.Release();
                }
            }
        }

        internal class AccountSettings
        {
            public CloudStorageAccount Account { get; set; }

            public string ConnString { get; set; }

            public LoggingType LoggingType { get; set; }
        }
    }
}
