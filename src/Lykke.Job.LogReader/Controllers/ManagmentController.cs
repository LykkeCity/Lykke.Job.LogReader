using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading.Tasks;
using Lykke.Job.LogReader.PeriodicalHandlers;
using Microsoft.AspNetCore.Mvc;
using Microsoft.CodeAnalysis.CSharp.Syntax;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Lykke.Job.LogReader.Controllers
{
    [Route("api/[controller]")]
    public class ManagmentController : Controller
    {
        private readonly AzureLogHandler _handler;

        public ManagmentController(AzureLogHandler handler)
        {
            _handler = handler;
        }

        [HttpGet]
        [SwaggerOperation("GetInfo")]
        public List<AccountInfoReport> GetInfo()
        {
            var data = _handler.GetTableInfo();

            var responce = data.GroupBy(e => e.Account).Select(g => new AccountInfoReport()
            {
                AccountName = g.Key,
                Tables = g.Select(e => new TableInfoReport(e.Name, e.PartitionKey, e.LastRowKey)).OrderBy(e => e.TableName).ToList()
            }).OrderBy(e => e.AccountName).ToList();

            return responce;
        }

        [HttpPost]
        [SwaggerOperation("LoadData")]
        public async Task<string> LoadData(string account, string table, DateTime fromTime, DateTime toTime)
        {
            if (fromTime.Date != toTime.Date)
                return "Plase use time range in ONE DAY";

            var partitionKey = fromTime.Date.ToString("yyyy-MM-dd");

            var result = await _handler.LoadData(account, table, partitionKey, fromTime, toTime);

            return result;
        }

    }

    public class TableInfoReport
    {
        public TableInfoReport(string tableName, string partitionKey, string lastRowKey)
        {
            TableName = tableName;
            PartitionKey = partitionKey;
            LastRowKey = lastRowKey;
        }

        public string TableName { get; set; }
        public string PartitionKey { get; set; }
        public string LastRowKey { get; set; }
    }

    public class AccountInfoReport
    {
        public string AccountName { get; set; }
        public List<TableInfoReport> Tables { get; set; }
    }

}
