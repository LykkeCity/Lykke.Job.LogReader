using System;

namespace Lykke.Job.LogReader.PeriodicalHandlers
{
    public class LogDto
    {
        public DateTime DateTime { get; set; }
        public string Level { get; set; }
        public string Version { get; set; }
        public string Component { get; set; }
        public string Process { get; set; }
        public string Context { get; set; }
        public string Type { get; set; }
        public string Stack { get; set; }
        public string Msg { get; set; }
        public string Table { get; set; }
        public string AccountName { get; set; }
    }
}
