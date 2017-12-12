namespace Lykke.Job.LogReader.Core.Settings.JobSettings
{
    public class ReaderSettings
    {
        public string[] ExcludeTables { get; set; }
        public LogStashClient LogStash { get; set; }

        public class LogStashClient
        {
            public string Host { get; set; }
            public int Port { get; set; }
        }
    }
}
