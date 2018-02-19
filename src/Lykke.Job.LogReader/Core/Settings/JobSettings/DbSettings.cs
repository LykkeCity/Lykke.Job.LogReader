namespace Lykke.Job.LogReader.Core.Settings.JobSettings
{
    public class DbSettings
    {
        public string LogsConnString { get; set; }
        public string[] ScanLogsConnString { get; set; }
        public string[] ScanSensitiveLogsConnString { get; set; }
    }
}
