using Lykke.SettingsReader.Attributes;

namespace Lykke.Job.LogReader.Core.Settings.JobSettings
{
    public class ReaderSettings
    {
        [Optional]
        public string[] ExcludeTables { get; set; }
        public bool ParseContextAsJson { get; set; }

        public string LogAggregatorHost { get; set; }
    }
}
