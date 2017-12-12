using Lykke.Job.LogReader.Core.Settings.JobSettings;
using Lykke.Job.LogReader.Core.Settings.SlackNotifications;

namespace Lykke.Job.LogReader.Core.Settings
{
    public class AppSettings
    {
        public LogReaderSettings LogReaderJob { get; set; }
        public SlackNotificationsSettings SlackNotifications { get; set; }
    }
}