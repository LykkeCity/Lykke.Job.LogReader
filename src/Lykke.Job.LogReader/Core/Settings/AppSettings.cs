using Lykke.Job.LogReader.Core.Settings.JobSettings;
using JetBrains.Annotations;
using Lykke.Sdk.Settings;

namespace Lykke.Job.LogReader.Core.Settings
{
    [UsedImplicitly(ImplicitUseTargetFlags.WithMembers)]
    public class AppSettings : BaseAppSettings
    {
        public LogReaderSettings LogReaderJob { get; set; }
    }
}
