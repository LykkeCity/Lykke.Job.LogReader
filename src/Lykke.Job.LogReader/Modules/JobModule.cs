using Autofac;
using Lykke.Job.LogReader.Core.Settings;
using Lykke.Common.Log;
using Lykke.SettingsReader;
using Lykke.Job.LogReader.PeriodicalHandlers;

namespace Lykke.Job.LogReader.Modules
{
    public class JobModule : Module
    {
        private readonly IReloadingManager<AppSettings> _settingsManager;

        public JobModule(IReloadingManager<AppSettings> settingsManager)
        {
            _settingsManager = settingsManager;
        }

        protected override void Load(ContainerBuilder builder)
        {
            builder.Register(ctx => new AzureLogHandler(ctx.Resolve<ILogFactory>(), _settingsManager.CurrentValue.LogReaderJob.Reader, _settingsManager.Nested(x => x.LogReaderJob.Db)))
                .As<IStartable>()
                .AsSelf()
                .AutoActivate()
                .SingleInstance();
        }

    }
}
