using Autofac;
using Autofac.Extensions.DependencyInjection;
using Common.Log;
using Lykke.Job.LogReader.Core.Services;
using Lykke.Job.LogReader.Core.Settings.JobSettings;
using Lykke.Job.LogReader.Services;
using Lykke.SettingsReader;
using Lykke.Job.LogReader.PeriodicalHandlers;
using Microsoft.Extensions.DependencyInjection;

namespace Lykke.Job.LogReader.Modules
{
    public class JobModule : Module
    {
        private readonly LogReaderSettings _settings;
        private readonly IReloadingManager<DbSettings> _dbSettingsManager;
        private readonly ILog _log;
        // NOTE: you can remove it if you don't need to use IServiceCollection extensions to register service specific dependencies
        private readonly IServiceCollection _services;

        public JobModule(LogReaderSettings settings, IReloadingManager<DbSettings> dbSettingsManager, ILog log)
        {
            _settings = settings;
            _log = log;
            _dbSettingsManager = dbSettingsManager;

            _services = new ServiceCollection();
        }

        protected override void Load(ContainerBuilder builder)
        {
            // NOTE: Do not register entire settings in container, pass necessary settings to services which requires them
            // ex:
            // builder.RegisterType<QuotesPublisher>()
            //  .As<IQuotesPublisher>()
            //  .WithParameter(TypedParameter.From(_settings.Rabbit.ConnectionString))

            builder.RegisterInstance(_log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>();

            builder.RegisterInstance(new AzureLogHandler(_log, _settings.Reader, _dbSettingsManager))
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();
            
            builder.RegisterInstance(_settings.Reader);
            builder.RegisterInstance(_settings.Db);

            builder.Populate(_services);
        }

    }
}
