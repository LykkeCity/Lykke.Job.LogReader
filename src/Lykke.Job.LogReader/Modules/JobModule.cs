using Autofac;
using Autofac.Extensions.DependencyInjection;
using Common.Log;
using Lykke.Job.LogReader.Core.Services;
using Lykke.Job.LogReader.Core.Settings.JobSettings;
using Lykke.Job.LogReader.PeriodicalHandlers;
using Lykke.Job.LogReader.Services;
using Lykke.SettingsReader;
using Microsoft.Extensions.DependencyInjection;

namespace Lykke.Job.LogReader.Modules
{
    public class JobModule : Module
    {
        private readonly LogReaderSettings settings;
        private readonly IReloadingManager<DbSettings> dbSettingsManager;
        private readonly ILog log;

        // NOTE: you can remove it if you don't need to use IServiceCollection extensions to register service specific dependencies
        private readonly IServiceCollection services;

        public JobModule(LogReaderSettings settings, IReloadingManager<DbSettings> dbSettingsManager, ILog log)
        {
            this.settings = settings;
            this.log = log;
            this.dbSettingsManager = dbSettingsManager;

            this.services = new ServiceCollection();
        }

        protected override void Load(ContainerBuilder builder)
        {
            // NOTE: Do not register entire settings in container, pass necessary settings to services which requires them
            // ex:
            // builder.RegisterType<QuotesPublisher>()
            //  .As<IQuotesPublisher>()
            //  .WithParameter(TypedParameter.From(_settings.Rabbit.ConnectionString))
            builder.RegisterInstance(this.log)
                .As<ILog>()
                .SingleInstance();

            builder.RegisterType<HealthService>()
                .As<IHealthService>()
                .SingleInstance();

            builder.RegisterType<StartupManager>()
                .As<IStartupManager>();

            builder.RegisterType<ShutdownManager>()
                .As<IShutdownManager>();

            builder.RegisterInstance(new AzureLogHandler(this.log, this.settings.Reader, this.dbSettingsManager))
                .As<IStartable>()
                .AutoActivate()
                .SingleInstance();

            builder.RegisterInstance(this.settings.Reader);
            builder.RegisterInstance(this.settings.Db);

            builder.Populate(this.services);
        }
    }
}
