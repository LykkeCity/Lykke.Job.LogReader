using System;
using System.Threading.Tasks;
using Autofac;
using Autofac.Extensions.DependencyInjection;
using AzureStorage.Tables;
using Common.Log;
using Lykke.Common.ApiLibrary.Middleware;
using Lykke.Common.ApiLibrary.Swagger;
using Lykke.Job.LogReader.Core.Services;
using Lykke.Job.LogReader.Core.Settings;
using Lykke.Job.LogReader.Models;
using Lykke.Job.LogReader.Modules;
using Lykke.Logs;
using Lykke.SettingsReader;
using Lykke.SlackNotification.AzureQueue;
using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;

namespace Lykke.Job.LogReader
{
    public class Startup
    {
        public Startup(IHostingEnvironment env)
        {
            var builder = new ConfigurationBuilder()
                .SetBasePath(env.ContentRootPath)
                .AddEnvironmentVariables();

            this.Configuration = builder.Build();
            this.Environment = env;
        }

        public IHostingEnvironment Environment { get; }

        public IContainer ApplicationContainer { get; private set; }

        public IConfigurationRoot Configuration { get; }

        public ILog Log { get; private set; }

        public IServiceProvider ConfigureServices(IServiceCollection services)
        {
            try
            {
                services.AddMvc()
                    .AddJsonOptions(options =>
                    {
                        options.SerializerSettings.ContractResolver =
                            new Newtonsoft.Json.Serialization.DefaultContractResolver();
                    });

                services.AddSwaggerGen(options =>
                {
                    options.DefaultLykkeConfiguration("v1", "LogReader API");
                });

                var builder = new ContainerBuilder();
                var appSettings = this.Configuration.LoadSettings<AppSettings>();

                this.Log = CreateLogWithSlack(appSettings);

                builder.RegisterModule(new JobModule(appSettings.CurrentValue.LogReaderJob, appSettings.Nested(x => x.LogReaderJob.Db), this.Log));

                builder.Populate(services);

                this.ApplicationContainer = builder.Build();

                return new AutofacServiceProvider(this.ApplicationContainer);
            }
            catch (Exception ex)
            {
                this.Log?.WriteFatalErrorAsync(nameof(Startup), nameof(this.ConfigureServices), string.Empty, ex).GetAwaiter().GetResult();
                throw;
            }
        }

        public void Configure(IApplicationBuilder app, IHostingEnvironment env, IApplicationLifetime appLifetime)
        {
            try
            {
                if (env.IsDevelopment())
                {
                    app.UseDeveloperExceptionPage();
                }

                app.UseLykkeMiddleware("LogReader", ex => new ErrorResponse { ErrorMessage = "Technical problem" });

                app.UseMvc();
                app.UseSwagger(c =>
                {
                    c.PreSerializeFilters.Add((swagger, httpReq) => swagger.Host = httpReq.Host.Value);
                });
                app.UseSwaggerUI(x =>
                {
                    x.RoutePrefix = "swagger/ui";
                    x.SwaggerEndpoint("/swagger/v1/swagger.json", "v1");
                });
                app.UseStaticFiles();

                appLifetime.ApplicationStarted.Register(() => this.StartApplication().GetAwaiter().GetResult());
                appLifetime.ApplicationStopping.Register(() => this.StopApplication().GetAwaiter().GetResult());
                appLifetime.ApplicationStopped.Register(() => this.CleanUp().GetAwaiter().GetResult());
            }
            catch (Exception ex)
            {
                this.Log?.WriteFatalErrorAsync(nameof(Startup), nameof(this.Configure), string.Empty, ex).GetAwaiter().GetResult();
                throw;
            }
        }

        private static ILog CreateLogWithSlack(IReloadingManager<AppSettings> settings)
        {
            var consoleLogger = new LogToConsole();
            var aggregateLogger = new AggregateLogger();

            aggregateLogger.AddLog(consoleLogger);

            var dbLogConnectionStringManager = settings.Nested(x => x.LogReaderJob.Db.LogsConnString);
            var dbLogConnectionString = dbLogConnectionStringManager.CurrentValue;

            if (string.IsNullOrEmpty(dbLogConnectionString))
            {
                consoleLogger.WriteWarningAsync(nameof(Startup), nameof(CreateLogWithSlack), "Table loggger is not inited").Wait();
                return aggregateLogger;
            }

            if (dbLogConnectionString.StartsWith("${", StringComparison.InvariantCultureIgnoreCase) && dbLogConnectionString.EndsWith("}", StringComparison.InvariantCultureIgnoreCase))
            {
                throw new InvalidOperationException($"LogsConnString {dbLogConnectionString} is not filled in settings");
            }

            var persistenceManager = new LykkeLogToAzureStoragePersistenceManager(
                AzureTableStorage<LogEntity>.Create(dbLogConnectionStringManager, "LogReaderLog", consoleLogger),
                consoleLogger);

            // Creating azure storage logger, which logs own messages to concole log
            var azureStorageLogger = new LykkeLogToAzureStorage(
                persistenceManager,
                null,
                consoleLogger);

            azureStorageLogger.Start();

            aggregateLogger.AddLog(azureStorageLogger);

            return aggregateLogger;
        }

        private async Task StartApplication()
        {
            try
            {
                // NOTE: Job not yet recieve and process IsAlive requests here
                await this.ApplicationContainer.Resolve<IStartupManager>().StartAsync().ConfigureAwait(false);
                await this.Log.WriteMonitorAsync(string.Empty, Program.EnvInfo, "Started").ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                await this.Log.WriteFatalErrorAsync(nameof(Startup), nameof(this.StartApplication), string.Empty, ex).ConfigureAwait(false);
                throw;
            }
        }

        private async Task StopApplication()
        {
            try
            {
                // NOTE: Job still can recieve and process IsAlive requests here, so take care about it if you add logic here.
                await this.ApplicationContainer.Resolve<IShutdownManager>().StopAsync().ConfigureAwait(false);
            }
            catch (Exception ex)
            {
                if (this.Log != null)
                {
                    await this.Log.WriteFatalErrorAsync(nameof(Startup), nameof(this.StopApplication), string.Empty, ex).ConfigureAwait(false);
                }

                throw;
            }
        }

        private async Task CleanUp()
        {
            try
            {
                // NOTE: Job can't recieve and process IsAlive requests here, so you can destroy all resources
                if (this.Log != null)
                {
                    await this.Log.WriteMonitorAsync(string.Empty, Program.EnvInfo, "Terminating").ConfigureAwait(false);
                }

                this.ApplicationContainer.Dispose();
            }
            catch (Exception ex)
            {
                if (this.Log != null)
                {
                    await this.Log.WriteFatalErrorAsync(nameof(Startup), nameof(this.CleanUp), string.Empty, ex).ConfigureAwait(false);
                    (this.Log as IDisposable)?.Dispose();
                }

                throw;
            }
        }
    }
}
