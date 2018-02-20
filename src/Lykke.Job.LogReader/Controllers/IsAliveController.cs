using System.Linq;
using System.Net;
using Lykke.Job.LogReader.Core.Services;
using Lykke.Job.LogReader.Models;
using Microsoft.AspNetCore.Mvc;
using Swashbuckle.AspNetCore.SwaggerGen;

namespace Lykke.Job.LogReader.Controllers
{
    // NOTE: See https://lykkex.atlassian.net/wiki/spaces/LKEWALLET/pages/35755585/Add+your+app+to+Monitoring
    [Route("api/[controller]")]
    public class IsAliveController : Controller
    {
        private readonly IHealthService healthService;

        public IsAliveController(IHealthService healthService)
        {
            this.healthService = healthService;
        }

        /// <summary>
        /// Checks service is alive
        /// </summary>
        /// <returns></returns>
        [HttpGet]
        [SwaggerOperation("IsAlive")]
        [ProducesResponseType(typeof(IsAliveResponse), (int)HttpStatusCode.OK)]
        [ProducesResponseType(typeof(ErrorResponse), (int)HttpStatusCode.InternalServerError)]
        public IActionResult Get()
        {
            var healthViloationMessage = this.healthService.GetHealthViolationMessage();
            if (healthViloationMessage != null)
            {
                return this.StatusCode((int)HttpStatusCode.InternalServerError, new ErrorResponse { ErrorMessage = $"Job is unhealthy: {healthViloationMessage}" });
            }

            // NOTE: Feel free to extend IsAliveResponse, to display job-specific indicators
            return this.Ok(new IsAliveResponse
            {
                Name = Microsoft.Extensions.PlatformAbstractions.PlatformServices.Default.Application.ApplicationName,
                Version = Microsoft.Extensions.PlatformAbstractions.PlatformServices.Default.Application.ApplicationVersion,
                Env = Program.EnvInfo,
#if DEBUG
                IsDebug = true,
#else
                IsDebug = false,
#endif
                IssueIndicators = this.healthService.GetHealthIssues()
                    .Select(i => new IsAliveResponse.IssueIndicator
                    {
                        Type = i.Type,
                        Value = i.Value
                    })
            });
        }
    }
}
