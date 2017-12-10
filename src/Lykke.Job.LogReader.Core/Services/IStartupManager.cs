using System.Threading.Tasks;

namespace Lykke.Job.LogReader.Core.Services
{
    public interface IStartupManager
    {
        Task StartAsync();
    }
}