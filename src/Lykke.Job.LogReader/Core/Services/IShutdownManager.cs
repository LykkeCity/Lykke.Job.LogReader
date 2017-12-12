using System.Threading.Tasks;

namespace Lykke.Job.LogReader.Core.Services
{
    public interface IShutdownManager
    {
        Task StopAsync();
    }
}