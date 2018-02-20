using System.Collections;
using System.Collections.Generic;

namespace Lykke.Job.LogReader.Core.Domain.Health
{
    public class HealthIssuesCollection : IReadOnlyCollection<HealthIssue>
    {
        private readonly List<HealthIssue> list;

        public HealthIssuesCollection()
        {
            this.list = new List<HealthIssue>();
        }

        public int Count => this.list.Count;

        public void Add(string type, string value)
        {
            this.list.Add(HealthIssue.Create(type, value));
        }

        public IEnumerator<HealthIssue> GetEnumerator()
        {
            return this.list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
