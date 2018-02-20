using System.Collections;
using System.Collections.Generic;

namespace Lykke.Job.LogReader.Core.Domain.Health
{
    public class HealthIssuesCollection : IReadOnlyCollection<HealthIssue>
    {
        private readonly List<HealthIssue> _list;

        public HealthIssuesCollection()
        {
            this._list = new List<HealthIssue>();
        }

        public int Count => this._list.Count;

        public void Add(string type, string value)
        {
            this._list.Add(HealthIssue.Create(type, value));
        }

        public IEnumerator<HealthIssue> GetEnumerator()
        {
            return this._list.GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return this.GetEnumerator();
        }
    }
}
