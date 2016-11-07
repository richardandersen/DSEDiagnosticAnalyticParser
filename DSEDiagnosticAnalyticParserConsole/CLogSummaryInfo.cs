using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using System.Runtime.CompilerServices;

namespace DSEDiagnosticAnalyticParserConsole
{
    public class CLogSummaryInfo : IEqualityComparer<CLogSummaryInfo>, IEquatable<CLogSummaryInfo>
    {
        static long NextGroupIndicator = 0;

        [MethodImpl(MethodImplOptions.AggressiveInlining)]
        public static long IncrementGroupInicator()
        {
            return System.Threading.Interlocked.Increment(ref NextGroupIndicator);
        }

        public CLogSummaryInfo(DateTime period,
                                TimeSpan periodSpan,
                                string itemType,
                                string itemKey,
                                string itemPath,
                                string dataCenter,
                                string ipAdress)
        {
            this.DataCenter = dataCenter;
            this.IPAddress = ipAdress;
            this.ItemPath = itemPath;
            this.ItemType = itemType;
            this.ItemKey = itemKey;
            this.Period = period;
            this.PeriodSpan = periodSpan;
            this.AggregationCount = 0;
			this.MaxTimeStamp = null;            
            this.GroupIndicator = System.Threading.Interlocked.Increment(ref NextGroupIndicator);
        }

        public long GroupIndicator;
        public string DataCenter;
        public string IPAddress;
        public DateTime Period;
        public TimeSpan PeriodSpan;		
        public string ItemType;
        public string ItemKey;
        public string ItemPath;
        public int AggregationCount;
		public DateTime? MaxTimeStamp;
        public List<string> AssociatedItems = new List<string>();
        public List<object[]> AssociatedDataArrays = new List<object[]>();

		public bool Equals(CLogSummaryInfo x, CLogSummaryInfo y)
        {
            if (x == null && y == null)
                return true;
            else if (x == null | y == null)
                return false;

            return x.IPAddress == y.IPAddress
                    && x.DataCenter == y.DataCenter
                    && x.ItemType == y.ItemType
                    && x.ItemKey == y.ItemKey
                    && x.ItemPath == y.ItemPath
                    && x.Period == y.Period;
        }

        public bool Equals(CLogSummaryInfo y)
        {
            if (y == null)
                return false;

            return this.IPAddress == y.IPAddress
                    && this.DataCenter == y.DataCenter
                    && this.ItemType == y.ItemType
                    && this.ItemKey == y.ItemKey
                    && this.ItemPath == y.ItemPath
                    && this.Period == y.Period;
        }

        public bool Equals(DateTime period, string itemType, string itemKey, string itemPath, string dataCenter, string ipAdress)
        {            
            return this.IPAddress == ipAdress
                    && this.DataCenter == dataCenter
                    && this.ItemType == itemType
                    && this.ItemKey == itemKey
                    && this.ItemPath == itemPath
                    && this.Period == period;
        }

        public int GetHashCode(CLogSummaryInfo x)
        {
            if (x == null) return 0;

            return (x.IPAddress + x.DataCenter + x.ItemType + x.ItemPath + x.Period).GetHashCode();
        }

		public int Increment(DateTime timestamp, string assocItem, object[] datarowItemArray)
		{
			if (!this.MaxTimeStamp.HasValue || this.MaxTimeStamp < timestamp)
			{
				this.MaxTimeStamp = timestamp;
			}

            if (datarowItemArray != null)
            {
                this.AssociatedDataArrays.Add(datarowItemArray);
            }

            if(!string.IsNullOrEmpty(assocItem))
            {
                if(!this.AssociatedItems.Contains(assocItem))
                {
                    this.AssociatedItems.Add(assocItem);
                }
            }

			return ++this.AggregationCount;
		}
    }

}
