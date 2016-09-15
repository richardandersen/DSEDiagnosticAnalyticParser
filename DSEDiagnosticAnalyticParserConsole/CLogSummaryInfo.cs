using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;

namespace DSEDiagnosticAnalyticParserConsole
{
    public class CLogSummaryInfo : IEqualityComparer<CLogSummaryInfo>, IEquatable<CLogSummaryInfo>
    {
        public CLogSummaryInfo(DateTime period, TimeSpan periodSpan, string itemType, string itemValue, DataRowView dataRow)
        {
            //		dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
            //		dtCLog.Columns.Add("Node IPAddress", typeof(string));
            //		dtCLog.Columns.Add("Timestamp", typeof(DateTime));
            //		dtCLog.Columns.Add("Indicator", typeof(string));
            //		dtCLog.Columns.Add("Task", typeof(string));
            //		dtCLog.Columns.Add("Item", typeof(string));
            //		dtCLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true;
            //		dtCLog.Columns.Add("Exception Description", typeof(string)).AllowDBNull = true;
            //		dtCLog.Columns.Add("Associated Item", typeof(string)).AllowDBNull = true;
            //		dtCLog.Columns.Add("Associated Value", typeof(object)).AllowDBNull = true;
            //		dtCLog.Columns.Add("Description", typeof(string));
            //		dtCLog.Columns.Add("Flagged", typeof(bool)).AllowDBNull = true;

            this.DataCenter = dataRow == null ? null : dataRow["Data Center"] as string;
            this.IPAddress = dataRow == null ? null : (string)dataRow["Node IPAddress"];
            this.AssociatedItem = dataRow == null ? null : dataRow["Associated Item"] as string;
            this.ItemType = itemType;
            this.ItemValue = itemValue;
            this.Period = period;
            this.PeriodSpan = periodSpan;
            this.AggregationCount = 0;
        }

        public CLogSummaryInfo(DateTime period, TimeSpan periodSpan, string itemType, string itemValue, string AssociatedItem, string ipAddress, string dcName)
        {
            this.DataCenter = dcName;
            this.IPAddress = ipAddress;
            this.ItemType = itemType;
            this.ItemValue = itemValue;
            this.Period = period;
            this.PeriodSpan = periodSpan;
            this.AssociatedItem = AssociatedItem;
            this.AggregationCount = 0;
        }

        public string DataCenter;
        public string IPAddress;
        public DateTime Period;
        public TimeSpan PeriodSpan;
        public string ItemType;
        public string ItemValue;
        public string AssociatedItem;
        public int AggregationCount;

        public bool Equals(CLogSummaryInfo x, CLogSummaryInfo y)
        {
            if (x == null && y == null)
                return true;
            else if (x == null | y == null)
                return false;

            return x.IPAddress == y.IPAddress
                    && x.DataCenter == y.DataCenter
                    && x.ItemType == y.ItemType
                    && x.ItemValue == y.ItemValue
                    && x.AssociatedItem == y.AssociatedItem
                    && x.Period == y.Period;
        }

        public bool Equals(CLogSummaryInfo y)
        {
            if (y == null)
                return false;

            return this.IPAddress == y.IPAddress
                    && this.DataCenter == y.DataCenter
                    && this.ItemType == y.ItemType
                    && this.ItemValue == y.ItemValue
                    && this.AssociatedItem == y.AssociatedItem
                    && this.Period == y.Period;
        }

        public bool Equals(DateTime period, string itemType, string itemValue, DataRowView dataRow)
        {
            if (dataRow == null) return false;

            return this.IPAddress == (string)dataRow["Node IPAddress"]
                    && this.DataCenter == dataRow["Data Center"] as string
                    && this.ItemType == itemType
                    && this.ItemValue == itemValue
                    && this.AssociatedItem == dataRow["Associated Item"] as string
                    && this.Period == period;
        }

        public int GetHashCode(CLogSummaryInfo x)
        {
            if (x == null) return 0;

            return (x.IPAddress + x.DataCenter + x.AssociatedItem + x.ItemType + x.ItemValue + x.Period).GetHashCode();
        }
    }

}
