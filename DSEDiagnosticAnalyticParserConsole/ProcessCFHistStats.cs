using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using System.Text.RegularExpressions;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
        public static void ProcessCFHistStats(DataTable dtCFHistogram,
                                              DataTable dtCFStats)
        {
            if(dtCFHistogram.Rows.Count == 0)
            {
                return;
            }

            initializeCFStatsDataTable(dtCFStats);

            //dtCFHistogram.Columns.Add("Source", typeof(string));
            //dtCFHistogram.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
            //dtCFHistogram.Columns.Add("Node IPAddress", typeof(string));
            //dtCFHistogram.Columns.Add("KeySpace", typeof(string));
            //dtCFHistogram.Columns.Add("Table", typeof(string));
            //dtCFHistogram.Columns.Add("Attribute", typeof(string));

            //dtCFHistogram.Columns.Add("50%", typeof(object)).AllowDBNull = true;
            //dtCFHistogram.Columns.Add("75%", typeof(object)).AllowDBNull = true;
            //dtCFHistogram.Columns.Add("95%", typeof(object)).AllowDBNull = true;
            //dtCFHistogram.Columns.Add("98%", typeof(object)).AllowDBNull = true;
            //dtCFHistogram.Columns.Add("99%", typeof(object)).AllowDBNull = true;
            //dtCFHistogram.Columns.Add("Min", typeof(object)).AllowDBNull = true;
            //dtCFHistogram.Columns.Add("Max", typeof(object)).AllowDBNull = true;

            //dtCFStats.Columns.Add("Source", typeof(string));
            //dtCFStats.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
            //dtCFStats.Columns.Add("Node IPAddress", typeof(string));
            //dtCFStats.Columns.Add("KeySpace", typeof(string));
            //dtCFStats.Columns.Add("Table", typeof(string)).AllowDBNull = true;
            //dtCFStats.Columns.Add("Attribute", typeof(string));
            //dtCFStats.Columns.Add("Value", typeof(object));
            //dtCFStats.Columns.Add("Unit of Measure", typeof(string)).AllowDBNull = true;

            //dtCFStats.Columns.Add("Size in MB", typeof(decimal)).AllowDBNull = true;
            //dtCFStats.Columns.Add("Active", typeof(bool)).AllowDBNull = true;
            //dtCFStats.Columns.Add("(Value)", typeof(object));

            var cfAttrs = from row in dtCFHistogram.AsEnumerable()
                            where (new string[] {"SSTables", "Write Latency", "Read Latency", "Partition Size" }).Contains(row.Field<string>("Attribute"))
                            select new { Source = row.Field<string>("Source"),
                                            DataCenter = row.Field<string>("Data Center"),
                                            IPAdress = row.Field<string>("Node IPAddress"),
                                            KeySpace = row.Field<string>("KeySpace"),
                                            Table = row.Field<string>("Table"),
                                            Attribute = row.Field<string>("Attribute"),
                                            Min = row.Field<object>("Min"),
                                            Max = row.Field<object>("Max")
                            };

            foreach (var attr in cfAttrs)
            {
                var row = dtCFStats.NewRow();

                row.SetField("Source", attr.Source);
                row.SetField("Data Center", attr.DataCenter);
                row.SetField("Node IPAddress", attr.IPAdress);
                row.SetField("KeySpace", attr.KeySpace);
                row.SetField("Table", attr.Table);

                var value = attr.Max;

                switch (attr.Attribute.ToLower())
                {
                    case "sstables":
                        row.SetField("Attribute", "SSTable read maximum");
                        row.SetField("Value", value);
                        row.SetField("Unit of Measure", "SSTables");
                        row.SetField("(Value)", value);
                        break;
                    case "write latency":
                        row.SetField("Attribute", "Write latency maximum");
                        row.SetField("Value", value);
                        row.SetField("Unit of Measure", "ms");
                        row.SetField("(Value)", value);
                        break;
                    case "read latency":
                        row.SetField("Attribute", "Read latency maximum");
                        row.SetField("Value", value);
                        row.SetField("Unit of Measure", "ms");
                        row.SetField("(Value)", value);
                        break;
                    case "partition size":
                        row.SetField("Attribute", "Partition large maximum");
                        row.SetField("Value", value == null ? (object) null : (object) (((dynamic) value) * BytesToMB)) ;
                        row.SetField("Unit of Measure", "bytes");
                        row.SetField("Size in MB", value);
                        row.SetField("(Value)", row.Field<object>("Value"));
                        break;
                    default:
                        var msg = string.Format("CFHist to CFStats invalid attribute \"{0}\" for {1}, {2}, {3}", attr.Attribute, attr.IPAdress, attr.KeySpace, attr.Table);
                        Program.ConsoleErrors.Increment(string.Empty, msg, 45);
                        Logger.Instance.Error(msg);
                        break;
                }

                dtCFStats.Rows.Add(row);
            }           
        }
    }
}
