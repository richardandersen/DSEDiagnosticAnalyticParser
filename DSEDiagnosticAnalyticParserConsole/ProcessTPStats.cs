using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using DSEDiagnosticToDataTable;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
        static public void initializeTPStatsDataTable(DataTable dtTPStats)
        {
            if (dtTPStats.Columns.Count == 0)
            {
                dtTPStats.Columns.Add("Source", typeof(string));
                dtTPStats.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtTPStats.Columns.Add("Node IPAddress", typeof(string));
                dtTPStats.Columns.Add("Attribute", typeof(string));

                dtTPStats.Columns.Add("Active", typeof(long)).AllowDBNull = true; //E
                dtTPStats.Columns.Add("Pending", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Completed", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Blocked", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("All time blocked", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Dropped", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Latency (ms)", typeof(int)).AllowDBNull = true;
                dtTPStats.Columns.Add("Occurrences", typeof(int)).AllowDBNull = true; //L
                dtTPStats.Columns.Add("Size (mb)", typeof(decimal)).AllowDBNull = true;
				dtTPStats.Columns.Add("GC Eden Space Change (mb)", typeof(decimal)).AllowDBNull = true;
				dtTPStats.Columns.Add("GC Survivor Space Change (mb)", typeof(decimal)).AllowDBNull = true;
				dtTPStats.Columns.Add("GC Old Space Change (mb)", typeof(decimal)).AllowDBNull = true; //p
				dtTPStats.Columns.Add("IORate (mb/sec)", typeof(decimal)).AllowDBNull = true; //q
                dtTPStats.Columns.Add("Max", typeof(decimal)).AllowDBNull = true; 
                dtTPStats.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;
            }
        }

        static public void ReadTPStatsFileParseIntoDataTable(IFilePath tpstatsFilePath,
                                                                string ipAddress,
                                                                string dcName,
                                                                DataTable dtTPStats)
        {

            initializeTPStatsDataTable(dtTPStats);

            var fileInfo = new DSEDiagnosticFileParser.file_nodetool_tpstats(tpstatsFilePath, "dseCluster", dcName);

            fileInfo.ProcessFile();
            fileInfo.Task?.Wait();

            if (fileInfo.NbrWarnings > 0)
            {
                Program.ConsoleWarnings.Increment("TPStats Parsing Warnings Detected");
            }
            if (fileInfo.NbrErrors > 0)
            {
                Program.ConsoleErrors.Increment("TPStats Parsing Exceptions Detected");
            }

            DataRow dataRow = null;
            var node = fileInfo.Node;
            var statCollection = node.AggregatedStats.Where(i => (i.Class & DSEDiagnosticLibrary.EventClasses.NodeStats) != 0
                                                                && (i.Class & DSEDiagnosticLibrary.EventClasses.Keyspace) == 0
                                                                && (i.Class & DSEDiagnosticLibrary.EventClasses.TableViewIndex) == 0);

            Logger.Instance.InfoFormat("Loading {0} NodeStats", statCollection.Count());

            foreach (var stat in statCollection)
            {                
                var tpStatGroups = from statItem in stat.Data
                                   let attrKeyCol = TPStatsDataTable.GetColumnNameFromAttributeKey(statItem.Key)
                                   group new { Col = attrKeyCol.Item2, Value = statItem.Value } by attrKeyCol.Item1 into g
                                   select new { Attr = g.Key, Values = g };

                foreach (var item in tpStatGroups)
                {                    
                    dataRow = dtTPStats.NewRow();
                    
                    dataRow.SetField("Source", stat.Source.ToString());
                    dataRow.SetField("Data Center", stat.DataCenter.Name);
                    dataRow.SetField("Node IPAddress", stat.Node.Id.NodeName());

                    dataRow.SetField("Attribute", item.Attr);

                    foreach (var itemValue in item.Values)
                    {
                        if (itemValue.Col.Length == 0 || itemValue.Col.Last() == '%') continue;

                        if (itemValue.Value is DSEDiagnosticLibrary.UnitOfMeasure)
                        {
                            dataRow.SetFieldToDecimal(itemValue.Col, (DSEDiagnosticLibrary.UnitOfMeasure)itemValue.Value, DSEDiagnosticLibrary.UnitOfMeasure.Types.MS);
                        }
                        else
                        {
                            dataRow.SetField(itemValue.Col, itemValue.Value);
                        }
                    }

                    if (stat.ReconciliationRefs.HasAtLeastOneElement())
                    {
                        dataRow.SetFieldStringLimit(ColumnNames.ReconciliationRef,
                                                        stat.ReconciliationRefs.IsMultiple()
                                                            ? (object)string.Join(",", stat.ReconciliationRefs)
                                                            : (object)stat.ReconciliationRefs.First());
                    }

                    dtTPStats.Rows.Add(dataRow);
                }
            }
        }


    }
}
