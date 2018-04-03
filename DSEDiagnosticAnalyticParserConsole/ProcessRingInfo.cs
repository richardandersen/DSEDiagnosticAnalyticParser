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
		static void InitializeRingDataTable(DataTable dtRingInfo)
		{
			if (dtRingInfo.Columns.Count == 0)
			{
				dtRingInfo.Columns.Add("Node IPAddress", typeof(string));
				dtRingInfo.Columns["Node IPAddress"].Unique = true; //A
				dtRingInfo.PrimaryKey = new System.Data.DataColumn[] { dtRingInfo.Columns["Node IPAddress"] };
				dtRingInfo.Columns.Add("Data Center", typeof(string));
				dtRingInfo.Columns.Add("Rack", typeof(string));
				dtRingInfo.Columns.Add("Status", typeof(string));
				dtRingInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;
				dtRingInfo.Columns.Add("Cluster Name", typeof(string)).AllowDBNull = true;
				dtRingInfo.Columns.Add("Storage Used (MB)", typeof(decimal)).AllowDBNull = true; //G
				dtRingInfo.Columns.Add("Storage Utilization", typeof(decimal)).AllowDBNull = true;
				dtRingInfo.Columns.Add("Health Rating", typeof(decimal)).AllowDBNull = true;
				dtRingInfo.Columns.Add("Uptime (Days)", typeof(TimeSpan)).AllowDBNull = true; //J
                dtRingInfo.Columns.Add("Uptime", typeof(string)).AllowDBNull = true; //K
                dtRingInfo.Columns.Add("Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;//L
                dtRingInfo.Columns.Add("Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Log Duration", typeof(TimeSpan)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;//O
                dtRingInfo.Columns.Add("Debug Log Min Timestamp", typeof(DateTime)).AllowDBNull = true;//P
                dtRingInfo.Columns.Add("Debug Log Max Timestamp", typeof(DateTime)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Debug Log Duration", typeof(TimeSpan)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Debug Log Timespan Difference", typeof(TimeSpan)).AllowDBNull = true;//s

                dtRingInfo.Columns.Add("Heap Memory (MB)", typeof(string)).AllowDBNull = true; //t
				dtRingInfo.Columns.Add("Off Heap Memory (MB)", typeof(decimal)).AllowDBNull = true;//u
				dtRingInfo.Columns.Add("Nbr VNodes", typeof(int)).AllowDBNull = true;//v
				dtRingInfo.Columns.Add("Nbr of Exceptions", typeof(int)).AllowDBNull = true;//w
                dtRingInfo.Columns.Add("Percent Repaired", typeof(decimal)).AllowDBNull = true;//x
                dtRingInfo.Columns.Add("Repair Service Enabled", typeof(bool)).AllowDBNull = true;//y
				dtRingInfo.Columns.Add("Gossip Enabled", typeof(bool)).AllowDBNull = true;//z
				dtRingInfo.Columns.Add("Thrift Enabled", typeof(bool)).AllowDBNull = true;//aa
				dtRingInfo.Columns.Add("Native Transport Enabled", typeof(bool)).AllowDBNull = true;//ab
				dtRingInfo.Columns.Add("Key Cache Information", typeof(string)).AllowDBNull = true;//ac
				dtRingInfo.Columns.Add("Row Cache Information", typeof(string)).AllowDBNull = true;//ad
				dtRingInfo.Columns.Add("Counter Cache Information", typeof(string)).AllowDBNull = true;//ae
			}
		}

        static public bool ReadRingFileParseIntoDataTables(IFilePath ringFilePath,
                                                            DataTable dtRingInfo,
                                                            DataTable dtTokenRange)
        {
			InitializeRingDataTable(dtRingInfo);

            if (dtTokenRange.Columns.Count == 0)
            {
                dtTokenRange.Columns.Add("Data Center", typeof(string));
                dtTokenRange.Columns.Add("Node IPAddress", typeof(string));
                dtTokenRange.Columns.Add("Start Token (exclusive)", typeof(string));
                dtTokenRange.Columns.Add("End Token (inclusive)", typeof(string));
                dtTokenRange.Columns.Add("Slots", typeof(string));
                dtTokenRange.Columns.Add("Load(MB)", typeof(decimal));
            }

            var fileInfo = new DSEDiagnosticFileParser.file_nodetool_ring(ringFilePath, "dseCluster", null);

            fileInfo.ProcessFile();
            fileInfo.Task?.Wait();

            DataRow dataRow;

            foreach (var node in DSEDiagnosticLibrary.Cluster.MasterCluster.Nodes)
            {
                dataRow = dtRingInfo.Rows.Find(node.Id.NodeName());

                if (dataRow == null)
                {
                    dataRow = dtRingInfo.NewRow();

                    dataRow.SetField("Node IPAddress", node.Id.NodeName());
                    dataRow.SetField("Data Center", node.DataCenter.Name);
                    dataRow.SetField("Rack", node.DSE.Rack);
                    dataRow.SetField("Status", node.DSE.Statuses.ToString());
                   
                    dtRingInfo.Rows.Add(dataRow);
                }

                foreach (var token in node.DSE.TokenRanges)
                {
                    dataRow = dtTokenRange.NewRow();

                    dataRow.SetField("Data Center", node.DataCenter.Name);
                    dataRow.SetField("Node IPAddress", node.Id.NodeName());

                    dataRow.SetField("Start Token (exclusive)", token.StartRange.ToString());
                    dataRow.SetField("End Token (inclusive)", token.EndRange.ToString());
                    dataRow.SetField("Slots", token.SlotsFormatted());
                    dataRow.SetFieldToDecimal("Load(MB)", token.Load, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB);

                    dtTokenRange.Rows.Add(dataRow);
                }
            }

            return true;
        }

        static public void UpdateRingInfo(DataTable dtRingInfo,
                                            DataTable dtCYaml)
        {
            if (dtRingInfo.Rows.Count == 0 || dtCYaml.Rows.Count == 0)
            {
                return;
            }

            var yamlClusterNameView = new DataView(dtCYaml,
                                                    "[Node IPAddress] = '<Common>' and [Property] = 'cluster_name' and [Yaml Type] = 'cassandra'",
                                                    null,
                                                    DataViewRowState.CurrentRows);

            if (yamlClusterNameView.Count >= 1)
            {
                foreach (DataRow drRingInfo in dtRingInfo.Rows)
                {
                    foreach (DataRowView drView in yamlClusterNameView)
                    {
                        if (drView["Data Center"] == drRingInfo["Data Center"])
                        {
                            drRingInfo["Cluster Name"] = drView["Value"];
                        }
                    }
                }

                return;
            }

            foreach (DataRow drRingInfo in dtRingInfo.Rows)
            {
                yamlClusterNameView = new DataView(dtCYaml,
                                                    string.Format("[Data Center] = '{0}' and [Node IPAddress] = '{1}' and [Property] = 'cluster_name' and [Yaml Type] = 'cassandra'",
                                                                    drRingInfo["Data Center"],
                                                                    drRingInfo["Node IPAddress"]),
                                                    null,
                                                    DataViewRowState.CurrentRows);

                if (yamlClusterNameView.Count >= 1)
                {
                    drRingInfo["Cluster Name"] = yamlClusterNameView[0]["Value"] as string;
                }
            }
        }

        static public void UpdateRingInfo(DataTable dtRingInfo,
                                            Common.Patterns.Collections.ThreadSafe.Dictionary<string, List<LogCassandraNodeMaxMinTimestamp>> logCassandraNodeMaxMinTimestamps)
        {
            if (logCassandraNodeMaxMinTimestamps.Count == 0)
            {
                return;
            }

            foreach (var logNodeMaxMin in logCassandraNodeMaxMinTimestamps)
            {
                var onlyLogRanges = logNodeMaxMin.Value.Where(r => !r.IsDebugFile).Select(r => r.LogRange);
                DataRow nodeRow = null;

                if (onlyLogRanges.HasAtLeastOneElement())
                {
                    var minTimeFrame = onlyLogRanges.Min(c => c.Min);
                    var maxTimeFrame = onlyLogRanges.Max(c => c.Max);
                    var timespan = TimeSpan.FromMilliseconds(onlyLogRanges.Sum(c => c.TimeSpan().TotalMilliseconds));
                    
                    if (!string.IsNullOrEmpty(logNodeMaxMin.Key)
                                && minTimeFrame != DateTime.MinValue
                                && maxTimeFrame != DateTime.MaxValue)
                    {
                        nodeRow = dtRingInfo.Rows.Find(logNodeMaxMin.Key);

                        if (nodeRow == null)
                        {
                            nodeRow = dtRingInfo.NewRow();
                            nodeRow.SetField<string>("Node IPAddress", logNodeMaxMin.Key);
                            nodeRow.SetField<DateTime>("Log Min Timestamp", minTimeFrame);
                            nodeRow.SetField<DateTime>("Log Max Timestamp", maxTimeFrame);
                            nodeRow.SetField<TimeSpan>("Log Duration", timespan);
                            nodeRow.SetField<TimeSpan>("Log Timespan Difference", TimeSpan.FromMilliseconds(Math.Abs((maxTimeFrame - minTimeFrame).TotalMilliseconds - timespan.TotalMilliseconds)));
                            dtRingInfo.Rows.Add(nodeRow);
                        }
                        else
                        {
                            nodeRow.BeginEdit();
                            nodeRow.SetField<DateTime>("Log Min Timestamp", minTimeFrame);
                            nodeRow.SetField<DateTime>("Log Max Timestamp", maxTimeFrame);
                            nodeRow.SetField<TimeSpan>("Log Duration", timespan);
                            nodeRow.SetField<TimeSpan>("Log Timespan Difference", TimeSpan.FromMilliseconds(Math.Abs((maxTimeFrame - minTimeFrame).TotalMilliseconds - timespan.TotalMilliseconds)));
                            nodeRow.EndEdit();
                        }
                    }
                }

                onlyLogRanges = logNodeMaxMin.Value.Where(r => r.IsDebugFile).Select(r => r.LogRange);

                if (onlyLogRanges.HasAtLeastOneElement())
                {
                    var minTimeFrame = onlyLogRanges.Min(c => c.Min);
                    var maxTimeFrame = onlyLogRanges.Max(c => c.Max);
                    var timespan = TimeSpan.FromMilliseconds(onlyLogRanges.Sum(c => c.TimeSpan().TotalMilliseconds));

                    if (!string.IsNullOrEmpty(logNodeMaxMin.Key)
                                && minTimeFrame != DateTime.MinValue
                                && maxTimeFrame != DateTime.MaxValue)
                    {
                        if (nodeRow == null)
                        {
                            nodeRow = dtRingInfo.Rows.Find(logNodeMaxMin.Key);
                        }

                        if (nodeRow == null)
                        {
                            nodeRow = dtRingInfo.NewRow();
                            nodeRow.SetField<string>("Node IPAddress", logNodeMaxMin.Key);
                            nodeRow.SetField<DateTime>("Debug Log Min Timestamp", minTimeFrame);
                            nodeRow.SetField<DateTime>("Debug Log Max Timestamp", maxTimeFrame);
                            nodeRow.SetField<TimeSpan>("Debug Log Duration", timespan);
                            nodeRow.SetField<TimeSpan>("Debug Log Timespan Difference", TimeSpan.FromMilliseconds(Math.Abs((maxTimeFrame - minTimeFrame).TotalMilliseconds - timespan.TotalMilliseconds)));
                            dtRingInfo.Rows.Add(nodeRow);
                        }
                        else
                        {
                            nodeRow.BeginEdit();
                            nodeRow.SetField<DateTime>("Debug Log Min Timestamp", minTimeFrame);
                            nodeRow.SetField<DateTime>("Debug Log Max Timestamp", maxTimeFrame);
                            nodeRow.SetField<TimeSpan>("Debug Log Duration", timespan);
                            nodeRow.SetField<TimeSpan>("Debug Log Timespan Difference", TimeSpan.FromMilliseconds(Math.Abs((maxTimeFrame - minTimeFrame).TotalMilliseconds - timespan.TotalMilliseconds)));
                            nodeRow.EndEdit();
                        }
                    }
                }
            }
        }
    }

}
