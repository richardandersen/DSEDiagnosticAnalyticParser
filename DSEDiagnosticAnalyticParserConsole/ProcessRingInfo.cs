using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
        static public void ReadRingFileParseIntoDataTables(IFilePath ringFilePath,
                                                            DataTable dtRingInfo,
                                                            DataTable dtTokenRange)
        {
            if (dtRingInfo.Columns.Count == 0)
            {
                dtRingInfo.Columns.Add("Node IPAddress", typeof(string));
                dtRingInfo.Columns[0].Unique = true;
                dtRingInfo.PrimaryKey = new System.Data.DataColumn[] { dtRingInfo.Columns[0] };
                dtRingInfo.Columns.Add("Data Center", typeof(string));
                dtRingInfo.Columns.Add("Rack", typeof(string));
                dtRingInfo.Columns.Add("Status", typeof(string));
                dtRingInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Cluster Name", typeof(string)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Storage Used (MB)", typeof(decimal)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Storage Utilization", typeof(decimal)).AllowDBNull = true;
                //dtRingInfo.Columns.Add("Number of Restarts", typeof(int)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Uptime", typeof(TimeSpan)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Heap Memory (MB)", typeof(string)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Off Heap Memory (MB)", typeof(decimal)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Nbr VNodes", typeof(int)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Nbr of Exceptions", typeof(int)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Read-Repair Service Enabled", typeof(bool)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Gossip Enableed", typeof(bool)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Thrift Enabled", typeof(bool)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Native Transport Enable", typeof(bool)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Key Cache Information", typeof(string)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Row Cache Information", typeof(string)).AllowDBNull = true;
                dtRingInfo.Columns.Add("Counter Cache Information", typeof(string)).AllowDBNull = true;
            }

            if (dtTokenRange.Columns.Count == 0)
            {
                dtTokenRange.Columns.Add("Data Center", typeof(string));
                dtTokenRange.Columns.Add("Node IPAddress", typeof(string));
                dtTokenRange.Columns.Add("Start Token (exclusive)", typeof(long));
                dtTokenRange.Columns.Add("End Token (inclusive)", typeof(long));
                dtTokenRange.Columns.Add("Slots", typeof(long));
                dtTokenRange.Columns.Add("Load(MB)", typeof(decimal));
            }

            var fileLines = ringFilePath.ReadAllLines();

            string currentDC = null;
            long? currentStartToken = null;
            long endToken;
            string line = null;
            string ipAddress;
            DataRow dataRow;
            List<string> parsedLine;
            bool newDC = true;
            bool rangeStart = false;

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (!string.IsNullOrEmpty(line))
                {
                    if (line.StartsWith("Datacenter:"))
                    {
                        newDC = true;
                        currentDC = line.Substring(12).Trim();
                    }
                    else if (newDC)
                    {
                        if (line[0] != '='
                                && !line.StartsWith("Address")
                                && !line.StartsWith("Note:")
                                && !line.StartsWith("Warning:"))
                        {
                            newDC = false;
                            rangeStart = true;
                            currentStartToken = long.Parse(line);
                        }
                    }
                    else
                    {

                        //Address         Rack        Status State   Load Type            Owns                Token (end)
                        parsedLine = Common.StringFunctions.Split(line,
                                                                    ' ',
                                                                    Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                    Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                        if (Char.IsDigit(parsedLine[0][0]) || parsedLine[0][0] == '-')
                        {
                            IPAddressStr(parsedLine[0], out ipAddress);

                            dataRow = dtRingInfo.Rows.Find(ipAddress);

                            if (dataRow == null)
                            {
                                dataRow = dtRingInfo.NewRow();

                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["Data Center"] = currentDC;
                                dataRow["Rack"] = parsedLine[1];
                                dataRow["Status"] = parsedLine[2];

                                dtRingInfo.Rows.Add(dataRow);
                            }

                            dataRow = dtTokenRange.NewRow();

                            dataRow["Data Center"] = currentDC;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Start Token (exclusive)"] = currentStartToken;
                            endToken = long.Parse(parsedLine[7]);
                            dataRow["End Token (inclusive)"] = endToken;

                            if (rangeStart)
                            {
                                rangeStart = false;
                                dataRow["Slots"] = (endToken - long.MinValue)
                                                        + (long.MaxValue - currentStartToken.Value);
                            }
                            else
                            {
                                dataRow["Slots"] = Math.Abs(endToken - currentStartToken.Value);
                            }

                            dataRow["Load(MB)"] = ConvertInToMB(parsedLine[4], parsedLine[5]);

                            currentStartToken = endToken;

                            dtTokenRange.Rows.Add(dataRow);
                        }
                    }
                }
            }
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

    }

}
