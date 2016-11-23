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
        static public void initializeTPStatsDataTable(DataTable dtTPStats)
        {
            if (dtTPStats.Columns.Count == 0)
            {
                dtTPStats.Columns.Add("Source", typeof(string));
                dtTPStats.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtTPStats.Columns.Add("Node IPAddress", typeof(string));
                dtTPStats.Columns.Add("Attribute", typeof(string));

                dtTPStats.Columns.Add("Active", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Pending", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Completed", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Blocked", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("All time blocked", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Dropped", typeof(long)).AllowDBNull = true;
                dtTPStats.Columns.Add("Latency (ms)", typeof(int)).AllowDBNull = true;
                dtTPStats.Columns.Add("Occurrences", typeof(int)).AllowDBNull = true;
                dtTPStats.Columns.Add("Size (mb)", typeof(decimal)).AllowDBNull = true;
                dtTPStats.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;
            }
        }

        static public void ReadTPStatsFileParseIntoDataTable(IFilePath tpstatsFilePath,
                                                                string ipAddress,
                                                                string dcName,
                                                                DataTable dtTPStats)
        {

            initializeTPStatsDataTable(dtTPStats);

            var fileLines = tpstatsFilePath.ReadAllLines();
            string line;
            DataRow dataRow;
            int parsingSection = -1; //0 -- Pool, 1 -- Message Type
            List<string> parsedValue;

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (string.IsNullOrEmpty(line))
                {
                    continue;
                }
                if (line.StartsWith("Pool Name"))
                {
                    parsingSection = 0;
                    continue;
                }
                else if (line.StartsWith("Message type"))
                {
                    parsingSection = 1;
                    continue;
                }
                else if(parsingSection == -1)
                {
                    Logger.Instance.ErrorFormat("TPStats file has an invalid file format for node {0}. File skipped.", ipAddress);
                    Program.ConsoleErrors.Increment("TPStats File Skipped",  string.Format("Invalid File Format for node {0}", ipAddress));
                    break;
                }

                parsedValue = Common.StringFunctions.Split(line,
                                                            ' ',
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
                try
                {                
                    dataRow = dtTPStats.NewRow();

                    dataRow["Source"] = "TPStats";
                    dataRow["Data Center"] = dcName;
                    dataRow["Node IPAddress"] = ipAddress;
                    dataRow["Attribute"] = parsedValue[0];

                    if (parsingSection == 0)
                    {
                        //Pool Name                    Active   Pending      Completed   Blocked  All time blocked
                        dataRow["Active"] = long.Parse(parsedValue[1]);
                        dataRow["Pending"] = long.Parse(parsedValue[2]);
                        dataRow["Completed"] = long.Parse(parsedValue[3]);
                        dataRow["Blocked"] = long.Parse(parsedValue[4]);
                        dataRow["All time blocked"] = long.Parse(parsedValue[5]);
                    }
                    else if (parsingSection == 1)
                    {
                        //Message type           Dropped
                        dataRow["Dropped"] = long.Parse(parsedValue[1]);
                    }

                    dtTPStats.Rows.Add(dataRow);
                }
                catch (System.Exception ex)
                {
                    Logger.Instance.Error(string.Format("Parsing for TPStats for Node {0} failed during parsing of line \"{1}\". Line skipped.",
                                                            ipAddress,
                                                            line),
                                            ex);
                    Program.ConsoleWarnings.Increment("TPStats Parsing Exception; Line Skipped");
                }
            }
        }


    }
}
