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
        static public void initializeCFStatsDataTable(DataTable dtCFStats)
        {
            if (dtCFStats.Columns.Count == 0)
            {
                dtCFStats.Columns.Add("Source", typeof(string));
                dtCFStats.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCFStats.Columns.Add("Node IPAddress", typeof(string));
                dtCFStats.Columns.Add("KeySpace", typeof(string));
                dtCFStats.Columns.Add("Table", typeof(string)).AllowDBNull = true;
                dtCFStats.Columns.Add("Attribute", typeof(string));
                dtCFStats.Columns.Add("Value", typeof(object));
                dtCFStats.Columns.Add("Unit of Measure", typeof(string)).AllowDBNull = true;

                dtCFStats.Columns.Add("Size in MB", typeof(decimal)).AllowDBNull = true;
                dtCFStats.Columns.Add("Active", typeof(bool)).AllowDBNull = true;
                dtCFStats.Columns.Add("(Value)", typeof(object));
                
                //dtCFStats.PrimaryKey = new System.Data.DataColumn[] { dtFSStats.Columns[0],  dtFSStats.Columns[1],  dtFSStats.Columns[2],  dtFSStats.Columns[3], dtFSStats.Columns[4] };
            }

        }

        static public void ReadCFStatsFileParseIntoDataTable(IFilePath cfstatsFilePath,
                                                                string ipAddress,
                                                                string dcName,
                                                                System.Data.DataTable dtCFStats,
                                                                IEnumerable<string> ignoreKeySpaces,
                                                                IEnumerable<string> addToMBColumn)
        {

            initializeCFStatsDataTable(dtCFStats);

            var fileLines = cfstatsFilePath.ReadAllLines();
            string line;
            DataRow dataRow;
            List<string> parsedLine;
            List<string> parsedValue;
            string currentKS = null;
            string currentTbl = null;
            object numericValue;

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (!string.IsNullOrEmpty(line) && line[0] != '-')
                {
                    parsedLine = Common.StringFunctions.Split(line,
                                                                ':',
                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                Common.StringFunctions.SplitBehaviorOptions.Default);

                    if (parsedLine[0] == "Keyspace")
                    {
                        if (ignoreKeySpaces != null && ignoreKeySpaces.Contains(parsedLine[1].ToLower()))
                        {
                            currentKS = null;
                        }
                        else
                        {
                            currentKS = parsedLine[1];
                        }
                        currentTbl = null;
                    }
                    else if (currentKS == null)
                    {
                        continue;
                    }
                    else if (parsedLine[0] == "Table")
                    {
                        currentTbl = parsedLine[1];
                    }
                    else if (parsedLine[0] == "Table (index)")
                    {
                        currentTbl = parsedLine[1] + " (index)";
                    }
                    else
                    {
                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = "CFStats";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["KeySpace"] = currentKS;
                        dataRow["Table"] = currentTbl;
                        dataRow["Attribute"] = parsedLine[0];

                        parsedValue = Common.StringFunctions.Split(parsedLine[1],
                                                                    ' ',
                                                                    Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                    Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                        if (Common.StringFunctions.ParseIntoNumeric(parsedValue[0], out numericValue, true))
                        {
                            dataRow["Value"] = numericValue;
                            dataRow["(Value)"] = ((dynamic)numericValue) < 0 ? 0 : numericValue;

                            if (parsedValue.Count() > 1)
                            {
                                dataRow["Unit of Measure"] = parsedValue[1];
                            }

                            if (addToMBColumn != null)
                            {
                                var decNbr = decimal.Parse(numericValue.ToString());

                                foreach (var item in addToMBColumn)
                                {
                                    if (parsedLine[0].ToLower().Contains(item))
                                    {
                                        dataRow["Size in MB"] = decNbr / BytesToMB;
                                        break;
                                    }
                                }
                            }
                        }
                        else
                        {
                            dataRow["Unit of Measure"] = parsedLine[1];
                        }

                        dtCFStats.Rows.Add(dataRow);
                    }
                }
            }
        }
        static public void ReadCFStatsFileForKeyspaceTableInfo(IFilePath cfstatsFilePath,
                                                                IEnumerable<string> ignoreKeySpaces,
                                                                List<CKeySpaceTableNames> kstblNames)
        {
            var fileLines = cfstatsFilePath.ReadAllLines();
            string line;
            List<string> parsedLine;
            string currentKS = null;
            string currentTbl = null;

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (!string.IsNullOrEmpty(line) && line[0] != '-')
                {
                    parsedLine = Common.StringFunctions.Split(line,
                                                                ':',
                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                Common.StringFunctions.SplitBehaviorOptions.Default);

                    if (parsedLine[0] == "Keyspace")
                    {
                        if (ignoreKeySpaces != null && ignoreKeySpaces.Contains(parsedLine[1].ToLower()))
                        {
                            currentKS = null;
                        }
                        else
                        {
                            currentKS = parsedLine[1];
                        }
                        currentTbl = null;
                    }
                    else if (currentKS == null)
                    {
                        continue;
                    }
                    else if (parsedLine[0] == "Table")
                    {
                        currentTbl = parsedLine[1];
                    }
                    else if (parsedLine[0] == "Table (index)")
                    {
                        currentTbl = parsedLine[1];
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(currentKS) && !string.IsNullOrEmpty(currentTbl))
                        {
                            kstblNames.Add(new CKeySpaceTableNames(currentKS, currentTbl));
                        }
                    }
                }
            }
        }

        static public Common.Patterns.Collections.ThreadSafe.List<string> ActiveTables = new Common.Patterns.Collections.ThreadSafe.List<string>();

        static public void UpdateTableActiveStatus(System.Data.DataTable dtCFStats)
        {
            var activeTblView = from r in dtCFStats.AsEnumerable()
                                where (r.Field<string>("Attribute") == "Local read count"
                                            || r.Field<string>("Attribute") == "Local write count")
                                        && r.Field<dynamic>("Value") > 0
                                group r by new { ks = r.Field<string>("KeySpace"), tbl = r.Field<string>("Table") } into g
                                select g.Key.ks + '.' + g.Key.tbl;

            ActiveTables.AddRange(activeTblView);

            foreach (DataRow dataRow in dtCFStats.Rows)
            {
                if (dataRow["Table"] != DBNull.Value)
                {
                    dataRow["Active"] = ActiveTables.Contains(((string)dataRow["KeySpace"]) + '.' + ((string)dataRow["Table"]));
                }
            }
        }
    }
}
