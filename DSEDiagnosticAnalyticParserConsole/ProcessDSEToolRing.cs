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
        static public void ReadDSEToolRingFileParseIntoDataTable(IFilePath dseRingFilePath,
                                                                    DataTable dtRingInfo)
        {
            var fileLines = dseRingFilePath.ReadAllLines();
            string line;
            List<string> parsedLine;
            string ipAddress;
            DataRow dataRow;

            //Note: Ownership information does not include topology, please specify a keyspace.
            //Address 			DC			Rack 	Workload	Status 	State	Load 		Owns	VNodes
            //10.27.34.17 		DC1 		RAC1	Cassandra 	Up		Normal	48.36 GB	6.31 % 	256
            //Warning: Node 10.27.34.54 is serving 1.20 times the token space of node 10.27.34.52, which means it will be using 1.20 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management
            //Warning: Node 10.27.34.12 is serving 1.11 times the token space of node 10.27.34.21, which means it will be using 1.11 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (string.IsNullOrEmpty(line)
                    || line.StartsWith("warning: ", StringComparison.OrdinalIgnoreCase)
                    || line.StartsWith("note: ", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                parsedLine = Common.StringFunctions.Split(line,
                                                            ' ',
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                if (IPAddressStr(parsedLine[0], out ipAddress))
                {
                    dataRow = dtRingInfo.Rows.Find(ipAddress);


                    if (dataRow == null)
                    {
                        ipAddress.Dump(Logger.DumpType.Warning, "IP Address was not found in the \"nodetool ring\" file but was found within the \"dsetool ring\" file. Ring information added.");

                        dataRow = dtRingInfo.NewRow();

                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Data Center"] = parsedLine[1];
                        dataRow["Rack"] = parsedLine[2];
                        dataRow["Status"] = parsedLine[4];
                        dataRow["Instance Type"] = parsedLine[3];
                        dataRow["Storage Used (MB)"] = ConvertInToMB(parsedLine[6], parsedLine[7]);
                        dataRow["Storage Utilization"] = decimal.Parse(parsedLine[8].LastIndexOf('%') >= 0
                                                                        ? parsedLine[8].Substring(0, parsedLine[8].Length - 1)
                                                                        : parsedLine[8]) / 100m;
                        dataRow["Nbr VNodes"] = int.Parse(parsedLine[9][0] == '%' ? parsedLine[10] : parsedLine[9]);

                        dtRingInfo.Rows.Add(dataRow);
                    }
                    else
                    {
                        dataRow.BeginEdit();

                        dataRow["Instance Type"] = parsedLine[3];
                        dataRow["Storage Utilization"] = decimal.Parse(parsedLine[8].LastIndexOf('%') >= 0
                                                                        ? parsedLine[8].Substring(0, parsedLine[8].Length - 1)
                                                                        : parsedLine[8]) / 100m;
                        dataRow["Storage Used (MB)"] = ConvertInToMB(parsedLine[6], parsedLine[7]);
                        dataRow["Nbr VNodes"] = int.Parse(parsedLine[9][0] == '%' ? parsedLine[10] : parsedLine[9]);

                        dataRow.EndEdit();
                        dataRow.AcceptChanges();
                    }
                }
            }


        }

    }
}
