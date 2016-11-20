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
            bool dse5Format = false;

            //Note: Ownership information does not include topology, please specify a keyspace.
            //Address 			DC			Rack 	Workload	Status 	State	Load 		Owns	VNodes
            //10.27.34.17 		DC1 		RAC1	Cassandra 	Up		Normal	48.36 GB	6.31 % 	256
            //Warning: Node 10.27.34.54 is serving 1.20 times the token space of node 10.27.34.52, which means it will be using 1.20 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management
            //Warning: Node 10.27.34.12 is serving 1.11 times the token space of node 10.27.34.21, which means it will be using 1.11 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management
            //
            // DSE 5.0
            // Address          DC  Rack    Workload    Graph  Status  State    Load        Owns    VNodes  Health [0,1] 
            //10.200.178.74    dc1  rack1   Cassandra   no     Up       Normal   376.55 MB  ?       32      0.90
            //10.200.178.76    dc1  rack1   Cassandra   no     Up       Normal   406.54 MB  ?       32      0.90
            //Note: you must specify a keyspace to get ownership information.

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

                if(parsedLine[0].ToLower() == "address")
                {
                    dse5Format = parsedLine.Count >= 4 && parsedLine[4].ToLower() == "graph";
                    continue;
                }
                else if (IPAddressStr(parsedLine[0], out ipAddress))
                {
                    int offSet = dse5Format ? 1 : 0;
                    
                    dataRow = dtRingInfo.Rows.Find(ipAddress);


                    if (dataRow == null)
                    {
                        ipAddress.Dump(Logger.DumpType.Warning, "IP Address was not found in the \"nodetool ring\" file but was found within the \"dsetool ring\" file. Ring information added.");
                        Program.ConsoleWarnings.Increment("IP Address found in dsetool but not in nodetool: " + ipAddress);

                        dataRow = dtRingInfo.NewRow();

                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Data Center"] = parsedLine[1];
                        dataRow["Rack"] = parsedLine[2];
                        dataRow["Status"] = parsedLine[4];

                        if (dse5Format && parsedLine[4].ToLower() == "yes")
                        {
                            dataRow["Instance Type"] = parsedLine[3] + "-Graph";
                        }
                        else
                        {
                            dataRow["Instance Type"] = parsedLine[3];
                        }
                        dataRow["Storage Used (MB)"] = ConvertInToMB(parsedLine[6 + offSet], parsedLine[7 + offSet]);

                        if (parsedLine[8 + offSet] != "?")
                        {
                            dataRow["Storage Utilization"] = decimal.Parse(parsedLine[8 + offSet].LastIndexOf('%') >= 0
                                                                            ? parsedLine[8 + offSet].Substring(0, parsedLine[8 + offSet].Length - 1)
                                                                            : parsedLine[8 + offSet])
                                                                / 100m;
                        }
                        dataRow["Nbr VNodes"] = int.Parse(parsedLine[9 + offSet][0] == '%' ? parsedLine[10 + offSet] : parsedLine[9 + offSet]);

                        if(dse5Format)
                        {
                            decimal healthRating;

                            if (decimal.TryParse(parsedLine[0], out healthRating))
                            {
                                dataRow["Health Rating"] = healthRating;
                            }
                        }

                        dtRingInfo.Rows.Add(dataRow);
                    }
                    else
                    {
                        dataRow.BeginEdit();

                        if (dse5Format && parsedLine[4].ToLower() == "yes")
                        {
                            dataRow["Instance Type"] = parsedLine[3] + "-Graph";
                        }
                        else
                        {
                            dataRow["Instance Type"] = parsedLine[3];
                        }
                        dataRow["Storage Used (MB)"] = ConvertInToMB(parsedLine[6 + offSet], parsedLine[7 + offSet]);

                        if (parsedLine[8 + offSet] != "?")
                        {
                            dataRow["Storage Utilization"] = decimal.Parse(parsedLine[8 + offSet].LastIndexOf('%') >= 0
                                                                            ? parsedLine[8 + offSet].Substring(0, parsedLine[8 + offSet].Length - 1)
                                                                            : parsedLine[8 + offSet])
                                                                / 100m;
                        }
                        dataRow["Nbr VNodes"] = int.Parse(parsedLine[9 + offSet][0] == '%' ? parsedLine[10 + offSet] : parsedLine[9 + offSet]);

                        if (dse5Format)
                        {
                            decimal healthRating;

                            if (decimal.TryParse(parsedLine[0], out healthRating))
                            {
                                dataRow["Health Rating"] = healthRating;
                            }
                        }

                        dataRow.EndEdit();
                        dataRow.AcceptChanges();
                    }
                }
            }


        }

    }
}
