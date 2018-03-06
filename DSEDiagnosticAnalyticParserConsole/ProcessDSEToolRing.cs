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
        static public bool ReadDSEToolRingFileParseIntoDataTable(IFilePath dseRingFilePath,
                                                                    DataTable dtRingInfo)
        {
			InitializeRingDataTable(dtRingInfo);

			var fileLines = dseRingFilePath.ReadAllLines();
            string line;
            List<string> parsedLine;
            string ipAddress;
            DataRow dataRow;
            bool dse5Format = false;
            bool vNodes = false;
            bool bResult = true;
            bool serverId = false;

            //INFO  17:16:14  Resource level latency tracking is not enabled
            // 
            //Note: Ownership information does not include topology, please specify a keyspace.            
            //Address 			DC			Rack 	Workload	Status 	State	Load 		Owns	VNodes
            //10.27.34.17 		DC1 		RAC1	Cassandra 	Up		Normal	48.36 GB	6.31 % 	256
            //Warning: Node 10.27.34.54 is serving 1.20 times the token space of node 10.27.34.52, which means it will be using 1.20 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management
            //Warning: Node 10.27.34.12 is serving 1.11 times the token space of node 10.27.34.21, which means it will be using 1.11 times more disk space and network bandwidth.If this is unintentional, check out http://wiki.apache.org/cassandra/Operations#Ring_management
            //
            //Note: Ownership information does not include topology, please specify a keyspace. 
            //Address           DC          Rack        Workload        Status  State       Load        Owns        Token
            //                                                                                                       3074457345618258602
            //10.20.70.107     Cassandra    rack1        Cassandra      Up      Normal      565.02 GB   20.00 %     -9223372036854775808
            //10.20.70.20      Analytics    rack1        Analytics(JT)  Up      Normal      540.89 GB   16.67 %     0
            //10.20.70.25      Analytics    rack1        Analytics(TT)  Up      Normal      531.24 GB   13.33 %     5534023222112865484
            //10.20.70.82      Cassandra    rack1        Cassandra      Up      Normal      564.08 GB   33.33 %     -3074457345618258603
            //10.20.70.83      Cassandra    rack1        Cassandra      Up      Normal      543.01 GB   16.67 %     3074457345618258602
            //
            // DSE 5.0 Format
            // Address          DC  Rack    Workload    Graph  Status  State    Load        Owns    VNodes  Health [0,1] 
            //10.200.178.74    dc1  rack1   Cassandra   no     Up       Normal   376.55 MB  ?       32      0.90
            //10.200.178.76    dc1  rack1   Cassandra   no     Up       Normal   406.54 MB  ?       32      0.90
            //Note: you must specify a keyspace to get ownership information.
            //
            //Server ID          Address          DC                   Rack         Workload             Graph  Status  State    Load             Owns                 VNodes                                       Health [0,1] 
            //3C-A8-2A-17-08-08  172.26.2.132     Chicago rack1        Cassandra yes    Up Normal   211.37 GB ? 32                                           0.90

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if(string.Empty == line)
                {
                    continue;
                }
                else if (line == "stderr:")
                {
                    Logger.Instance.ErrorFormat("DSETool Ring File is not valid or failed to generate properly. File \"{0}\" will be ignored", dseRingFilePath.PathResolved);
                    Program.ConsoleErrors.Increment("DSETool Ring File Invalid");
                    bResult = false;
                    break;
                }
                else if (line.StartsWith("info ", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("warning: ", StringComparison.OrdinalIgnoreCase)
                            || line.StartsWith("note: ", StringComparison.OrdinalIgnoreCase))
                {
                    continue;
                }

                parsedLine = Common.StringFunctions.Split(line,
                                                            ' ',
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                if(serverId)
                {
                    parsedLine = parsedLine.Skip(1).ToList();
                }

                if(parsedLine[0].ToLower() == "server")
                {
                    dse5Format = true;
                    vNodes = !(parsedLine.Count >= 10 && (parsedLine[10].ToLower() == "token"));
                    serverId = true;
                    continue;
                }
                else if(parsedLine[0].ToLower() == "address")
                {
                    dse5Format = parsedLine.Count >= 4 && parsedLine[4].ToLower() == "graph";
                    vNodes = !(dse5Format 
                                ? parsedLine.Count >= 10 && (parsedLine[9].ToLower() == "token")
                                : parsedLine.Count >= 9 && (parsedLine[8].ToLower() == "token"));
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
                        dtRingInfo.Rows.Add(dataRow);
                    }

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

                    if (vNodes)
                    {
                        try
                        {
                            dataRow["Nbr VNodes"] = int.Parse(parsedLine[9 + offSet][0] == '%' ? parsedLine[10 + offSet] : parsedLine[9 + offSet]);
                        }
                        catch (System.Exception e)
                        {
                            Program.ConsoleErrors.Increment("Invalid VNode Value in dsetool ring for node " + ipAddress);
                            Logger.Instance.Error("Invalid VNode Value in dsetool ring for node " + ipAddress, e);
                        }
                    }

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

            return bResult;
        }

    }
}
