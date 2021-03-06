﻿using System;
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
        static public void ReadInfoFileParseIntoDataTable(IFilePath infoFilePath,
                                                            string ipAddress,
                                                            string dcName,
                                                            DataTable dtRingInfo)
        {
            var fileLines = infoFilePath.ReadAllLines();
            string line;
            DataRow dataRow;

            lock (dtRingInfo)
            {
                dataRow = dtRingInfo.Rows.Find(ipAddress);
            }

            if (dataRow == null)
            {
                ipAddress.Dump(Logger.DumpType.Warning, "IP Address was not found in the \"nodetool ring\" file but was found within the \"nodetool info\" file.");
                Program.ConsoleWarnings.Increment("IP Address found in dsetool but not in nodetool: " + ipAddress);
                return;
            }

            string lineCommand;
            string lineValue;
            int delimitorPos;

            dataRow.BeginEdit();

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (string.IsNullOrEmpty(line))
                {
                    continue;
                }

                delimitorPos = line.IndexOf(':');

                if (delimitorPos <= 0)
                {
                    continue;
                }

                lineCommand = line.Substring(0, delimitorPos).Trim().ToLower();
                lineValue = line.Substring(delimitorPos + 1).Trim();

                switch (lineCommand)
                {
                    case "gossip active":
                        dataRow["Gossip Enabled"] = bool.Parse(lineValue);
                        break;
                    case "thrift active":
                        dataRow["Thrift Enabled"] = bool.Parse(lineValue);
                        break;
                    case "native transport active":
                        dataRow["Native Transport Enabled"] = bool.Parse(lineValue);
                        break;
                    case "load":
                        dataRow["Storage Used (MB)"] = ConvertInToMB(lineValue);
                        break;
                    case "generation no":
                        //dataRow["Number of Restarts"] = int.Parse(lineValue);
                        break;
                    case "uptime (seconds)":
                        {
                            var tsUptime = TimeSpan.FromSeconds(double.Parse(lineValue));
                            dataRow["Uptime"] = tsUptime.ToString(@"d\ hh\:mm");
                            dataRow["Uptime (Days)"] = tsUptime;
                        }
                        break;
                    case "heap memory (mb)":
                        dataRow["Heap Memory (MB)"] = lineValue;
                        break;
                    case "off heap memory (mb)":
                        dataRow["Off Heap Memory (MB)"] = decimal.Parse(lineValue);
                        break;                    
                    case "percent repaired":
                        var decValue = lineValue.Last() == '%'
                                        ? lineValue.Substring(0,lineValue.Length - 1).Trim()
                                        : lineValue;
                        decimal dec;

                        if(decimal.TryParse(decValue,out dec))
                        {
                            dataRow["Percent Repaired"] = dec / 100m;
                        }
                        break;
                    case "id":
                    case "token":
                    case "datacenter":
                    case "data center":
                    case "rack":
                        break;
                    case "exceptions":
                        dataRow["Nbr of Exceptions"] = int.Parse(lineValue);
                        break;
                    case "key cache":
                        dataRow["Key Cache Information"] = lineValue;
                        break;
                    case "row cache":
                        dataRow["Row Cache Information"] = lineValue;
                        break;
                    case "counter cache":
                        dataRow["Counter Cache Information"] = lineValue;
                        break;
                    default:
                        line.Dump(Logger.DumpType.Warning, "\"nodetool info\" Invalid line found.");
                        Program.ConsoleWarnings.Increment("nodetool info invalid line", line);
                        break;
                }
            }

            dataRow.EndEdit();
            //dataRow.AcceptChanges();
        }

    }
}
