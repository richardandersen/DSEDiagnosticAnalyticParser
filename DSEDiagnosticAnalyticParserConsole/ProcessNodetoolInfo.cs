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
                        dataRow["Gossip Enableed"] = bool.Parse(lineValue);
                        break;
                    case "thrift active":
                        dataRow["Thrift Enabled"] = bool.Parse(lineValue);
                        break;
                    case "native transport active":
                        dataRow["Native Transport Enable"] = bool.Parse(lineValue);
                        break;
                    case "load":
                        dataRow["Storage Used (MB)"] = ConvertInToMB(lineValue);
                        break;
                    case "generation no":
                        //dataRow["Number of Restarts"] = int.Parse(lineValue);
                        break;
                    case "uptime (seconds)":
                        dataRow["Uptime"] = new TimeSpan(0, 0, int.Parse(lineValue));
                        break;
                    case "heap memory (mb)":
                        dataRow["Heap Memory (MB)"] = lineValue;
                        break;
                    case "off heap memory (mb)":
                        dataRow["Off Heap Memory (MB)"] = decimal.Parse(lineValue);
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
                        break;
                }
            }

            dataRow.EndEdit();
            //dataRow.AcceptChanges();
        }

    }
}
