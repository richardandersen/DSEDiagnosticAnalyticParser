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
        static public void ParseOPSCenterInfoDataTable(IDirectoryPath directoryPath,
                                                        string[] ospCenterFiles,
                                                        DataTable dtOSMachineInfo,
                                                        DataTable dtRingInfo)
        {
            if (dtOSMachineInfo.Rows.Count <= 0)
            {
                return;
            }

            foreach (var fileName in ospCenterFiles)
            {
                IFilePath filePath;

                if (directoryPath.Clone().MakeFile(fileName, out filePath))
                {
                    if (filePath.Exist())
                    {
                        if (fileName.Contains("node_info"))
                        {
                            var infoObject = ParseJson(filePath.ReadAllText());
                            var nodeInfoDict = (Dictionary<string, object>)infoObject;

                            foreach (DataRow dataRow in dtOSMachineInfo.Rows)
                            {
                                if (nodeInfoDict.ContainsKey((string)dataRow["Node IPAddress"]))
                                {
                                    var nodeInfo = (Dictionary<string, object>)nodeInfoDict[(string)dataRow["Node IPAddress"]];
                                    var dseVersions = (Dictionary<string, object>)nodeInfo["node_version"];

                                    dataRow.BeginEdit();

                                    if (nodeInfo.ContainsKey("ec2"))
                                    {
                                        dataRow["Instance Type"] = ((Dictionary<string, object>)nodeInfo["ec2"])["instance-type"];
                                    }

                                    if (dataRow["Cores"] == DBNull.Value)
                                    {
                                        dataRow["Cores"] = nodeInfo["num_procs"];
                                    }
                                    dataRow["DSE"] = dseVersions["dse"];
                                    dataRow["Cassandra"] = dseVersions["cassandra"];
                                    dataRow["Search"] = dseVersions["search"];
                                    dataRow["Spark"] = ((Dictionary<string, object>)dseVersions["spark"])["version"];
                                    dataRow["VNodes"] = nodeInfo["vnodes"];

                                    dataRow.EndEdit();
                                }
                            }
                        }
                        else if (fileName.Contains("repair_service"))
                        {
                            var infoObject = ParseJson(filePath.ReadAllText());
                            var nodeInfoDict = (Dictionary<string, object>)infoObject;

                            foreach (DataRow dataRow in dtRingInfo.Rows)
                            {
                                if (nodeInfoDict.ContainsKey("all_tasks")
                                        && ((object[])nodeInfoDict["all_tasks"]).Any(c => (string)((object[])c)[0] == (string)dataRow["Node IPAddress"]))
                                {
                                    dataRow["Read-Repair Service Enabled"] = true;
                                }
                            }
                        }
                    }
                }
            }
        }
    }
}
