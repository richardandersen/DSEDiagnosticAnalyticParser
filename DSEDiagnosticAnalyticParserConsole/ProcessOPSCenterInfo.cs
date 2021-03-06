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
                                    try
                                    {
                                        var nodeInfo = (Dictionary<string, object>)nodeInfoDict.TryGetValue((string)dataRow["Node IPAddress"]);
                                        var dseVersions = (Dictionary<string, object>)nodeInfo.TryGetValue("node_version");

                                        dataRow.BeginEdit();

                                        if (nodeInfo.ContainsKey("ec2"))
                                        {
                                            dataRow["Instance Type"] = ((Dictionary<string, object>)nodeInfo.TryGetValue("ec2")).TryGetValue("instance-type");
                                        }

                                        if (dataRow["Cores"] == DBNull.Value
                                                && nodeInfo.ContainsKey("num_procs"))
                                        {
                                            var dataValue = nodeInfo["num_procs"];
                                            dataRow["Cores"] = dataValue == null ? DBNull.Value : dataValue;
                                        }
                                        dataRow["DSE"] = dseVersions.TryGetValue("dse");
                                        dataRow["Cassandra"] = dseVersions.TryGetValue("cassandra");
                                        dataRow["Search"] = dseVersions.TryGetValue("search");

                                        dataRow["Spark"] = ((Dictionary<string, object>)dseVersions.TryGetValue("spark")).TryGetValue("version");

                                        if (nodeInfo.ContainsKey("vnodes"))
                                        {                                            
                                            var dataValue = nodeInfo["vnodes"];
                                            dataRow["VNodes"] = dataValue == null ? DBNull.Value : dataValue;
                                        }

                                        dataRow.EndEdit();
                                    }
                                    catch(System.Exception e)
                                    {                                        
                                        Logger.Instance.Error(string.Format("Error in OpsCenter's node_info file for node {0}.",
                                                                                (string)dataRow["Node IPAddress"]),
                                                                e);
                                    }
                                }
                                else
                                {
                                    Logger.Instance.WarnFormat("Node {0} is missing from the OpsCenter's node_info file.", (string)dataRow["Node IPAddress"]);
                                }
                            }
                        }
                        else if (fileName.Contains("repair_service"))
                        {
                            var infoText = filePath.ReadAllText();
                            var definition = new { time_to_completion = 0L, status = string.Empty, parallel_tasks = 0m, all_tasks = new object[1][] };
                            var infoObject = Newtonsoft.Json.JsonConvert.DeserializeAnonymousType(infoText, definition);

                            foreach (DataRow dataRow in dtRingInfo.Rows)
                            {
                                if (infoObject.all_tasks != null
                                        && infoObject.all_tasks.Any((c => (string)((object[])c)[0] == (string)dataRow["Node IPAddress"])))
                                {
                                    dataRow["Repair Service Enabled"] = true;
                                }
                            }                          
                        }
                    }
                }
            }
        }
    }
}
