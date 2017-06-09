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
        static public void ParseOSMachineInfoDataTable(IDirectoryPath directoryPath,
                                                        string[] osmachineFiles,
                                                        string ipAddress,
                                                        string dcName,
                                                        DataTable dtOSMachineInfo)
        {
            lock (dtOSMachineInfo)
            {
                if (dtOSMachineInfo.Columns.Count == 0)
                {
                    dtOSMachineInfo.Columns.Add("Node IPAddress", typeof(string)).Unique = true;
                    dtOSMachineInfo.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                    dtOSMachineInfo.PrimaryKey = new System.Data.DataColumn[] { dtOSMachineInfo.Columns["Node IPAddress"] };

                    dtOSMachineInfo.Columns.Add("Instance Type", typeof(string)).AllowDBNull = true;//c
                    dtOSMachineInfo.Columns.Add("CPU Architecture", typeof(string));
                    dtOSMachineInfo.Columns.Add("Cores", typeof(int)).AllowDBNull = true; //e
                    dtOSMachineInfo.Columns.Add("Physical Memory (MB)", typeof(int)); //f
                    dtOSMachineInfo.Columns.Add("OS", typeof(string));
                    dtOSMachineInfo.Columns.Add("OS Version", typeof(string));
                    dtOSMachineInfo.Columns.Add("TimeZone", typeof(string));
                    //CPU Load
                    dtOSMachineInfo.Columns.Add("Average", typeof(decimal)); //j
                    dtOSMachineInfo.Columns.Add("Idle", typeof(decimal));
                    dtOSMachineInfo.Columns.Add("System", typeof(decimal));
                    dtOSMachineInfo.Columns.Add("User", typeof(decimal)); //m
                                                                          //Memory
                    dtOSMachineInfo.Columns.Add("Available", typeof(int)); //n
                    dtOSMachineInfo.Columns.Add("Cache", typeof(int));
                    dtOSMachineInfo.Columns.Add("Buffers", typeof(int));
                    dtOSMachineInfo.Columns.Add("Shared", typeof(int));
                    dtOSMachineInfo.Columns.Add("Free", typeof(int));
                    dtOSMachineInfo.Columns.Add("Used", typeof(int)); //s
                                                                      //Java
                    dtOSMachineInfo.Columns.Add("Vendor", typeof(string));//t
                    dtOSMachineInfo.Columns.Add("Model", typeof(string));
                    dtOSMachineInfo.Columns.Add("Runtime Name", typeof(string));
                    dtOSMachineInfo.Columns.Add("Runtime Version", typeof(string));//w
                    dtOSMachineInfo.Columns.Add("GC", typeof(string)).AllowDBNull = true;
                    //Java NonHeapMemoryUsage
                    dtOSMachineInfo.Columns.Add("Non-Heap Committed", typeof(decimal)); //y
                    dtOSMachineInfo.Columns.Add("Non-Heap Init", typeof(decimal));
                    dtOSMachineInfo.Columns.Add("Non-Heap Max", typeof(decimal));//aa
                    dtOSMachineInfo.Columns.Add("Non-Heap Used", typeof(decimal));//ab
                                                                                  //Javaa HeapMemoryUsage
                    dtOSMachineInfo.Columns.Add("Heap Committed", typeof(decimal)); //ac
                    dtOSMachineInfo.Columns.Add("Heap Init", typeof(decimal)); //ad
                    dtOSMachineInfo.Columns.Add("Heap Max", typeof(decimal)); //ae
                    dtOSMachineInfo.Columns.Add("Heap Used", typeof(decimal)); //af

                    //DataStax Versions
                    dtOSMachineInfo.Columns.Add("DSE", typeof(string)).AllowDBNull = true; //ag
                    dtOSMachineInfo.Columns.Add("Cassandra", typeof(string)).AllowDBNull = true;
                    dtOSMachineInfo.Columns.Add("Search", typeof(string)).AllowDBNull = true;
                    dtOSMachineInfo.Columns.Add("Spark", typeof(string)).AllowDBNull = true;//aj
                    dtOSMachineInfo.Columns.Add("Agent", typeof(string)).AllowDBNull = true; //ak
                    dtOSMachineInfo.Columns.Add("VNodes", typeof(bool)).AllowDBNull = true; //al

                    //NTP
                    dtOSMachineInfo.Columns.Add("Correction (ms)", typeof(int)); //am
                    dtOSMachineInfo.Columns.Add("Polling (secs)", typeof(int));
                    dtOSMachineInfo.Columns.Add("Maximum Error (us)", typeof(int));
                    dtOSMachineInfo.Columns.Add("Estimated Error (us)", typeof(int));
                    dtOSMachineInfo.Columns.Add("Time Constant", typeof(int)); //aq
                    dtOSMachineInfo.Columns.Add("Precision (us)", typeof(decimal)); //ar
                    dtOSMachineInfo.Columns.Add("Frequency (ppm)", typeof(decimal));
                    dtOSMachineInfo.Columns.Add("Tolerance (ppm)", typeof(decimal)); //at
                }
            }

            DataRow dataRow;
            IFilePath filePath = null;

            try
            {
                lock (dtOSMachineInfo)
                {
                    dataRow = dtOSMachineInfo.NewRow();
                }

                dataRow["Node IPAddress"] = ipAddress;
                dataRow["Data Center"] = dcName;

                foreach (var fileName in osmachineFiles)
                {

                    if (directoryPath.Clone().MakeFile(fileName, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(filePath);

                            if (fileName.Contains("machine-info"))
                            {
                                var infoObject = ParseJson(filePath.ReadAllText());

                                dataRow["CPU Architecture"] = infoObject["arch"];
                                dataRow["Physical Memory (MB)"] = infoObject["memory"];
                            }
                            else if (fileName.Contains("os-info"))
                            {
                                var infoObject = ParseJson(filePath.ReadAllText());
                                
                                dataRow["OS"] = infoObject.TryGetValue("sub_os");
                                dataRow["OS Version"] = infoObject["os_version"];
                            }
                            else if (fileName.Contains("cpu"))
                            {
                                var infoObject = ParseJson(filePath.ReadAllText());

                                dataRow["Idle"] = infoObject["%idle"];
                                dataRow["System"] = infoObject["%system"];
                                dataRow["User"] = infoObject["%user"];
                            }
                            else if (fileName.Contains("load_avg"))
                            {
                                dataRow["Average"] = decimal.Parse(filePath.ReadAllText());
                            }
                            else if (fileName.Contains("agent_version"))
                            {
                                dataRow["Agent"] = filePath.ReadAllText();
                            }
                            else if (fileName.Contains("memory"))
                            {
                                var infoObject = ParseJson(filePath.ReadAllText());

                                if (infoObject.ContainsKey("available")) dataRow["Available"] = infoObject["available"];
                                if (infoObject.ContainsKey("cache")) dataRow["Cache"] = infoObject["cache"];
                                if (infoObject.ContainsKey("cached")) dataRow["Cache"] = infoObject["cached"];
                                dataRow["Buffers"] = infoObject["buffers"];
                                dataRow["Shared"] = infoObject["shared"];
                                dataRow["Free"] = infoObject["free"];
                                dataRow["Used"] = infoObject["used"];
                            }
                            else if (fileName.Contains("java_system_properties"))
                            {
                                var infoObject = ParseJson(filePath.ReadAllText());

                                dataRow["Vendor"] = infoObject["java.vendor"];
                                dataRow["Model"] = infoObject["sun.arch.data.model"];
                                dataRow["Runtime Name"] = infoObject["java.runtime.name"];
                                dataRow["Runtime Version"] = infoObject["java.runtime.version"];
                                dataRow["TimeZone"] = ((string)infoObject["user.timezone"])
                                                        .Replace((string)infoObject["file.separator"], "/");

                                if (infoObject.ContainsKey("dse.system_cpu_cores"))
                                {
                                    dataRow["Cores"] = infoObject["dse.system_cpu_cores"];
                                }
                            }
                            else if (fileName.Contains("java_heap"))
                            {
                                var infoObject = ParseJson(filePath.ReadAllText());
                                var nonHeapJson = (Dictionary<string, object>)infoObject["NonHeapMemoryUsage"];
                                var heapJson = (Dictionary<string, object>)infoObject["HeapMemoryUsage"];

                                //Java NonHeapMemoryUsage
                                dataRow["Non-Heap Committed"] = ((dynamic)(nonHeapJson["committed"])) / BytesToMB;
                                dataRow["Non-Heap Init"] = ((dynamic)(nonHeapJson["init"])) / BytesToMB;
                                dataRow["Non-Heap Max"] = ((dynamic)(nonHeapJson["max"])) / BytesToMB;
                                dataRow["Non-Heap Used"] = ((dynamic)(nonHeapJson["used"])) / BytesToMB;
                                //Javaa HeapMemoryUsage
                                dataRow["Heap Committed"] = ((dynamic)(heapJson["committed"])) / BytesToMB;
                                dataRow["Heap Init"] = ((dynamic)(heapJson["init"])) / BytesToMB;
                                dataRow["Heap Max"] = ((dynamic)(heapJson["max"])) / BytesToMB;
                                dataRow["Heap Used"] = ((dynamic)(heapJson["used"])) / BytesToMB;
                            }
                            else if (fileName.Contains("ntpstat"))
                            {
                                var fileText = filePath.ReadAllText();
                                var words = StringFunctions.Split(fileText,
                                                                    ' ',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                    StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                                for (int nIndex = 0; nIndex < words.Count; ++nIndex)
                                {
                                    var element = words[nIndex];

                                    if (element == "within")
                                    {
                                        dataRow["Correction (ms)"] = DetermineTime(words[++nIndex]);
                                    }
                                    else if (element == "every")
                                    {
                                        dataRow["Polling (secs)"] = DetermineTime(words[++nIndex]);
                                    }
                                }
                            }
                            else if (fileName.Contains("ntptime"))
                            {
                                var fileText = filePath.ReadAllText();
                                var words = StringFunctions.Split(fileText,
                                                                    ' ',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                    StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                                for (int nIndex = 0; nIndex < words.Count; ++nIndex)
                                {
                                    var element = words[nIndex];

                                    if (element == "maximum")
                                    {
                                        dataRow["Maximum Error (us)"] = DetermineTime(words[nIndex += 2]);
                                    }
                                    else if (element == "estimated")
                                    {
                                        dataRow["Estimated Error (us)"] = DetermineTime(words[nIndex += 2]);
                                    }
                                    else if (element == "constant")
                                    {
                                        dataRow["Time Constant"] = DetermineTime(words[++nIndex]);
                                    }
                                    else if (element == "precision")
                                    {
                                        dataRow["Precision (us)"] = DetermineTime(words[++nIndex]);
                                    }
                                    else if (element == "frequency")
                                    {
                                        dataRow["Frequency (ppm)"] = DetermineTime(words[++nIndex]);
                                    }
                                    else if (element == "tolerance")
                                    {
                                        dataRow["Tolerance (ppm)"] = DetermineTime(words[++nIndex]);
                                    }
                                }
                            }

                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);
                        }
                    }
                }

                lock (dtOSMachineInfo)
                {
                    dtOSMachineInfo.Rows.Add(dataRow);
                }
            }
            catch (System.Exception e)
            {
                Logger.Instance.ErrorFormat("Error: Exception \"{0}\" ({1}) occurred while parsing file \"{2}\" within ParseOSMachineInfoDataTable for IpAddress: {3} ({4})",
                                                e.Message,
                                                e.GetType().Name,
                                                filePath?.PathResolved,
                                                ipAddress,
                                                dcName);
                Logger.Instance.Error("Parsing OS Machine Info", e);
            }
        }

        static public void UpdateMachineInfo(DataTable dtOSMachineInfo,
                                                Common.Patterns.Collections.ThreadSafe.Dictionary<string, string> dictGCIno)
        {
            if (dtOSMachineInfo.Rows.Count == 0 || dictGCIno.IsEmpty())
            {
                return;
            }

            foreach (DataRow drMachineInfo in dtOSMachineInfo.Rows)
            {
                //dcName == null ? string.Empty : dcName) + "|" + ipAddress
                var dcName = drMachineInfo["Data Center"] as string;
                var ipAddress = drMachineInfo["Node IPAddress"] as string;
                string gcValue;

                if (dictGCIno.TryGetValue((dcName == null ? string.Empty : dcName) + "|" + ipAddress, out gcValue))
                {
                    drMachineInfo["GC"] = gcValue;
                }
            }
        }

    }
}
