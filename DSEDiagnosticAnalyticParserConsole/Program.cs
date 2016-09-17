using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;

namespace DSEDiagnosticAnalyticParserConsole
{
    class Program
    {
        public static readonly DateTime RunDateTime = DateTime.Now;
        public static string CommandArgsString = null;

        static public ConsoleDisplay ConsoleNonLogFiles = null;
        static public ConsoleDisplay ConsoleLogFiles = null;
        static public ConsoleDisplay ConsoleParsingNonLog = null;
        static public ConsoleDisplay ConsoleParsingLog = null;
        static public ConsoleDisplay ConsoleExcelNonLog = null;
        static public ConsoleDisplay ConsoleExcelLog = null;
        static public ConsoleDisplay ConsoleExcelLogStatus = null;
        static public ConsoleDisplay ConsoleExcel = null;
        static public ConsoleDisplay ConsoleExcelWorkbook = null;
        static public ConsoleDisplay ConsoleWarnings = null;
        static public ConsoleDisplay ConsoleErrors = null;

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            var argResult = CommandLine.Parser.Default.ParseArguments<ConsoleArguments>(args);

            if (argResult.Value.DisplayDefaults)
            {
                Console.WriteLine(argResult.Value.ToString());
                return;
            }

            if (!argResult.Errors.IsEmpty())
            {
                return;
            }

            Logger.Instance.InfoFormat("Program: {0} Version: {1} Directory: {2}",
                                            Common.Functions.Instance.ApplicationName,
                                            Common.Functions.Instance.ApplicationVersion,
                                            Common.Functions.Instance.AssemblyDir);

            Logger.Instance.Info("Starting with " + (CommandArgsString = argResult.Value.ToString()));

            if(!argResult.Value.CheckArguments())
            {
                return;
            }

            ConsoleDisplay.Console.ClearScreen();

            Console.WriteLine(" ");

            ConsoleNonLogFiles = new ConsoleDisplay("Non-Log Files: {0} Count: {1} Task: {2}");
            ConsoleLogFiles = new ConsoleDisplay("Log Files: {0}  Count: {1} Task: {2}");
            ConsoleParsingNonLog = new ConsoleDisplay("Non-Log Processing: {0}  Count: {1} Task: {2}");
            ConsoleParsingLog = new ConsoleDisplay("Log Processing: {0}  Count: {1} Task: {2}");
            ConsoleExcel = new ConsoleDisplay("Excel: {0}  Count: {1} WorkSheet: {2}");
            ConsoleExcelNonLog = new ConsoleDisplay("Excel Non-Log: {0}  Count: {1} Task: {2}");
            ConsoleExcelLog = new ConsoleDisplay("Excel Log: {0}  Count: {1} Task: {2}");
            ConsoleExcelLogStatus = new ConsoleDisplay("Excel Status Log: {0}  Count: {1} Task: {2}");
            ConsoleExcelWorkbook = new ConsoleDisplay("Excel Workbooks: {0} File: {2}");
            ConsoleWarnings = new ConsoleDisplay("Warnings: {0} Last: {2}", 2, false);
            ConsoleErrors = new ConsoleDisplay("Errors: {0} Last: {2}", 2, false);

            #region Local Variables

            //Local Variables used for processing
            bool opsCtrDiag = false;
            var dtRingInfo = new System.Data.DataTable(ParserSettings.ExcelWorkSheetRingInfo);
            var dtTokenRange = new System.Data.DataTable(ParserSettings.ExcelWorkSheetRingTokenRanges);
            var dtKeySpace = new System.Data.DataTable(ParserSettings.ExcelWorkSheetDDLKeyspaces);
            var dtTable = new System.Data.DataTable(ParserSettings.ExcelWorkSheetDDLTables);
            var cqlHashCheck = new Dictionary<string, int>();
            var dtCFStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtNodeStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtLogsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtLogStatusStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtCompHistStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var listCYamlStack = new Common.Patterns.Collections.LockFree.Stack<List<YamlInfo>>();
            var dtYaml = new System.Data.DataTable(ParserSettings.ExcelWorkSheetYaml);
            var dtOSMachineInfo = new System.Data.DataTable(ParserSettings.ExcelWorkSheetOSMachineInfo);
            var nodeGCInfo = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, string>();
            var maxminMaxLogDate = new DateTimeRange();

            var includeLogEntriesAfterThisTimeFrame = ParserSettings.LogCurrentDate == DateTime.MinValue ? DateTime.MinValue : ParserSettings.LogCurrentDate - ParserSettings.LogTimeSpanRange;

            #endregion

            ConsoleDisplay.Start();

            #region Parsing Files

            if (includeLogEntriesAfterThisTimeFrame != DateTime.MinValue)
            {
                Logger.Instance.InfoFormat("Log Entries after \"{0}\" will only be parsed", includeLogEntriesAfterThisTimeFrame);
            }
            else
            {
                Logger.Instance.Info("All Log Entries will be parsed!");
            }

            var diagPath = Common.Path.PathUtils.BuildDirectoryPath(ParserSettings.DiagnosticPath);
            var logParsingTasks = new Common.Patterns.Collections.ThreadSafe.List<Task>();
            var kstblNames = new List<CKeySpaceTableNames>();

            if (ParserSettings.DiagnosticNoSubFolders)
            {
                #region Parse -- All Files in one Folder

                var diagChildren = diagPath.Children();

                //Need to process nodetool ring files first
                var nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(ParserSettings.NodetoolRingFile));

                if (ParserSettings.ParseNonLogs && nodetoolRingChildFiles.HasAtLeastOneElement())
                {
                    foreach (var element in nodetoolRingChildFiles)
                    {
                        Program.ConsoleNonLogFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadRingFileParseIntoDataTables((IFilePath)element, dtRingInfo, dtTokenRange);

                        Program.ConsoleNonLogFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(ParserSettings.DSEToolDir + "_" + ParserSettings.DSEtoolRingFile));

                if (ParserSettings.ParseNonLogs && nodetoolRingChildFiles.HasAtLeastOneElement())
                {
                    foreach (var element in nodetoolRingChildFiles)
                    {
                        Program.ConsoleNonLogFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadDSEToolRingFileParseIntoDataTable((IFilePath)element, dtRingInfo);

                        Program.ConsoleNonLogFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                IFilePath cqlFilePath;

                if (ParserSettings.ParseNonLogs && diagPath.MakeFile(ParserSettings.CQLDDLDirFileExt, out cqlFilePath))
                {
                    foreach (IFilePath element in cqlFilePath.GetWildCardMatches())
                    {
                        Program.ConsoleNonLogFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadCQLDDLParseIntoDataTable(element,
                                                                        null,
                                                                        null,
                                                                        dtKeySpace,
                                                                        dtTable,
                                                                        cqlHashCheck,
                                                                        ParserSettings.IgnoreKeySpaces);

                        foreach (DataRow dataRow in dtTable.Rows)
                        {
                            if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.TableName == (dataRow["Name"] as string)))
                            {
                                kstblNames.Add(new CKeySpaceTableNames(dataRow));
                            }
                        }

                        Program.ConsoleNonLogFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                #region alternative file paths (DDL)

                if (!string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeDDLFilePath);
                    List<IFilePath> alterFiles = null;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else
                    {
                        alterFiles = new List<IFilePath>() { (IFilePath)alterPath };
                    }

                    foreach (IFilePath element in alterFiles)
                    {
                        Program.ConsoleNonLogFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadCQLDDLParseIntoDataTable(element,
                                                                        null,
                                                                        null,
                                                                        dtKeySpace,
                                                                        dtTable,
                                                                        cqlHashCheck,
                                                                        ParserSettings.IgnoreKeySpaces);

                        foreach (DataRow dataRow in dtTable.Rows)
                        {
                            if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.TableName == (dataRow["Name"] as string)))
                            {
                                kstblNames.Add(new CKeySpaceTableNames(dataRow));
                            }
                        }

                        Program.ConsoleNonLogFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                #endregion

                Parallel.ForEach(diagChildren, (diagFile) =>
                //foreach (var diagFile in diagChildren)
                {
                    if (diagFile is IFilePath && !diagFile.IsEmpty)
                    {
                        string ipAddress;
                        string dcName;

                        if (ProcessFileTasks.DetermineIPDCFromFileName(((IFilePath)diagFile).FileName, dtRingInfo, out ipAddress, out dcName))
                        {
                            if (ParserSettings.ParseNonLogs && diagFile.Name.Contains(ParserSettings.NodetoolCFStatsFile))
                            {
                                if (ParserSettings.ParseNonLogs && string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                Program.ConsoleNonLogFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                var dtCFStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCFStats + "-" + ipAddress);
                                dtCFStatsStack.Push(dtCFStats);
                                ProcessFileTasks.ReadCFStatsFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtCFStats, ParserSettings.IgnoreKeySpaces, ParserSettings.CFStatsCreateMBColumns);

                                if (kstblNames.Count == 0)
                                {
                                    //We need to have a list of valid Keyspaces and Tables...
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "DDL was not found, parsing a TPStats file to obtain data model information");
                                    Program.ConsoleWarnings.Increment("DDL Not Found");
                                    ProcessFileTasks.ReadCFStatsFileForKeyspaceTableInfo((IFilePath)diagFile, ParserSettings.IgnoreKeySpaces, kstblNames);
                                }
                                Program.ConsoleNonLogFiles.TaskEnd((IFilePath)diagFile);
                            }
                            else if (ParserSettings.ParseNonLogs && diagFile.Name.Contains(ParserSettings.NodetoolTPStatsFile))
                            {
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                Program.ConsoleNonLogFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                var dtTPStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetNodeStats + "-" + ipAddress);
                                dtNodeStatsStack.Push(dtTPStats);
                                ProcessFileTasks.ReadTPStatsFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtTPStats);
                                Program.ConsoleNonLogFiles.TaskEnd((IFilePath)diagFile);
                            }
                            else if (ParserSettings.ParseNonLogs && diagFile.Name.Contains(ParserSettings.NodetoolInfoFile))
                            {
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }
                                Program.ConsoleNonLogFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                ProcessFileTasks.ReadInfoFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtRingInfo);
                                Program.ConsoleNonLogFiles.TaskEnd((IFilePath)diagFile);
                            }
                            else if (ParserSettings.ParseNonLogs && diagFile.Name.Contains(ParserSettings.NodetoolCompactionHistFile))
                            {
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                Program.ConsoleNonLogFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                var dtCompHist = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCompactionHist + "-" + ipAddress);
                                dtCompHistStack.Push(dtCompHist);
                                ProcessFileTasks.ReadCompactionHistFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtCompHist, dtTable, ParserSettings.IgnoreKeySpaces, kstblNames);
                                Program.ConsoleNonLogFiles.TaskEnd((IFilePath)diagFile);
                            }
                            else if (ParserSettings.ParseLogs && diagFile.Name.Contains(ParserSettings.LogCassandraSystemLogFile))
                            {
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                logParsingTasks.Add(ProcessFileTasks.ProcessLogFileTasks((IFilePath)diagFile,
                                                                                            ParserSettings.ExcelWorkSheetLogCassandra,
                                                                                            dcName,
                                                                                            ipAddress,
                                                                                            includeLogEntriesAfterThisTimeFrame,
                                                                                            maxminMaxLogDate,
                                                                                            -1,
                                                                                            dtLogsStack,
                                                                                            null,
                                                                                            ParserSettings.ParseNonLogs,
                                                                                            ParserSettings.ExcelWorkSheetStatusLogCassandra,
                                                                                            nodeGCInfo,
                                                                                            ParserSettings.IgnoreKeySpaces,
                                                                                            kstblNames,
                                                                                            dtLogStatusStack,
                                                                                            dtCFStatsStack,
                                                                                            dtNodeStatsStack,
                                                                                            ParserSettings.GCPausedFlagThresholdInMS,
                                                                                            ParserSettings.CompactionFlagThresholdInMS,
                                                                                            ParserSettings.SlowLogQueryThresholdInMS));
                            }
                        }
                        else if (((IFilePath)diagFile).FileExtension.ToLower() != ".cql")
                        {
                            diagFile.Path.Dump(Logger.DumpType.Error, "File was Skipped");
                            Program.ConsoleErrors.Increment("File Skipped");
                        }
                    }
                });

                #region alternative file paths (Logs)

                if (!string.IsNullOrEmpty(ParserSettings.AlternativeLogFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeDDLFilePath);
                    List<IFilePath> alterFiles = null;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else
                    {
                        alterFiles = new List<IFilePath>() { (IFilePath)alterPath };
                    }

                    foreach (IFilePath element in alterFiles)
                    {
                        string ipAddress;
                        string dcName;

                        if (ProcessFileTasks.DetermineIPDCFromFileName(element.FileName, dtRingInfo, out ipAddress, out dcName))
                        {
                            if (string.IsNullOrEmpty(dcName))
                            {
                                element.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                            }

                            logParsingTasks.Add(ProcessFileTasks.ProcessLogFileTasks(element,
                                                                                        ParserSettings.ExcelWorkSheetLogCassandra,
                                                                                        dcName,
                                                                                        ipAddress,
                                                                                        includeLogEntriesAfterThisTimeFrame,
                                                                                        maxminMaxLogDate,
                                                                                        -1,
                                                                                        dtLogsStack,
                                                                                        null,
                                                                                        ParserSettings.ParseNonLogs,
                                                                                        ParserSettings.ExcelWorkSheetStatusLogCassandra,
                                                                                        nodeGCInfo,
                                                                                        ParserSettings.IgnoreKeySpaces,
                                                                                        kstblNames,
                                                                                        dtLogStatusStack,
                                                                                        dtCFStatsStack,
                                                                                        dtNodeStatsStack,
                                                                                        ParserSettings.GCPausedFlagThresholdInMS,
                                                                                        ParserSettings.CompactionFlagThresholdInMS,
                                                                                        ParserSettings.SlowLogQueryThresholdInMS));
                        }
                    }
                }

                #endregion

                if (kstblNames.Count == 0)
                {
                    Logger.Dump("DDL was not found which can cause missing information in the Excel workbooks.", Logger.DumpType.Warning);
                    Program.ConsoleWarnings.Increment("DDL Not Found");
                }

                #endregion
            }
            else
            {
                #region Parse -- Files located in separate folders

                var diagNodePath = diagPath.MakeChild(ParserSettings.DiagNodeDir) as Common.IDirectoryPath;
                List<Common.IDirectoryPath> nodeDirs = null;

                if (diagNodePath != null && (opsCtrDiag = diagNodePath.Exist()))
                {
                    var childrenItems = diagNodePath.Children();
                    var files = childrenItems.Where(item => item.IsFilePath).Cast<Common.IFilePath>();
                    
                    if(files.HasAtLeastOneElement())
                    {
                        var filesInWarning = string.Format("Invalid File(s) Found in OpsCenter Folder: {0}",
                                                            string.Join(", ", files.Select(file => "\"" + file.Path + "\"")));
                        Logger.Instance.Warn(filesInWarning);
                        Program.ConsoleWarnings.Increment("Invalid File(s) Found in OpsCenter Folder");
                    }

                    nodeDirs = childrenItems.Where(item => item.IsDirectoryPath).Cast<Common.IDirectoryPath>().ToList();
                }
                else
                {
                    var childrenItems = diagPath.Children();
                    
                    nodeDirs = childrenItems.Where(item => item.IsDirectoryPath).Cast<Common.IDirectoryPath>().ToList();
                }

                IFilePath filePath = null;

                if (ParserSettings.ParseNonLogs && nodeDirs.First().MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolRingFile, out filePath))
                {
                    if (filePath.Exist())
                    {
                        Program.ConsoleNonLogFiles.Increment(filePath);
                        Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                        ProcessFileTasks.ReadRingFileParseIntoDataTables(filePath, dtRingInfo, dtTokenRange);
                        Program.ConsoleNonLogFiles.TaskEnd(filePath);
                    }
                }

                if (ParserSettings.ParseNonLogs && nodeDirs.First().MakeChild(ParserSettings.DSEToolDir).MakeFile(ParserSettings.DSEtoolRingFile, out filePath))
                {
                    if (filePath.Exist())
                    {
                        Program.ConsoleNonLogFiles.Increment(filePath);
                        Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                        ProcessFileTasks.ReadDSEToolRingFileParseIntoDataTable(filePath, dtRingInfo);
                        Program.ConsoleNonLogFiles.TaskEnd(filePath);
                    }
                }

                if (ParserSettings.ParseNonLogs && nodeDirs.First().MakeFile(ParserSettings.CQLDDLDirFile, out filePath))
                {
                    if (filePath.Exist())
                    {
                        Program.ConsoleNonLogFiles.Increment(filePath);
                        Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                        ProcessFileTasks.ReadCQLDDLParseIntoDataTable(filePath,
                                                                        null,
                                                                        null,
                                                                        dtKeySpace,
                                                                        dtTable,
                                                                        cqlHashCheck,
                                                                        ParserSettings.IgnoreKeySpaces);

                        foreach (DataRow dataRow in dtTable.Rows)
                        {
                            if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.TableName == (dataRow["Name"] as string)))
                            {
                                kstblNames.Add(new CKeySpaceTableNames(dataRow));
                            }
                        }
                        Program.ConsoleNonLogFiles.TaskEnd(filePath);
                    }
                }

                #region alternative file paths

                if (!string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeDDLFilePath);
                    List<IFilePath> alterFiles = null;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else
                    {
                        alterFiles = new List<IFilePath>() { (IFilePath)alterPath };
                    }

                    foreach (IFilePath element in alterFiles)
                    {
                        Program.ConsoleNonLogFiles.Increment(element);
                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadCQLDDLParseIntoDataTable(element,
                                                                        null,
                                                                        null,
                                                                        dtKeySpace,
                                                                        dtTable,
                                                                        cqlHashCheck,
                                                                        ParserSettings.IgnoreKeySpaces);

                        foreach (DataRow dataRow in dtTable.Rows)
                        {
                            if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.TableName == (dataRow["Name"] as string)))
                            {
                                kstblNames.Add(new CKeySpaceTableNames(dataRow));
                            }
                        }
                        Program.ConsoleNonLogFiles.TaskEnd(element);
                        element.MakeEmpty();
                    }
                }

                if (kstblNames.Count == 0)
                {
                    //We need to have a list of valid Keyspaces and Tables...
                    if (nodeDirs.First().Clone().AddChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolCFStatsFile, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            filePath.Path.Dump(Logger.DumpType.Warning, "DDL was not found, parsing a TPStats file to obtain data model information");
                            Program.ConsoleWarnings.Increment("DDL Not Found");
                            Program.ConsoleNonLogFiles.Increment(filePath);
                            ProcessFileTasks.ReadCFStatsFileForKeyspaceTableInfo(filePath, ParserSettings.IgnoreKeySpaces, kstblNames);
                            Program.ConsoleNonLogFiles.TaskEnd(filePath);
                        }
                    }
                }

                if (kstblNames.Count == 0)
                {
                    Logger.Dump("DDL was not found which can cause missing information in the Excel workbooks.", Logger.DumpType.Warning);
                    Program.ConsoleWarnings.Increment("DDL Not Found");
                }

                if (!string.IsNullOrEmpty(ParserSettings.AlternativeLogFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeDDLFilePath);
                    List<IFilePath> alterFiles = null;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children().Where(p => p.IsFilePath).Cast<IFilePath>().ToList();
                    }
                    else
                    {
                        alterFiles = new List<IFilePath>() { (IFilePath)alterPath };
                    }

                    foreach (IFilePath element in alterFiles)
                    {
                        string ipAddress;
                        string dcName;

                        if (ProcessFileTasks.DetermineIPDCFromFileName(element.FileName, dtRingInfo, out ipAddress, out dcName))
                        {
                            if (string.IsNullOrEmpty(dcName))
                            {
                                element.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                            }

                            logParsingTasks.Add(ProcessFileTasks.ProcessLogFileTasks(element,
                                                                                        ParserSettings.ExcelWorkSheetLogCassandra,
                                                                                        dcName,
                                                                                        ipAddress,
                                                                                        includeLogEntriesAfterThisTimeFrame,
                                                                                        maxminMaxLogDate,
                                                                                        -1,
                                                                                        dtLogsStack,
                                                                                        null,
                                                                                        ParserSettings.ParseNonLogs,
                                                                                        ParserSettings.ExcelWorkSheetStatusLogCassandra,
                                                                                        nodeGCInfo,
                                                                                        ParserSettings.IgnoreKeySpaces,
                                                                                        kstblNames,
                                                                                        dtLogStatusStack,
                                                                                        dtCFStatsStack,
                                                                                        dtNodeStatsStack,
                                                                                        ParserSettings.GCPausedFlagThresholdInMS,
                                                                                        ParserSettings.CompactionFlagThresholdInMS,
                                                                                        ParserSettings.SlowLogQueryThresholdInMS));
                        }
                    }
                }

                #endregion

                Parallel.ForEach(nodeDirs, (element) =>
                //foreach (var element in nodeDirs)
                {
                    string ipAddress = null;
                    string dcName = null;
                    IFilePath diagFilePath = null;

                    ProcessFileTasks.DetermineIPDCFromFileName(element.Name, dtRingInfo, out ipAddress, out dcName);

                    if (ParserSettings.ParseNonLogs && string.IsNullOrEmpty(dcName))
                    {
                        element.Path.Dump(Logger.DumpType.Warning, "DataCenter Name was not found in the Ring file.");
                        Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                    }

                    if (ParserSettings.ParseNonLogs)
                    {
                        Logger.Instance.InfoFormat("Processing Files {{{0}}} in directory \"{1}\"",
                                                    string.Join(", ", ParserSettings.OSMachineFiles),
                                                    element.Path);

                        ProcessFileTasks.ParseOSMachineInfoDataTable(element,
                                                                        ParserSettings.OSMachineFiles,
                                                                        ipAddress,
                                                                        dcName,
                                                                        dtOSMachineInfo);
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolCFStatsFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogFiles.Increment(diagFilePath);

                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var dtCFStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCFStats + "-" + ipAddress);
                            dtCFStatsStack.Push(dtCFStats);
                            ProcessFileTasks.ReadCFStatsFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtCFStats, ParserSettings.IgnoreKeySpaces, ParserSettings.CFStatsCreateMBColumns);
                            Program.ConsoleNonLogFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolTPStatsFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var dtTPStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetNodeStats + "-" + ipAddress);
                            dtNodeStatsStack.Push(dtTPStats);
                            ProcessFileTasks.ReadTPStatsFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtTPStats);
                            Program.ConsoleNonLogFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolInfoFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            ProcessFileTasks.ReadInfoFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtRingInfo);
                            Program.ConsoleNonLogFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolCompactionHistFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var dtHistComp = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCompactionHist + "-" + ipAddress);
                            dtCompHistStack.Push(dtHistComp);
                            ProcessFileTasks.ReadCompactionHistFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtHistComp, dtTable, ParserSettings.IgnoreKeySpaces, kstblNames);
                            Program.ConsoleNonLogFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseLogs && element.MakeChild(ParserSettings.LogsDir).MakeFile(ParserSettings.LogCassandraDirSystemLog, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            IFilePath archivedFilePath = null;

                            if (ParserSettings.ParseArchivedLogs)
                            {
                                diagFilePath.ParentDirectoryPath.MakeFile(ParserSettings.LogCassandraSystemLogFileArchive, out archivedFilePath);
                            }

                            logParsingTasks.Add(ProcessFileTasks.ProcessLogFileTasks(diagFilePath,
                                                                                        ParserSettings.ExcelWorkSheetLogCassandra,
                                                                                        dcName,
                                                                                        ipAddress,
                                                                                        includeLogEntriesAfterThisTimeFrame,
                                                                                        maxminMaxLogDate,
                                                                                        ParserSettings.LogMaxRowsPerNode,
                                                                                        dtLogsStack,
                                                                                        archivedFilePath,
                                                                                        ParserSettings.ParseNonLogs,
                                                                                        ParserSettings.ExcelWorkSheetStatusLogCassandra,
                                                                                        nodeGCInfo,
                                                                                        ParserSettings.IgnoreKeySpaces,
                                                                                        kstblNames,
                                                                                        dtLogStatusStack,
                                                                                        dtCFStatsStack,
                                                                                        dtNodeStatsStack,
                                                                                        ParserSettings.GCPausedFlagThresholdInMS,
                                                                                        ParserSettings.CompactionFlagThresholdInMS,
                                                                                        ParserSettings.SlowLogQueryThresholdInMS));
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.ConfCassandraDir).MakeFile(ParserSettings.ConfCassandraFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, ParserSettings.ConfCassandraType, yamlList);
                            Program.ConsoleNonLogFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.ConfDSEDir).MakeFile(ParserSettings.ConfDSEYamlFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, ParserSettings.ConfDSEYamlType, yamlList);
                            Program.ConsoleNonLogFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.ConfDSEDir).MakeFile(ParserSettings.ConfDSEFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, ParserSettings.ConfDSEType, yamlList);
                            Program.ConsoleNonLogFiles.TaskEnd(diagFilePath);
                        }
                    }

                });

                #endregion
            }

            Program.ConsoleNonLogFiles.Terminate();

            var runYamlListIntoDTTask = Task.Factory.StartNew(() =>
                                            {
                                                Program.ConsoleParsingNonLog.Increment("Yaml");
                                                ProcessFileTasks.ParseYamlListIntoDataTable(listCYamlStack, dtYaml);
                                                Program.ConsoleParsingNonLog.TaskEnd("Yaml");
                                            }, 
                                             TaskCreationOptions.LongRunning);

            var updateRingWYamlInfoTask = runYamlListIntoDTTask.ContinueWith(task =>
                                            {
                                                Program.ConsoleParsingNonLog.Increment("OpsCenter");

                                                ProcessFileTasks.ParseOPSCenterInfoDataTable((IDirectoryPath)diagPath.Clone().AddChild(ParserSettings.OPSCenterDir),
                                                                                                ParserSettings.OPSCenterFiles,
                                                                                                dtOSMachineInfo,
                                                                                                dtRingInfo);

                                                ProcessFileTasks.UpdateMachineInfo(dtOSMachineInfo,
                                                                                    nodeGCInfo);

                                                ProcessFileTasks.UpdateRingInfo(dtRingInfo,
                                                                                dtYaml);

                                                Program.ConsoleParsingNonLog.TaskEnd("OpsCenter");
                                            },
                                        TaskContinuationOptions.AttachedToParent
                                            | TaskContinuationOptions.LongRunning
                                            | TaskContinuationOptions.OnlyOnRanToCompletion);

            Task<DataTable> runLogParsingTask = null;
            Task<DataTable> runSummaryLogTask = null;
            
            if (ParserSettings.ParseLogs && logParsingTasks.Count > 0)
            {
                var combLogParsingTasks = Task<int>
                                        .Factory
                                        .ContinueWhenAll(logParsingTasks.ToArray(), tasks => tasks.Sum(t => ((Task<int>)t).Result));

                combLogParsingTasks.ContinueWith(action =>
                                        {
                                            Program.ConsoleLogFiles.Terminate();
                                        });

                runLogParsingTask = combLogParsingTasks.ContinueWith(action =>
                                    {
                                        Program.ConsoleParsingLog.Increment("Log Merge");
                                        var dtlog = dtLogsStack.MergeIntoOneDataTable(new Tuple<string, string, DataViewRowState>(ParserSettings.LogExcelWorkbookFilter,
                                                                                                                                            "[Data Center], [Timestamp] DESC",
                                                                                                                                            DataViewRowState.CurrentRows));
                                        Program.ConsoleParsingLog.TaskEnd("Log Merge");
                                        return dtlog;
                                    },
                                    TaskContinuationOptions.AttachedToParent
                                        | TaskContinuationOptions.LongRunning
                                        | TaskContinuationOptions.OnlyOnRanToCompletion);

                runLogParsingTask.ContinueWith(action =>
                                        {
                                            Program.ConsoleParsingNonLog.Terminate();
                                        });

                runSummaryLogTask = ProcessFileTasks.ParseCassandraLogIntoSummaryDataTable(runLogParsingTask,
                                                                                            ParserSettings.ExcelWorkSheetLogCassandra,                                                                                            
                                                                                            ProcessFileTasks.LogCassandraMaxMinTimestamp,
                                                                                            ParserSettings.LogSummaryPeriods,
                                                                                            ParserSettings.LogSummaryPeriodRanges,
                                                                                            ParserSettings.LogSummaryIndicatorType,
                                                                                            ParserSettings.LogSummaryTaskItems,
                                                                                            ParserSettings.LogSummaryIgnoreTaskExceptions);

                Task.Factory
                    .ContinueWhenAll(new Task[] { runSummaryLogTask, updateRingWYamlInfoTask },
                                        tasks => Program.ConsoleParsingNonLog.Terminate());
            }
            else
            {
                updateRingWYamlInfoTask.ContinueWith(task => Program.ConsoleParsingNonLog.Terminate());
            }
            #endregion

            Logger.Instance.InfoFormat("Log {0}", ProcessFileTasks.LogCassandraMaxMinTimestamp);

            #region Excel Creation/Formatting

            if (!string.IsNullOrEmpty(ParserSettings.ExcelTemplateFilePath))
            {
                var excelTemplateFile = ParserSettings.ExcelTemplateFilePath == null ? null : Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelTemplateFilePath);
                var excelFile = Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelFilePath);

                if (excelTemplateFile != null
                        && !excelFile.Exist()
                        && excelTemplateFile.Exist())
                {
                    try
                    {
                        if (excelTemplateFile.Copy(excelFile))
                        {
                            Logger.Instance.InfoFormat("Created Workbook \"{0}\" from Template \"{1}\"", excelFile.Path, excelTemplateFile.Path);
                        }
                    }
                    catch (System.Exception ex)
                    {
                        Logger.Instance.Error(string.Format("Created Workbook \"{0}\" from Template \"{1}\" Failed", excelFile.Path, excelTemplateFile.Path), ex);
                        Program.ConsoleErrors.Increment("Workbook Template Copy Failed");
                    }
                }
            }

            #region Load Logs into Excel
            
            Task runLogToExcel = null;

            if (ParserSettings.LoadLogsIntoExcel && ParserSettings.ParseLogs && logParsingTasks.Count > 0)
            {                
                var statusLogToExcel = DTLoadIntoExcel.LoadStatusLog(runLogParsingTask,
                                                                            dtLogStatusStack,
                                                                            ParserSettings.ExcelFilePath,
                                                                            ParserSettings.ExcelWorkSheetStatusLogCassandra,                                                                           
                                                                            ProcessFileTasks.LogCassandraMaxMinTimestamp,
                                                                            ParserSettings.MaxRowInExcelWorkBook,
                                                                            ParserSettings.MaxRowInExcelWorkSheet,
                                                                            ParserSettings.LogExcelWorkbookFilter);

                statusLogToExcel.ContinueWith(action =>
                                                {
                                                    Program.ConsoleExcelLogStatus.Terminate();
                                                });

                var logToExcel = DTLoadIntoExcel.LoadCassandraLog(runLogParsingTask,
                                                                        ParserSettings.ExcelFilePath,
                                                                        ParserSettings.ExcelWorkSheetLogCassandra,                                                                        
                                                                        ProcessFileTasks.LogCassandraMaxMinTimestamp,
                                                                        ParserSettings.MaxRowInExcelWorkBook,
                                                                        ParserSettings.MaxRowInExcelWorkSheet,
                                                                        ParserSettings.LogExcelWorkbookFilter);

                logToExcel.ContinueWith(action =>
                                        {
                                            Program.ConsoleExcelLogStatus.Terminate();
                                        });

                runLogToExcel = Task.Factory
                                    .ContinueWhenAll(new Task[] { statusLogToExcel, logToExcel} , tasks => { });
            }
            #endregion

            //Non-Logs
            if (ParserSettings.ParseNonLogs)
            {
                var excelFile = Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelFilePath);
                using (var excelPkg = new ExcelPackage(excelFile.FileInfo()))
                {
                    DTLoadIntoExcel.LoadTokenRangeInfo(excelPkg, dtTokenRange, ParserSettings.ExcelWorkSheetRingTokenRanges);
                    DTLoadIntoExcel.LoadCompacationHistory(excelPkg, dtCompHistStack, ParserSettings.ExcelWorkSheetCompactionHist);
                    DTLoadIntoExcel.LoadKeySpaceDDL(excelPkg, dtKeySpace, ParserSettings.ExcelWorkSheetDDLKeyspaces);
                    DTLoadIntoExcel.LoadTableDDL(excelPkg, dtTable, ParserSettings.ExcelWorkSheetDDLTables);
                    DTLoadIntoExcel.LoadYamlRingOSInfo(updateRingWYamlInfoTask,
                                                        excelPkg,
                                                        dtYaml,
                                                        ParserSettings.ExcelWorkSheetYaml,
                                                        dtRingInfo,
                                                        ParserSettings.ExcelWorkSheetRingInfo,
                                                        dtOSMachineInfo,
                                                        ParserSettings.ExcelWorkSheetOSMachineInfo);
                    
                    DTLoadIntoExcel.LoadSummaryLogCFNodeStats(runLogParsingTask,
                                                                runSummaryLogTask,
                                                                excelPkg,                                                                
                                                                ParserSettings.ExcelWorkSheetSummaryLogCassandra,
                                                                ProcessFileTasks.LogCassandraMaxMinTimestamp,
                                                                maxminMaxLogDate,
                                                                ParserSettings.LogTimeSpanRange,
                                                                ParserSettings.LogExcelWorkbookFilter,
                                                                dtCFStatsStack,
                                                                ParserSettings.ExcelWorkSheetCFStats,
                                                                dtNodeStatsStack,
                                                                ParserSettings.ExcelWorkSheetNodeStats);
                 
                    DTLoadIntoExcel.UpdateApplicationWs(excelPkg);

                    excelPkg.Save();
                    Program.ConsoleExcelWorkbook.Increment(excelFile);
                    Program.ConsoleExcelNonLog.Terminate();
                } //Save non-log data
                Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
            }
           
            runLogToExcel?.Wait();
            Program.ConsoleExcelWorkbook.Terminate();

            #endregion

            ConsoleDisplay.End();
            Logger.Instance.InfoFormat("Completed");
        }

        static void CurrentDomain_UnhandledException(object sender, UnhandledExceptionEventArgs e)
        {
            ConsoleDisplay.Console.WriteLine(" ");

            Logger.Instance.FatalFormat("Unhandled Exception Occurred! Exception Is \"{0}\" ({1}) Terminating Processing: {2}",
                                            e.ExceptionObject.GetType(),
                                            e.ExceptionObject is System.Exception ? ((System.Exception)e.ExceptionObject).Message : "<Not an Exception Object>",
                                            e.IsTerminating);

            if (e.ExceptionObject is System.Exception)
            {
                Logger.Instance.Error("Unhandled Exception", ((System.Exception)e.ExceptionObject));
                Program.ConsoleErrors.Increment("Unhandled Exception");
            }

            //ExceptionOccurred = true;
        }

    }
}
