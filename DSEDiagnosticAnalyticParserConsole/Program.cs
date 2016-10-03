﻿using System;
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

        static public ConsoleDisplay ConsoleNonLogReadFiles = null;
        static public ConsoleDisplay ConsoleLogReadFiles = null;
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

            ConsoleDisplay.Console.WriteLine(" ");
            ConsoleDisplay.Console.WriteLine("Diagnostic Source Folder: \"{0}\"", Common.Path.PathUtils.BuildDirectoryPath(argResult.Value.DiagnosticPath)?.PathResolved);
            ConsoleDisplay.Console.WriteLine("Excel Target File: \"{0}\"", Common.Path.PathUtils.BuildDirectoryPath(argResult.Value.ExcelFilePath)?.PathResolved);
            ConsoleDisplay.Console.WriteLine("Parse Non-Logs: {0} Logs: {1} Archived Logs: {2} Excel Load Logs: {3}",
                                                argResult.Value.ParseNonLogs,
                                                argResult.Value.ParseLogs,
                                                argResult.Value.ParseArchivedLogs,
                                                argResult.Value.LoadLogsIntoExcel);

            ConsoleDisplay.Console.WriteLine(" ");

            ConsoleNonLogReadFiles = new ConsoleDisplay("Non-Log Files: {0} Working: {1} Task: {2}");
            ConsoleLogReadFiles = new ConsoleDisplay("Log Files: {0}  Working: {1} Task: {2}");
            ConsoleParsingNonLog = new ConsoleDisplay("Non-Log Processing: {0}  Working: {1} Task: {2}");
            ConsoleParsingLog = new ConsoleDisplay("Log Processing: {0}  Working: {1} Task: {2}");
            ConsoleExcel = new ConsoleDisplay("Excel: {0}  Working: {1} WorkSheet: {2}");
            ConsoleExcelNonLog = new ConsoleDisplay("Excel Non-Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelLog = new ConsoleDisplay("Excel Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelLogStatus = new ConsoleDisplay("Excel Status Log: {0}  Working: {1} Task: {2}");
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
            Task<DataTable> tskdtCFHistogram = Task.FromResult<DataTable>(null);
            var includeLogEntriesAfterThisTimeFrame = ParserSettings.LogCurrentDate == DateTime.MinValue ? DateTime.MinValue : ParserSettings.LogCurrentDate - ParserSettings.LogTimeSpanRange;
            int nbrNodes = -1;

            ProcessFileTasks.InitializeCQLDDLDataTables(dtKeySpace, dtTable);

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
            var parsedLogList = new Common.Patterns.Collections.ThreadSafe.List<string>();
            var parsedDDLList = new Common.Patterns.Collections.ThreadSafe.List<string>();
            var parsedRingList = new Common.Patterns.Collections.ThreadSafe.List<string>();
            var parsedCFStatList = new Common.Patterns.Collections.ThreadSafe.List<string>();
            var parsedCFHistList = new Common.Patterns.Collections.ThreadSafe.List<string>();
            var parsedTPStatList = new Common.Patterns.Collections.ThreadSafe.List<string>();
            var parsedOSMachineList = new Common.Patterns.Collections.ThreadSafe.List<string>();
            var parsedYamlList = new Common.Patterns.Collections.ThreadSafe.List<string>();

            if (ParserSettings.DiagnosticNoSubFolders)
            {
                #region Read/Parse -- All Files under one Folder (IpAddress must be in the beginning/end of the file name)

                var diagChildren = diagPath.Children();

                //Need to process nodetool ring files first
                var nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(ParserSettings.NodetoolRingFile));

                #region preprocessing File

                if (ParserSettings.ParseNonLogs && nodetoolRingChildFiles.HasAtLeastOneElement())
                {
                    foreach (var element in nodetoolRingChildFiles)
                    {
                        Program.ConsoleNonLogReadFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadRingFileParseIntoDataTables((IFilePath)element, dtRingInfo, dtTokenRange);
                        //parsedRingList.TryAdd(((IFilePath)element).FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(ParserSettings.DSEToolDir + "_" + ParserSettings.DSEtoolRingFile));

                if (ParserSettings.ParseNonLogs && nodetoolRingChildFiles.HasAtLeastOneElement())
                {
                    foreach (var element in nodetoolRingChildFiles)
                    {
                        Program.ConsoleNonLogReadFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadDSEToolRingFileParseIntoDataTable((IFilePath)element, dtRingInfo);
                        //parsedRingList.TryAdd(((IFilePath)element).FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                IFilePath cqlFilePath;

                if (ParserSettings.ParseNonLogs && diagPath.MakeFile(ParserSettings.CQLDDLDirFileExt, out cqlFilePath))
                {
                    foreach (IFilePath element in cqlFilePath.GetWildCardMatches())
                    {
                        Program.ConsoleNonLogReadFiles.Increment((IFilePath)element);

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
                        parsedDDLList.TryAdd(((IFilePath)element).FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                #endregion

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

                    Logger.Instance.InfoFormat("Queing {0} Alternative CQL Files: {1}",
                                                alterFiles.Count,
                                                string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
                    foreach (IFilePath element in alterFiles)
                    {
                        Program.ConsoleNonLogReadFiles.Increment((IFilePath)element);

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
                        parsedDDLList.TryAdd(((IFilePath)element).FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }
                }

                #endregion

                Logger.Instance.InfoFormat("Queing {0} Files: {1}",
                                            diagChildren.Count,
                                            string.Join(", ", diagChildren.Select(p => p.Name).Sort()));

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

                                Program.ConsoleNonLogReadFiles.Increment((IFilePath)diagFile);
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
                                parsedCFStatList.TryAdd(ipAddress);
                                Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)diagFile);
                            }
                            else if (ParserSettings.ParseNonLogs && diagFile.Name.Contains(ParserSettings.NodetoolTPStatsFile))
                            {
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                Program.ConsoleNonLogReadFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                var dtTPStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetNodeStats + "-" + ipAddress);
                                dtNodeStatsStack.Push(dtTPStats);
                                ProcessFileTasks.ReadTPStatsFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtTPStats);
                                parsedTPStatList.TryAdd(ipAddress);
                                Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)diagFile);
                            }
                            else if (ParserSettings.ParseNonLogs && diagFile.Name.Contains(ParserSettings.NodetoolInfoFile))
                            {
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }
                                Program.ConsoleNonLogReadFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                ProcessFileTasks.ReadInfoFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtRingInfo);
                                parsedRingList.TryAdd(ipAddress);
                                Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)diagFile);
                            }
                            else if (ParserSettings.ParseNonLogs && diagFile.Name.Contains(ParserSettings.NodetoolCompactionHistFile))
                            {
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                Program.ConsoleNonLogReadFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                var dtCompHist = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCompactionHist + "-" + ipAddress);
                                dtCompHistStack.Push(dtCompHist);
                                ProcessFileTasks.ReadCompactionHistFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtCompHist, dtTable, ParserSettings.IgnoreKeySpaces, kstblNames);
                                Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)diagFile);
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
                                                                                            ParserSettings.GCFlagThresholdInMS,
                                                                                            ParserSettings.CompactionFlagThresholdInMS,
                                                                                            ParserSettings.SlowLogQueryThresholdInMS));
                                parsedLogList.TryAdd(ipAddress);
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

                    Logger.Instance.InfoFormat("Queing {0} Alternative Log Files: {1}",
                                                alterFiles.Count,
                                                string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
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
                                                                                        ParserSettings.GCFlagThresholdInMS,
                                                                                        ParserSettings.CompactionFlagThresholdInMS,
                                                                                        ParserSettings.SlowLogQueryThresholdInMS));
                            parsedLogList.TryAdd(ipAddress);
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
                #region Read/Parse -- Files located in separate folders where each folder's name is the IP Address

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
                var preFilesProcessed = new bool[3];

                #region preparsing Files

                for (int fileIndex = 0;
                        fileIndex < nodeDirs.Count && !preFilesProcessed.All(flag => flag);
                        ++fileIndex)
                {
                    if (ParserSettings.ParseNonLogs
                            && !preFilesProcessed[0]
                            && nodeDirs[fileIndex].MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolRingFile, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(filePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                            ProcessFileTasks.ReadRingFileParseIntoDataTables(filePath, dtRingInfo, dtTokenRange);
                            //parsedRingList.TryAdd(filePath.PathResolved);
                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);

                            preFilesProcessed[0] = true;
                        }
                        else
                        {
                            Logger.Instance.InfoFormat("NodeTool Ring file for \"{0}\" is missing. Trying next node folder", filePath.ParentDirectoryPath.PathResolved);
                        }
                    }

                    if (ParserSettings.ParseNonLogs
                            && !preFilesProcessed[1]
                            && nodeDirs[fileIndex].MakeChild(ParserSettings.DSEToolDir).MakeFile(ParserSettings.DSEtoolRingFile, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(filePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                            ProcessFileTasks.ReadDSEToolRingFileParseIntoDataTable(filePath, dtRingInfo);
                            //parsedRingList.TryAdd(filePath.PathResolved);
                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);

                            preFilesProcessed[1] = true;
                        }
                        else
                        {
                            Logger.Instance.InfoFormat("DSETool Ring file for \"{0}\" is missing. Trying next node folder", filePath.ParentDirectoryPath.PathResolved);
                        }
                    }

                    if (ParserSettings.ParseNonLogs
                            && !preFilesProcessed[2]
                            && nodeDirs[fileIndex].MakeFile(ParserSettings.CQLDDLDirFile, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(filePath);
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
                            parsedDDLList.TryAdd(filePath.PathResolved);
                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);

                            preFilesProcessed[2] = true;
                        }
                        else
                        {
                            Logger.Instance.InfoFormat("CQL DDL file for \"{0}\" is missing. Trying next node folder", filePath.ParentDirectoryPath.PathResolved);
                        }
                    }
                }

                #endregion

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

                    Logger.Instance.InfoFormat("Queing {0} Alternative CQL Files: {1}",
                                                alterFiles.Count,
                                                string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
                    foreach (IFilePath element in alterFiles)
                    {
                        Program.ConsoleNonLogReadFiles.Increment(element);
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
                        parsedDDLList.TryAdd(filePath.FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd(element);
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
                            Program.ConsoleNonLogReadFiles.Increment(filePath);
                            ProcessFileTasks.ReadCFStatsFileForKeyspaceTableInfo(filePath, ParserSettings.IgnoreKeySpaces, kstblNames);
                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);
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

                    Logger.Instance.InfoFormat("Queing {0} Alternative Log Files: {1}",
                                                    alterFiles.Count,
                                                    string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
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
                                                                                        ParserSettings.GCFlagThresholdInMS,
                                                                                        ParserSettings.CompactionFlagThresholdInMS,
                                                                                        ParserSettings.SlowLogQueryThresholdInMS));
                            parsedLogList.TryAdd(ipAddress);
                        }
                    }
                }

                #endregion

                nbrNodes = nodeDirs.Count;

                Logger.Instance.InfoFormat("Queing {0} Nodes: {1}",
                                            nodeDirs.Count,
                                            string.Join(", ", nodeDirs.Select(p => p.Name).Sort()));
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
                        parsedOSMachineList.TryAdd(ipAddress);
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolCFStatsFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);

                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var dtCFStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCFStats + "-" + ipAddress);
                            dtCFStatsStack.Push(dtCFStats);
                            ProcessFileTasks.ReadCFStatsFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtCFStats, ParserSettings.IgnoreKeySpaces, ParserSettings.CFStatsCreateMBColumns);
                            parsedCFStatList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolTPStatsFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var dtTPStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetNodeStats + "-" + ipAddress);
                            dtNodeStatsStack.Push(dtTPStats);
                            ProcessFileTasks.ReadTPStatsFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtTPStats);
                            parsedTPStatList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolInfoFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            ProcessFileTasks.ReadInfoFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtRingInfo);
                            parsedRingList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolCompactionHistFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var dtHistComp = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCompactionHist + "-" + ipAddress);
                            dtCompHistStack.Push(dtHistComp);
                            ProcessFileTasks.ReadCompactionHistFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtHistComp, dtTable, ParserSettings.IgnoreKeySpaces, kstblNames);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
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
                                                                                        ParserSettings.GCFlagThresholdInMS,
                                                                                        ParserSettings.CompactionFlagThresholdInMS,
                                                                                        ParserSettings.SlowLogQueryThresholdInMS));
                            parsedLogList.TryAdd(ipAddress);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.ConfCassandraDir).MakeFile(ParserSettings.ConfCassandraFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, ParserSettings.ConfCassandraType, yamlList);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.ConfDSEDir).MakeFile(ParserSettings.ConfDSEYamlFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, ParserSettings.ConfDSEYamlType, yamlList);
                            parsedYamlList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                    }

                    if (ParserSettings.ParseNonLogs && element.MakeChild(ParserSettings.ConfDSEDir).MakeFile(ParserSettings.ConfDSEFile, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, ParserSettings.ConfDSEType, yamlList);
                            parsedYamlList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                    }

                });

                #endregion
            }

            #region cfHistogram

            bool parseCFHistFiles = false;
            IFilePath cfHistogramWildFilePath;
            IFilePath tableHistogramWildFilePath;
            Task<IEnumerable<IFilePath>> cfhistFilesTask = Task.FromResult( (IEnumerable<IFilePath>) new IFilePath[0]);

            if (diagPath.MakeFile(string.Format("*{0}*", ParserSettings.CFHistogramFileName), out cfHistogramWildFilePath))
            {
                cfhistFilesTask = cfhistFilesTask.ContinueWith(wildFileMatchesTask =>
                                        {
                                            var tblMatches = cfHistogramWildFilePath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>();

                                            return wildFileMatchesTask.Result.Append(tblMatches.ToArray());
                                        },
                                    TaskContinuationOptions.AttachedToParent                                        
                                        | TaskContinuationOptions.OnlyOnRanToCompletion);
                parseCFHistFiles = true;
            }
            if (diagPath.MakeFile(string.Format("*{0}*", ParserSettings.TableHistogramFileName), out tableHistogramWildFilePath))
            {
                cfhistFilesTask = cfhistFilesTask.ContinueWith(wildFileMatchesTask =>
                                        {
                                            var tblMatches = tableHistogramWildFilePath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>();

                                            return wildFileMatchesTask.Result.Append(tblMatches.ToArray());
                                        },
                                    TaskContinuationOptions.AttachedToParent                                      
                                        | TaskContinuationOptions.OnlyOnRanToCompletion);
                parseCFHistFiles = true;
            }

            if (parseCFHistFiles)
            {                
                tskdtCFHistogram = cfhistFilesTask.ContinueWith(fileTask =>
                                    {
                                        var cfHistogramFiles = fileTask.Result;
                                        var dtCFHistogramsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();

                                        Logger.Instance.InfoFormat("Queing {0} Histogram Files: {1}",
                                                                        cfHistogramFiles.Count(),
                                                                        string.Join(", ", cfHistogramFiles.Select(p => p.Name).Sort()));

                                        if (cfHistogramFiles.HasAtLeastOneElement())
                                        {
                                            Parallel.ForEach(cfHistogramFiles, (chFile) =>
                                            //foreach (var chFile in cfHistogramFiles)
                                            {
                                                string ipAddress = null;
                                                string dcName = null;

                                                ProcessFileTasks.DetermineIPDCFromFileName(chFile.Name, dtRingInfo, out ipAddress, out dcName);

                                                if (string.IsNullOrEmpty(ipAddress))
                                                {
                                                    chFile.Path.Dump(Logger.DumpType.Warning, "IPAdress was not found in the CFHistogram file. File Ignored.");
                                                    Program.ConsoleWarnings.Increment("IPAdress Not Found");
                                                }
                                                else
                                                {
                                                    if (string.IsNullOrEmpty(dcName))
                                                    {
                                                        chFile.Path.Dump(Logger.DumpType.Warning, "DataCenter Name was not found in the CFHistogram file.");
                                                        Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                                    }

                                                    var dataTable = new DataTable(ParserSettings.ExcelCFHistogramWorkSheet + "-" + ipAddress);
                                                    dtCFHistogramsStack.Push(dataTable);

                                                    Program.ConsoleNonLogReadFiles.Increment(chFile);

                                                    ProcessFileTasks.ReadCFHistogramFileParseIntoDataTable(chFile, ipAddress, dcName, dataTable);
                                                    parsedCFHistList.TryAdd(ipAddress);
                                                    Program.ConsoleNonLogReadFiles.TaskEnd(chFile);
                                                }
                                            });
                                        }

                                        return dtCFHistogramsStack.MergeIntoOneDataTable();
                                    },
                                    TaskContinuationOptions.AttachedToParent
                                        | TaskContinuationOptions.LongRunning
                                        | TaskContinuationOptions.OnlyOnRanToCompletion);
            }

            #endregion

            tskdtCFHistogram.ContinueWith(Task =>
                                {
                                    ConsoleNonLogReadFiles.Terminate();
                                    Logger.Instance.InfoFormat("Parsed Ring Data for {0} files: {1}",
                                                                parsedRingList.Count,
                                                                string.Join(", ", parsedRingList.Sort<string>()));
                                    Logger.Instance.InfoFormat("Parsed DDL Data for {0} files: {1}",
                                                                parsedDDLList.Count,
                                                                string.Join(", ", parsedDDLList.Sort<string>()));
                                    Logger.Instance.InfoFormat("Parsed CFStats Data for {0} Nodes: {1}",
                                                                parsedCFStatList.Count,
                                                                string.Join(", ", parsedCFStatList.Sort<string>()));
                                    Logger.Instance.InfoFormat("Parsed TPStats Data for {0} Nodes: {1}",
                                                                parsedTPStatList.Count,
                                                                string.Join(", ", parsedTPStatList.Sort<string>()));
                                    Logger.Instance.InfoFormat("Parsed OS/Machine Data for {0} Nodes: {1}",
                                                                parsedOSMachineList.Count,
                                                                string.Join(", ", parsedOSMachineList.Sort<string>()));
                                    Logger.Instance.InfoFormat("Parsed Yaml/Config Data for {0} Nodes: {1}",
                                                                parsedYamlList.Count,
                                                                string.Join(", ", parsedYamlList.Sort<string>()));
                                    Logger.Instance.InfoFormat("Parsed CFHistogram Data for {0} Nodes: {1}",
                                                                parsedCFHistList.Count,
                                                                string.Join(", ", parsedCFHistList.Sort<string>()));
                                });
            
            //Task
            //    .Factory
            //    .ContinueWhenAll(new Task[] { tskdtCFHistogram }, tasks => Program.ConsoleNonLogFiles.Terminate());

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

            Task<DataTable> runLogParsingTask = Task.FromResult((DataTable)null); ;
            Task<DataTable> runSummaryLogTask = Task.FromResult((DataTable) null);            
            Task<int> runningLogTask = logParsingTasks.Count == 0
                                        ? Task.FromResult(0)
                                        : Task<int>
                                            .Factory
                                            .ContinueWhenAll(logParsingTasks.ToArray(), tasks => tasks.Sum(t => ((Task<int>)t).Result));

            if (ParserSettings.ParseLogs && logParsingTasks.Count > 0)
            {
                runningLogTask.ContinueWith(action =>
                                        {                                                                        
                                            Program.ConsoleLogReadFiles.Terminate();
                                            Logger.Instance.InfoFormat("Log {0}", ProcessFileTasks.LogCassandraMaxMinTimestamp);
                                            Logger.Instance.InfoFormat("Parsed Log Data for {0} Nodes: {1}",
                                                                        parsedLogList.Count,
                                                                        string.Join(", ", parsedLogList.Sort<string>()));
                                        });

                
                runLogParsingTask = runningLogTask.ContinueWith(action =>
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

                runSummaryLogTask = ProcessFileTasks.ParseCassandraLogIntoSummaryDataTable(runLogParsingTask,
                                                                                            ParserSettings.ExcelWorkSheetLogCassandra,                                                                                            
                                                                                            ProcessFileTasks.LogCassandraMaxMinTimestamp,
                                                                                            ParserSettings.LogSummaryPeriods,
                                                                                            ParserSettings.LogSummaryPeriodRanges,
                                                                                            ParserSettings.LogSummaryIndicatorType,
                                                                                            ParserSettings.LogSummaryTaskItems,
                                                                                            ParserSettings.LogSummaryIgnoreTaskExceptions);

                runSummaryLogTask.ContinueWith(action =>
                                        {
                                            Program.ConsoleParsingLog.Terminate();
                                        });
            }

            tskdtCFHistogram = tskdtCFHistogram.ContinueWith(taskResult =>
                                    {
                                        var dtCFHist = taskResult.Result;

                                        if (dtCFHist.Rows.Count > 0)
                                        {

                                            Program.ConsoleParsingNonLog.Increment("TableHistogram => CFStats...");

                                            var dtCFStat = new DataTable("CFHistogram");

                                            dtCFStatsStack.Push(dtCFStat);
                                            ProcessFileTasks.ProcessCFHistStats(dtCFHist, dtCFStat);

                                            Program.ConsoleParsingNonLog.Decrement("TableHistogram => CFStats...");
                                        }
                                        return dtCFHist;
                                    },
                                    TaskContinuationOptions.AttachedToParent
                                        | TaskContinuationOptions.LongRunning
                                        | TaskContinuationOptions.OnlyOnRanToCompletion);

            var runUpdateActiveTblStatus = Task.Factory
                                                .ContinueWhenAll(new Task[] { tskdtCFHistogram, runningLogTask }, ignoreItem => { })
                                                .ContinueWith(action =>
                                                    {
                                                        Program.ConsoleParsingLog.Increment("CFStats Merge");
                                                        var dtCFTable = dtCFStatsStack.MergeIntoOneDataTable(new Tuple<string, string, DataViewRowState>(null,
                                                                                                                                                            "[Data Center], [Node IPAddress], [KeySpace], [Table]",
                                                                                                                                                            DataViewRowState.CurrentRows));
                                                        ProcessFileTasks.UpdateTableActiveStatus(dtCFTable);
                                                        Program.ConsoleParsingLog.TaskEnd("CFStats Merge");

                                                        Program.ConsoleParsingLog.Increment("DDL Active Table Update");
                                                        ProcessFileTasks.UpdateCQLDDLTableActiveStatus(dtTable);
                                                        Program.ConsoleParsingLog.TaskEnd("DDL Active Table Update");

                                                        return dtCFTable;
                                                    },
                                                TaskContinuationOptions.AttachedToParent
                                                    | TaskContinuationOptions.LongRunning
                                                    | TaskContinuationOptions.OnlyOnRanToCompletion);

            Task.Factory
                   .ContinueWhenAll(new Task[] { tskdtCFHistogram, runSummaryLogTask, updateRingWYamlInfoTask, runUpdateActiveTblStatus },
                                       tasks => Program.ConsoleParsingNonLog.Terminate());


            #endregion

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
                                                                runUpdateActiveTblStatus,
                                                                ParserSettings.ExcelWorkSheetCFStats,
                                                                dtNodeStatsStack,
                                                                ParserSettings.ExcelWorkSheetNodeStats,
                                                                dtTable,
                                                                ParserSettings.ExcelWorkSheetDDLTables);

                    DTLoadIntoExcel.LoadCFHistogram(excelPkg, tskdtCFHistogram, ParserSettings.ExcelCFHistogramWorkSheet);

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

            var parsedItemCounts = new int[] { parsedCFHistList.Count,
                                                parsedCFStatList.Count,
                                               // parsedDDLList.Count,
                                                parsedLogList.Count,
                                                parsedOSMachineList.Count,
                                                parsedRingList.Count,
                                                parsedTPStatList.Count,
                                                parsedYamlList.Count };

            if (nbrNodes < 0)
            {
                nbrNodes = parsedItemCounts.Max();
            }

            if(nbrNodes != parsedItemCounts.Where(cnt => cnt > 0).Min())
            {
                var msg = string.Format("Number of components read/parsed should have been {0}, but some components only parsed/read {1}. Review Application Log for Warning/Errors.",
                                            nbrNodes,
                                            parsedItemCounts.Where(cnt => cnt > 0).Min());
                Logger.Instance.Warn(msg);
                ConsoleDisplay.Console.WriteLine(msg);
            }
           
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