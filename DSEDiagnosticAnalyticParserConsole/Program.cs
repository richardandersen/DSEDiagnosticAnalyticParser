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
        public static string CommandLineArgsString = null;

        static public ConsoleDisplay ConsoleNonLogReadFiles = null;
        static public ConsoleDisplay ConsoleLogReadFiles = null;
        static public ConsoleDisplay ConsoleParsingNonLog = null;
        static public ConsoleDisplay ConsoleParsingLog = null;
		static public ConsoleDisplay ConsoleLogCount = null;
		static public ConsoleDisplay ConsoleExcelNonLog = null;
        static public ConsoleDisplay ConsoleExcelLog = null;
        static public ConsoleDisplay ConsoleExcelLogStatus = null;
        static public ConsoleDisplay ConsoleExcel = null;
        static public ConsoleDisplay ConsoleExcelWorkbook = null;
        static public ConsoleDisplay ConsoleWarnings = null;
        static public ConsoleDisplay ConsoleErrors = null;
        static public System.Data.DataTable DTRingInfo = null; //

        static void Main(string[] args)
        {
            AppDomain.CurrentDomain.UnhandledException += CurrentDomain_UnhandledException;

            Logger.Instance.Info("DSEDiagnosticAnalyticParserConsole");

            #region Command Line Argument and Settings

            CommandLineArgsString = string.Join(" ", args);
            var argResult = CommandLine.Parser.Default.ParseArguments<ConsoleArguments>(args);

            if(argResult.Value.Debug)
            {
                Common.ConsoleHelper.Prompt("Attach Debugger and Press Return to Continue", ConsoleColor.Gray, ConsoleColor.DarkRed);
                ConsoleDisplay.DisableAllConsoleWriter();
            }

            if (argResult.Value.DisplayDefaults)
            {
                Console.WriteLine(argResult.Value.ToString());
                return;
            }

            if (!argResult.Errors.IsEmpty())
            {
                Common.ConsoleHelper.Prompt("Press Return to Continue", ConsoleColor.Gray, ConsoleColor.DarkRed);
                ConsoleDisplay.DisableAllConsoleWriter();
                return;
            }

            if (ParserSettings.ParsingExcelOptions.ParseSummaryLogs.IsEnabled())
            {
                ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.LoadSummaryWorkSheets;
            }

			if (ParserSettings.ParsingExcelOptions.LoadReadRepairWorkSheets.IsEnabled())
			{
				ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.ParseReadRepairs;
			}

			if (ParserSettings.ParsingExcelOptions.Detect.IsEnabled())
            {
                if (!string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath)) ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.ParseDDLFiles;
                if (!string.IsNullOrEmpty(ParserSettings.AlternativeLogFilePath)) ParserSettings.LogParsingExcelOption |= ParserSettings.LogParsingExcelOptions.ParseLogs;
                if (!string.IsNullOrEmpty(ParserSettings.CFHistogramFileName)) ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.ParseTblHistogramFiles;
                if ((ParserSettings.ToleranceContinuousGCInMS >= 0
                            && ParserSettings.ContinuousGCNbrInSeries > 1)
                    || (ParserSettings.GCTimeFrameDetectionPercentage > 0
                            && ParserSettings.GCTimeFrameDetection != TimeSpan.Zero))
                {
                    ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.ParseNodeStatsLogs;
                    ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.LoadWorkSheets;
                }
                if (ParserSettings.LogSummaryPeriods.Length > 0
                    || ParserSettings.LogSummaryPeriodRanges.Length > 0)
                {
                    ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.ParseSummaryLogs;
                    ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.LoadSummaryWorkSheets;
                }
            }

            if (ParserSettings.ParsingExcelOptions.ProduceSummaryWorkbook.IsEnabled())
            {
                ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.ParseSummaryLogs;
            }

            if (ParserSettings.ParsingExcelOptions.ParseSummaryLogs.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseNodeStatsLogs.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseCFStatsLogs.IsEnabled()
					|| ParserSettings.ParsingExcelOptions.ParseReadRepairs.IsEnabled())
            {
                if (ParserSettings.LogParsingExcelOptions.Detect.IsEnabled())
                {
                    ParserSettings.LogParsingExcelOption |= ParserSettings.LogParsingExcelOptions.ParseLogs;
                }
            }

            if (ParserSettings.ParsingExcelOptions.ParseCFStatsFiles.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseCompacationHistFiles.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseMachineInfoFiles.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseOpsCenterFiles.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseTblHistogramFiles.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseTPStatsFiles.IsEnabled()
                    || ParserSettings.ParsingExcelOptions.ParseYamlFiles.IsEnabled())
            {
                if (!ParserSettings.ParsingExcelOptions.LoadWorkSheets.IsEnabled())
                {
                    ParserSettings.ParsingExcelOption |= ParserSettings.ParsingExcelOptions.LoadWorkSheets;
                }
            }

            Logger.Instance.InfoFormat("Program: {0} Version: {1} Directory: {2}",
                                            Common.Functions.Instance.ApplicationName,
                                            Common.Functions.Instance.ApplicationVersion,
                                            Common.Functions.Instance.AssemblyDir);

            Logger.Instance.Info("Starting with " + (CommandArgsString = argResult.Value.ToString()));

            if(!argResult.Value.CheckArguments())
            {
                Common.ConsoleHelper.Prompt("Press Return to Continue", ConsoleColor.Gray, ConsoleColor.DarkRed);
                ConsoleDisplay.DisableAllConsoleWriter();
                return;
            }

			#endregion

			#region Console Display Setup

			ConsoleDisplay.Console.ClearScreen();

            ConsoleDisplay.Console.WriteLine(" ");
            ConsoleDisplay.Console.WriteLine("Diagnostic Source Folder: \"{0}\"", Common.Path.PathUtils.BuildDirectoryPath(argResult.Value.DiagnosticPath)?.PathResolved);
            ConsoleDisplay.Console.WriteLine("Excel Target File: \"{0}\"", Common.Path.PathUtils.BuildDirectoryPath(argResult.Value.ExcelFilePath)?.PathResolved);
            ConsoleDisplay.Console.WriteLine("Parse Options: {{{0}}}, Log Options: {{{1}}}{2}",
                                                ParserSettings.ParsingExcelOption,
                                                ParserSettings.LogParsingExcelOption,
                                                ParserSettings.LogStartDate.IsEmpty()
                                                    ? string.Empty
                                                    : string.Format(" From: {0} ({0:ddd}) To: {1} ({1:ddd})",
                                                                        ParserSettings.LogStartDate.Min,
                                                                        ParserSettings.LogStartDate.Max));

            ConsoleDisplay.Console.WriteLine(" ");

            ConsoleNonLogReadFiles = new ConsoleDisplay("Non-Log Files: {0} Working: {1} Task: {2}");
            ConsoleLogReadFiles = new ConsoleDisplay("Log Files: {0}  Working: {1} Task: {2}");
            ConsoleLogCount = new ConsoleDisplay("Log Item Count: {0:###,###,##0}", 2, false);
            ConsoleParsingNonLog = new ConsoleDisplay("Non-Log Processing: {0}  Working: {1} Task: {2}");
            ConsoleParsingLog = new ConsoleDisplay("Log Processing: {0}  Working: {1} Task: {2}");
			ConsoleExcel = new ConsoleDisplay("Excel: {0}  Working: {1} WorkSheet: {2}");
            ConsoleExcelNonLog = new ConsoleDisplay("Excel Non-Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelLog = new ConsoleDisplay("Excel Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelLogStatus = new ConsoleDisplay("Excel Status/Summary Log: {0}  Working: {1} Task: {2}");
            ConsoleExcelWorkbook = new ConsoleDisplay("Excel Workbooks: {0} File: {2}");
            ConsoleWarnings = new ConsoleDisplay("Warnings: {0} Last: {2}", 2, false);
            ConsoleErrors = new ConsoleDisplay("Errors: {0} Last: {2}", 2, false);
            
            #endregion
            
            //if (!System.Runtime.GCSettings.IsServerGC
            //		&& System.Runtime.GCSettings.LatencyMode == System.Runtime.GCLatencyMode.Batch)
            //{
            GCMonitor.GetInstance().StartGCMonitoring();
			//}

			#region Local Variables

			//Local Variables used for processing
			bool opsCtrDiag = false;
            var dtRingInfo = DTRingInfo = new System.Data.DataTable(ParserSettings.ExcelWorkSheetRingInfo);
            var dtTokenRange = new System.Data.DataTable(ParserSettings.ExcelWorkSheetRingTokenRanges);
            var dtKeySpace = new System.Data.DataTable(ParserSettings.ExcelWorkSheetDDLKeyspaces);
            var dtDDLTable = new System.Data.DataTable(ParserSettings.ExcelWorkSheetDDLTables);
            var dtCFStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtNodeStatsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtLogsStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtLogStatusStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var dtCompHistStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();
            var listCYamlStack = new Common.Patterns.Collections.LockFree.Stack<List<YamlInfo>>();

            var dtYaml = new System.Data.DataTable(ParserSettings.ExcelWorkSheetYaml);
            var dtOSMachineInfo = new System.Data.DataTable(ParserSettings.ExcelWorkSheetOSMachineInfo);
            var nodeGCInfo = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, string>();            
			Task<DataTable> tskdtCFHistogram = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();
            int nbrNodes = -1;

            ProcessFileTasks.InitializeCQLDDLDataTables(dtKeySpace, dtDDLTable);

            #endregion

            System.Console.CancelKeyPress += Console_CancelKeyPress;

            ConsoleDisplay.Start();

            #region Parsing Files/Task Processing

            var diagPath = Common.Path.PathUtils.BuildDirectoryPath(ParserSettings.DiagnosticPath);

            if (ParserSettings.CreateDirStructForNodes != null)
            {
                CreateDiagFolders.CreateFolders(ParserSettings.CreateDirStructForNodes, diagPath);
                ConsoleDisplay.End();
                Logger.Instance.InfoFormat("Completed");
                GCMonitor.GetInstance().StopGCMonitoring();

                if (argResult.Value.Debug)
                {
                    Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);
                }
                return;
            }

            if (ParserSettings.LogStartDate.IsEmpty())
            {
                Logger.Instance.Info("All Log Entries will be parsed!");
            }
            else
            {
                Logger.Instance.InfoFormat("Log Entries between \"{0}\" and \"{1}\" will only be parsed",
                                            ParserSettings.LogStartDate.Min,
                                            ParserSettings.LogStartDate.Max);
            }
            
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
            var alternativeDebugLogs = new List<IFilePath>();
            var alternativeLogTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<int>();

            if (ParserSettings.FileParsingOption == ParserSettings.FileParsingOptions.IndivFiles)
            {
                #region Read/Parse -- All Files under one Folder (IpAddress must be in the beginning/end of the file name)

                var diagChildren = diagPath.Children()
                                            .Where(c => !ParserSettings.ExcludePathName(c.Name));
                var logTaskStartPos = 0;

                //Need to process nodetool ring files first
                var nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(ParserSettings.NodetoolRingFile));

                #region preprocessing File

                if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && nodetoolRingChildFiles.HasAtLeastOneElement())
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
                else
                {
                    Logger.Instance.Warn("Node/DSE tool Ring File is either missing or this option disabled. Data center information will be missing from the worksheets!");
                }

                nodetoolRingChildFiles = diagChildren.Where(c => c is IFilePath && c.Name.Contains(ParserSettings.DSEToolDir + "_" + ParserSettings.DSEtoolRingFile));

                if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && nodetoolRingChildFiles.HasAtLeastOneElement())
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

                if (ParserSettings.ParsingExcelOptions.ParseDDLFiles.IsEnabled() && diagPath.MakeFile(ParserSettings.CQLDDLDirFileExt, out cqlFilePath))
                {
                    foreach (IFilePath element in cqlFilePath.GetWildCardMatches()
                                                                .Where(c => !ParserSettings.ExcludePathName(c.Name)))
                    {
                        Program.ConsoleNonLogReadFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadParseCQLDDLParse(element);                        
                        parsedDDLList.TryAdd(((IFilePath)element).FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }

                    Program.ConsoleNonLogReadFiles.Increment("CQL DDL into Data table");
                    ProcessFileTasks.ProcessCQLDDLIntoDataTable(dtKeySpace, dtDDLTable, ParserSettings.IgnoreKeySpaces);
                    foreach (DataRow dataRow in dtDDLTable.Rows)
                    {
                        if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.Name == (dataRow["Name"] as string)))
                        {
                            kstblNames.Add(new CKeySpaceTableNames(dataRow));
                        }
                    }
                    Program.ConsoleNonLogReadFiles.TaskEnd("CQL DDL into Data table");
                }

                #endregion

                #region alternative file paths (DDL)

                if (ParserSettings.ParsingExcelOptions.ParseDDLFiles.IsEnabled()
                        && !string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeDDLFilePath);
                    IEnumerable<IFilePath> alterFiles;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches()
                                                .Where(p => p.IsFilePath && !ParserSettings.ExcludePathName(p.Name))
                                                .Cast<IFilePath>();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children()
                                                                    .Where(p => p.IsFilePath && ParserSettings.ExcludePathName(p.Name))
                                                                    .Cast<IFilePath>();
                    }
                    else
                    {
                        alterFiles = ParserSettings.ExcludePathName(alterPath.Name)
                                            ? Enumerable.Empty<IFilePath>()
                                            : new IFilePath[] { (IFilePath)alterPath };
                    }

                    Logger.Instance.InfoFormat("Queing {0} Alternative CQL Files: {1}",
                                                alterFiles.Count(),
                                                string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
                    foreach (IFilePath element in alterFiles)
                    {
                        Program.ConsoleNonLogReadFiles.Increment((IFilePath)element);

                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadParseCQLDDLParse(element);                        
                        parsedDDLList.TryAdd(((IFilePath)element).FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)element);
                        element.MakeEmpty();
                    }

                    Program.ConsoleNonLogReadFiles.Increment("CQL DDL into Data table");
                    ProcessFileTasks.ProcessCQLDDLIntoDataTable(dtKeySpace, dtDDLTable, ParserSettings.IgnoreKeySpaces);
                    foreach (DataRow dataRow in dtDDLTable.Rows)
                    {
                        if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.Name == (dataRow["Name"] as string)))
                        {
                            kstblNames.Add(new CKeySpaceTableNames(dataRow));
                        }
                    }
                    Program.ConsoleNonLogReadFiles.TaskEnd("CQL DDL into Data table");
                }

                #endregion
                #region alternative file paths (log)
                if (!string.IsNullOrEmpty(ParserSettings.AlternativeLogFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeLogFilePath);
                    IEnumerable<IFilePath> alterFiles = null;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children().Where(p => p.IsFilePath).Cast<IFilePath>();
                    }
                    else
                    {
                        alterFiles = new List<IFilePath>() { (IFilePath)alterPath };
                    }

                    Logger.Instance.InfoFormat("Queing {0} Alternative Log Files: {1}",
                                                    alterFiles.Count(),
                                                    string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
                    var logTasks = new List<Task<int>>();

                    alternativeDebugLogs.AddRange(alterFiles.Where(f => ProcessFileTasks.RegExIsDebugFile.IsMatch(f.FileName)));

                    foreach (IFilePath element in alterFiles.Where(f => !ProcessFileTasks.RegExIsDebugFile.IsMatch(f.FileName)))
                    {
                        string ipAddress;
                        string dcName;
                        var nodeInfoFound = ProcessFileTasks.DetermineIPDCFromFileName(element.FileName, dtRingInfo, out ipAddress, out dcName);

                        //No Node Info, look at parent directories...
                        if (!nodeInfoFound && element.ParentDirectoryPath != null)
                        {
                            nodeInfoFound = element.ParentDirectoryPath.PathResolved.Replace(diagPath.PathResolved, string.Empty)
                                                .Split(System.IO.Path.DirectorySeparatorChar)
                                                .Any(f => ProcessFileTasks.DetermineIPDCFromFileName(f, dtRingInfo, out ipAddress, out dcName));
                        }

                        if (nodeInfoFound)
                        {
                            if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && string.IsNullOrEmpty(dcName))
                            {
                                element.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                            }

                            var logTask = ProcessFileTasks.DetermineLogFilesAndProcess(null,
                                                                                        false,
                                                                                        element,
                                                                                        false,
                                                                                        dcName,
                                                                                        ipAddress,
                                                                                        dtLogsStack,
                                                                                        nodeGCInfo,
                                                                                        kstblNames,
                                                                                        dtLogStatusStack,
                                                                                        dtCFStatsStack,
                                                                                        dtNodeStatsStack);
                            logParsingTasks.Add(logTask);
                            logTasks.Add(logTask);
                            parsedLogList.TryAdd(ipAddress);
                        }
                    }

                    if (logTasks.Count > 0
                            && alternativeDebugLogs.HasAtLeastOneElement())
                    {
                        alternativeLogTask = Task<int>
                                                    .Factory
                                                    .ContinueWhenAll(logTasks.ToArray(), tasks => tasks.Sum(t => t.Result));
                    }
                    logTaskStartPos = logParsingTasks.Count();
                }

                #endregion

                Logger.Instance.InfoFormat("Queing {0} Files: {1}",
                                            diagChildren.Count(),
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
                            if (ParserSettings.ParsingExcelOptions.ParseCFStatsFiles.IsEnabled()
                                    && diagFile.Name.Contains(ParserSettings.NodetoolCFStatsFile))
                            {
                                #region Parse CFStats
                                if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && string.IsNullOrEmpty(dcName))
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
                                #endregion
                            }
                            else if (ParserSettings.ParsingExcelOptions.ParseTPStatsFiles.IsEnabled() && diagFile.Name.Contains(ParserSettings.NodetoolTPStatsFile))
                            {
                                #region Parse TPStats
                                if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && string.IsNullOrEmpty(dcName))
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
                                #endregion
                            }
                            else if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && diagFile.Name.Contains(ParserSettings.NodetoolInfoFile))
                            {
                                #region Ring Info
                                if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }
                                Program.ConsoleNonLogReadFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                ProcessFileTasks.ReadInfoFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtRingInfo);
                                parsedRingList.TryAdd(ipAddress);
                                Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)diagFile);
                                #endregion
                            }
                            else if (ParserSettings.ParsingExcelOptions.ParseCompacationHistFiles.IsEnabled() && diagFile.Name.Contains(ParserSettings.NodetoolCompactionHistFile))
                            {
                                #region Parse Compacation History
                                if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                Program.ConsoleNonLogReadFiles.Increment((IFilePath)diagFile);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFile.Path);
                                var dtCompHist = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCompactionHist + "-" + ipAddress);
                                dtCompHistStack.Push(dtCompHist);
                                ProcessFileTasks.ReadCompactionHistFileParseIntoDataTable((IFilePath)diagFile, ipAddress, dcName, dtCompHist, dtDDLTable, ParserSettings.IgnoreKeySpaces, kstblNames);
                                Program.ConsoleNonLogReadFiles.TaskEnd((IFilePath)diagFile);
                                #endregion
                            }
                            else if ((ParserSettings.LogParsingExcelOptions.Parse.IsEnabled() || ParserSettings.LogParsingExcelOptions.ParseArchivedLogs.IsEnabled())
                                        && diagFile.Name.Contains(ParserSettings.LogCassandraSystemLogFile)
                                        && !ParserSettings.ParsingExcelOptions.OnlyAlternativeLogs.CheckEnabled())
                            {
                                #region C* Log Parsing
                                if (string.IsNullOrEmpty(dcName))
                                {
                                    diagFile.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                    Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                                }

                                if(ProcessFileTasks.RegExIsDebugFile.IsMatch(((IFilePath)diagFile).FileName))
                                {
                                    alternativeDebugLogs.Add((IFilePath)diagFile);
                                }
                                else
                                {
                                    logParsingTasks.Add(ProcessFileTasks.DetermineLogFilesAndProcess(null,
                                                                                                        false,
                                                                                                        (IFilePath)diagFile,
                                                                                                        false,
                                                                                                        dcName,
                                                                                                        ipAddress,
                                                                                                        dtLogsStack,
                                                                                                        nodeGCInfo,
                                                                                                        kstblNames,
                                                                                                        dtLogStatusStack,
                                                                                                        dtCFStatsStack,
                                                                                                        dtNodeStatsStack));
                                    parsedLogList.TryAdd(ipAddress);
                                }
                                #endregion
                            }
                        }
                        else if (((IFilePath)diagFile).FileExtension.ToLower() != ".cql")
                        {
                            diagFile.Path.Dump(Logger.DumpType.Error, "File was Skipped");
                            Program.ConsoleErrors.Increment("File Skipped");
                        }
                    }
                });

                if(alternativeDebugLogs.Count > 0 && logTaskStartPos < logParsingTasks.Count)
                {
                    var logTasks = new List<Task<int>>() { alternativeLogTask };

                    logTasks.AddRange(logParsingTasks.TakeLast(logParsingTasks.Count - logTaskStartPos).Cast<Task<int>>());

                    alternativeLogTask = Task<int>
                                            .Factory
                                            .ContinueWhenAll(logTasks.ToArray(), tasks => tasks.Sum(t => t.Result));
                }

                if (kstblNames.Count == 0)
                {
                    Logger.Dump("DDL was not found which can cause missing information in the Excel workbooks.", Logger.DumpType.Warning);
                    Program.ConsoleWarnings.Increment("DDL Not Found");
                }

                #endregion
            }
            else
            {
                #region Read/Parse -- OpsCenter Diag Tar-Ball or Files located in separate folders where each folder's name is the IP Address

                var diagNodePath = diagPath.MakeChild(ParserSettings.DiagNodeDir) as Common.IDirectoryPath;
                List<Common.IDirectoryPath> nodeDirs;

                if (diagNodePath != null && (opsCtrDiag = diagNodePath.Exist()))
                {
                    var childrenItems = diagNodePath.Children()
                                                        .Where(c => !ParserSettings.ExcludePathName(c.Name));
                    var files = childrenItems.Where(item => item.IsFilePath).Cast<Common.IFilePath>();

                    if(files.HasAtLeastOneElement())
                    {
                        var filesInWarning = string.Format("Invalid File(s) Found in OpsCenter Folder ({1}): {0}",
                                                            string.Join(", ", files.Select(file => "\"" + file.Path + "\"")),
                                                            files.Count());
                        Logger.Instance.Warn(filesInWarning);
                        Program.ConsoleWarnings.Increment("Invalid File(s) Found in OpsCenter Folder");
                    }

                    nodeDirs = childrenItems.Where(item => item.IsDirectoryPath)
                                                                    .Cast<Common.IDirectoryPath>()
                                                                    .ToList();
                }
                else
                {
                    nodeDirs = diagPath.Children()
                                        .Where(c => c.IsDirectoryPath && !ParserSettings.ExcludePathName(c.Name))
                                        .Cast<Common.IDirectoryPath>().ToList();
                }

                nbrNodes = nodeDirs.Count;

                if (nbrNodes == 0)
                {
                    throw new System.IO.DirectoryNotFoundException(string.Format("No Node Directories Found within Folder \"{0}\".", diagPath.PathResolved));
                }

                IFilePath filePath = null;
                var preFilesProcessed = new bool[3];
                bool callResult = true;

                #region preparsing Files

                for (int fileIndex = 0;
                        fileIndex < nbrNodes && !preFilesProcessed.All(flag => flag);
                        ++fileIndex)
                {
                    if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled()
                            && !preFilesProcessed[0]
                            && MakeFile(nodeDirs[fileIndex], ParserSettings.NodetoolDir, ParserSettings.NodetoolRingFile, opsCtrDiag, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(filePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                            callResult = ProcessFileTasks.ReadRingFileParseIntoDataTables(filePath, dtRingInfo, dtTokenRange);
                            //parsedRingList.TryAdd(filePath.PathResolved);
                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);

                            if (!callResult || dtRingInfo == null || dtRingInfo.Rows.Count == 0)
                            {
                                Logger.Instance.InfoFormat("NodeTool Ring file \"{0}\" did not contain or invalid information. Trying next node folder", filePath.PathResolved);
                            }
                            else
                            {
                                preFilesProcessed[0] = true;
                            }
                        }
                        else
                        {
                            Logger.Instance.InfoFormat("NodeTool Ring file for \"{0}\" is missing. Trying next node folder", filePath.PathResolved);
                        }
                    }

                    if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled()
                            && !preFilesProcessed[1]
                            && MakeFile(nodeDirs[fileIndex], ParserSettings.DSEToolDir, ParserSettings.DSEtoolRingFile, opsCtrDiag, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(filePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                            callResult = ProcessFileTasks.ReadDSEToolRingFileParseIntoDataTable(filePath, dtRingInfo);
                            //parsedRingList.TryAdd(filePath.PathResolved);
                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);
                            if (!callResult || dtRingInfo == null || dtRingInfo.Rows.Count == 0)
                            {
                                Logger.Instance.InfoFormat("DSETool Ring file \"{0}\" did not contain or invalid information. Trying next node folder", filePath.PathResolved);
                            }
                            else
                            {
                                preFilesProcessed[1] = true;
                            }
                        }
                        else
                        {
                            Logger.Instance.InfoFormat("DSETool Ring file for \"{0}\" is missing. Trying next node folder", filePath.PathResolved);
                        }
                    }

                    if (ParserSettings.ParsingExcelOptions.ParseDDLFiles.IsEnabled()
                            //&& !preFilesProcessed[2]
                            && nodeDirs[fileIndex].MakeFile(ParserSettings.CQLDDLDirFile, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(filePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                            ProcessFileTasks.ReadParseCQLDDLParse(filePath);
                            parsedDDLList.TryAdd(filePath.PathResolved);
                            Program.ConsoleNonLogReadFiles.TaskEnd(filePath);

                            //preFilesProcessed[2] = true;
                        }
                        else
                        {
                            //Look for DDL in OpsCenter 6 folder structure
                            if (nodeDirs[fileIndex].MakeFile(Properties.Settings.Default.CQLDDLDirFileOpsCtr6, out filePath)
                                    && filePath.Exist())
                            {
                                Program.ConsoleNonLogReadFiles.Increment(filePath);
                                Logger.Instance.InfoFormat("Processing File \"{0}\"", filePath.Path);
                                ProcessFileTasks.ReadParseCQLDDLParse(filePath);
                                parsedDDLList.TryAdd(filePath.PathResolved);
                                Program.ConsoleNonLogReadFiles.TaskEnd(filePath);
                            }
                            else
                            {
                                Logger.Instance.InfoFormat("CQL DDL file for \"{0}\" is missing. Trying next node folder", filePath.PathResolved);
                            }
                        }
                    }
                }

                Program.ConsoleNonLogReadFiles.Increment("CQL DDL into Data table");
                Logger.Instance.InfoFormat("Processing CQL DDL into DataTable");
                ProcessFileTasks.ProcessCQLDDLIntoDataTable(dtKeySpace, dtDDLTable, ParserSettings.IgnoreKeySpaces);
                Logger.Instance.InfoFormat("Inserted {0} CQL DDLs into DataTable", dtDDLTable.Rows.Count + dtKeySpace.Rows.Count);
                foreach (DataRow dataRow in dtDDLTable.Rows)
                {
                    if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.Name == (dataRow["Name"] as string)))
                    {
                        kstblNames.Add(new CKeySpaceTableNames(dataRow));
                    }
                }
                Program.ConsoleNonLogReadFiles.TaskEnd("CQL DDL into Data table");

                #endregion

                #region alternative file paths (DDL)

                if (ParserSettings.ParsingExcelOptions.ParseDDLFiles.IsEnabled() && !string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeDDLFilePath);
                    IEnumerable<IFilePath> alterFiles;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches()
                                                .Where(p => p.IsFilePath && !ParserSettings.ExcludePathName(p.Name))
                                                .Cast<IFilePath>();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children()
                                                                    .Where(p => p.IsFilePath && !ParserSettings.ExcludePathName(p.Name))
                                                                    .Cast<IFilePath>();
                    }
                    else
                    {
                        alterFiles = ParserSettings.ExcludePathName(alterPath.Name)
                                        ? Enumerable.Empty<IFilePath>()
                                        : new IFilePath[] { (IFilePath)alterPath };
                    }

                    Logger.Instance.InfoFormat("Queing {0} Alternative CQL Files: {1}",
                                                alterFiles.Count(),
                                                string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
                    foreach (IFilePath element in alterFiles)
                    {
                        Program.ConsoleNonLogReadFiles.Increment(element);
                        Logger.Instance.InfoFormat("Processing File \"{0}\"", element.Path);
                        ProcessFileTasks.ReadParseCQLDDLParse(element);
                        parsedDDLList.TryAdd(filePath.FileNameWithoutExtension);
                        Program.ConsoleNonLogReadFiles.TaskEnd(element);
                        element.MakeEmpty();
                    }

                    Program.ConsoleNonLogReadFiles.Increment("CQL DDL into Data table");
                    Logger.Instance.InfoFormat("Processing CQL DDL into DataTable");
                    ProcessFileTasks.ProcessCQLDDLIntoDataTable(dtKeySpace, dtDDLTable, ParserSettings.IgnoreKeySpaces);
                    Logger.Instance.InfoFormat("Inserted {0} CQL DDLs into DataTable", dtDDLTable.Rows.Count + dtKeySpace.Rows.Count);
                    foreach (DataRow dataRow in dtDDLTable.Rows)
                    {
                        if (!kstblNames.Exists(item => item.KeySpaceName == (dataRow["Keyspace Name"] as string) && item.Name == (dataRow["Name"] as string)))
                        {
                            kstblNames.Add(new CKeySpaceTableNames(dataRow));
                        }
                    }
                    Program.ConsoleNonLogReadFiles.TaskEnd("CQL DDL into Data table");
                }

                if (kstblNames.Count == 0)
                {
                    //We need to have a list of valid Keyspaces and Tables...
                    if (opsCtrDiag
							? nodeDirs.First().Clone().AddChild(ParserSettings.NodetoolDir).MakeFile(ParserSettings.NodetoolCFStatsFile, out filePath)
							: nodeDirs.First().MakeFile(ParserSettings.NodetoolDir + "." + ParserSettings.NodetoolCFStatsFile, out filePath))
                    {
                        if (filePath.Exist())
                        {
                            filePath.Path.Dump(Logger.DumpType.Warning, "DDL was not found, parsing a CFStats file to obtain data model information");
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
				#endregion
				#region alternative file paths (log)
				if (!string.IsNullOrEmpty(ParserSettings.AlternativeLogFilePath))
                {
                    var alterPath = Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeLogFilePath);
                    IEnumerable<IFilePath> alterFiles = null;

                    if (alterPath.HasWildCardPattern())
                    {
                        alterFiles = alterPath.GetWildCardMatches().Where(p => p.IsFilePath).Cast<IFilePath>();
                    }
                    else if (alterPath.IsDirectoryPath)
                    {
                        alterFiles = ((IDirectoryPath)alterPath).Children().Where(p => p.IsFilePath).Cast<IFilePath>();
                    }
                    else
                    {
                        alterFiles = new List<IFilePath>() { (IFilePath)alterPath };
                    }

                    Logger.Instance.InfoFormat("Queing {0} Alternative Log Files: {1}",
                                                    alterFiles.Count(),
                                                    string.Join(", ", alterFiles.Select(p => p.Name).Sort()));
                    var logTasks = new List<Task<int>>();

                    alternativeDebugLogs.AddRange(alterFiles.Where(f => ProcessFileTasks.RegExIsDebugFile.IsMatch(f.FileName)));

                    foreach (IFilePath element in alterFiles.Where(f => !ProcessFileTasks.RegExIsDebugFile.IsMatch(f.FileName)))
                    {
                        string ipAddress;
                        string dcName;
						var nodeInfoFound = ProcessFileTasks.DetermineIPDCFromFileName(element.FileName, dtRingInfo, out ipAddress, out dcName);

						//No Node Info, look at parent directories...
						if(!nodeInfoFound && element.ParentDirectoryPath != null)
						{
							nodeInfoFound = element.ParentDirectoryPath.PathResolved.Replace(diagPath.PathResolved, string.Empty)
												.Split(System.IO.Path.DirectorySeparatorChar)
												.Any(f => ProcessFileTasks.DetermineIPDCFromFileName(f, dtRingInfo, out ipAddress, out dcName));
						}

						if (nodeInfoFound)
                        {
                            if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && string.IsNullOrEmpty(dcName))
                            {
                                element.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                                Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                            }

                            var logTask = ProcessFileTasks.DetermineLogFilesAndProcess(null,
                                                                                        false,
                                                                                        element,
                                                                                        false,
                                                                                        dcName,
                                                                                        ipAddress,
                                                                                        dtLogsStack,
                                                                                        nodeGCInfo,
                                                                                        kstblNames,
                                                                                        dtLogStatusStack,
                                                                                        dtCFStatsStack,
                                                                                        dtNodeStatsStack);
                            logParsingTasks.Add(logTask);
                            logTasks.Add(logTask);
                            parsedLogList.TryAdd(ipAddress);
                        }
                    }

                    if (logTasks.Count > 0
                            && alternativeDebugLogs.HasAtLeastOneElement())
                    {
                        alternativeLogTask = Task<int>
                                                    .Factory
                                                    .ContinueWhenAll(logTasks.ToArray(), tasks => tasks.Sum(t => t.Result));
                    }                        
                }

                #endregion

                Logger.Instance.InfoFormat("Queing {0} Nodes: {1}",
                                            nbrNodes,
                                            string.Join(", ", nodeDirs.Select(p => p.Name).Sort()));

                if(dtRingInfo == null || dtRingInfo.Rows.Count == 0)
                {
                    Logger.Instance.Warn("Node/DSE tool Ring File is either missing or this option disabled. Data center information will be missing from the worksheets!");
                }

                Parallel.ForEach(nodeDirs, (element) =>
                //foreach (var element in nodeDirs)
                {
                    string ipAddress = null;
                    string dcName = null;
                    IFilePath diagFilePath = null;

                    ProcessFileTasks.DetermineIPDCFromFileName(element.Name, dtRingInfo, out ipAddress, out dcName);

                    if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled()
                            && string.IsNullOrEmpty(dcName))
                    {
                        element.Path.Dump(Logger.DumpType.Warning, "DataCenter Name was not found in the Ring file.");
                        Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                    }

                    #region Parse OS/Machine info
                    if (ParserSettings.ParsingExcelOptions.ParseMachineInfoFiles.IsEnabled())
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
                    #endregion
                    #region Parse CF Stats
                    if (ParserSettings.ParsingExcelOptions.ParseCFStatsFiles.IsEnabled()
                            && MakeFile(element, ParserSettings.NodetoolDir, ParserSettings.NodetoolCFStatsFile, opsCtrDiag, out diagFilePath))
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
                        else
                        {
                            Logger.Instance.DebugFormat("File \"{0}\" does not exists.", diagFilePath.Path);
                        }
                    }
                    #endregion
                    #region Parse TPStats
                    if (ParserSettings.ParsingExcelOptions.ParseTPStatsFiles.IsEnabled()
                            && MakeFile(element, ParserSettings.NodetoolDir, ParserSettings.NodetoolTPStatsFile, opsCtrDiag, out diagFilePath))
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
                        else
                        {
                            Logger.Instance.DebugFormat("File \"{0}\" does not exists.", diagFilePath.Path);
                        }
                    }
                    #endregion
                    #region Parse Ring Info
                    if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled()
                            && MakeFile(element, ParserSettings.NodetoolDir, ParserSettings.NodetoolInfoFile, opsCtrDiag, out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            ProcessFileTasks.ReadInfoFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtRingInfo);
                            parsedRingList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                        else
                        {
                            Logger.Instance.DebugFormat("File \"{0}\" does not exists.", diagFilePath.Path);
                        }
                    }
                    #endregion
                    #region Parse Compaction History
                    if (ParserSettings.ParsingExcelOptions.ParseCompacationHistFiles.IsEnabled()
                            && MakeFile(element, ParserSettings.NodetoolDir, ParserSettings.NodetoolCompactionHistFile,opsCtrDiag ,out diagFilePath))
                    {
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var dtHistComp = new System.Data.DataTable(ParserSettings.ExcelWorkSheetCompactionHist + "-" + ipAddress);
                            dtCompHistStack.Push(dtHistComp);
                            ProcessFileTasks.ReadCompactionHistFileParseIntoDataTable(diagFilePath, ipAddress, dcName, dtHistComp, dtDDLTable, ParserSettings.IgnoreKeySpaces, kstblNames);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                        else
                        {
                            Logger.Instance.DebugFormat("File \"{0}\" does not exists.", diagFilePath.Path);
                        }
                    }
                    #endregion
                    #region Parse C* Log
                    if ((ParserSettings.LogParsingExcelOptions.Parse.IsEnabled() || ParserSettings.LogParsingExcelOptions.ParseArchivedLogs.IsEnabled())
                            && !ParserSettings.ParsingExcelOptions.OnlyAlternativeLogs.CheckEnabled())
                    {
                        var logTasks = new List<Task<int>>();

                        foreach (var logDir in ParserSettings.LogCassandraDirSystemLogs)
                        {
                            if (opsCtrDiag
									? element.MakeChild(ParserSettings.LogsDir).MakeFile(logDir, out diagFilePath)
									: element.MakeFile(logDir, out diagFilePath))
							{
                                if (diagFilePath.Exist())
                                {
                                    var logTask = ProcessFileTasks.DetermineLogFilesAndProcess(null,
                                                                                                false,
                                                                                                diagFilePath,
                                                                                                true,
                                                                                                dcName,
                                                                                                ipAddress,
                                                                                                dtLogsStack,
                                                                                                nodeGCInfo,
                                                                                                kstblNames,
                                                                                                dtLogStatusStack,
                                                                                                dtCFStatsStack,
                                                                                                dtNodeStatsStack);
                                    logParsingTasks.Add(logTask);
                                    logTasks.Add(logTask);
                                    parsedLogList.TryAdd(ipAddress);
                                }
                                else
                                {
                                    Logger.Instance.DebugFormat("Log File \"{0}\" does not exists.", diagFilePath.Path);
                                }
                            }
                        }

                        if (logTasks.Count > 0)
                        {
                            var completedLogTask = Task<int>
                                                    .Factory
                                                    .ContinueWhenAll(logTasks.ToArray(), tasks => tasks.Sum(t => t.Result));

                            foreach (var logDir in ParserSettings.LogCassandraDebugDirSystemLogs)
                            {
                                if (opsCtrDiag
                                        ? element.MakeChild(ParserSettings.LogsDir).MakeFile(logDir, out diagFilePath)
                                        : element.MakeFile(logDir, out diagFilePath))
                                {
                                    if (diagFilePath.Exist())
                                    {
                                        logParsingTasks.Add(ProcessFileTasks.DetermineLogFilesAndProcess(completedLogTask,
                                                                                                            true,
                                                                                                            diagFilePath,
                                                                                                            true,
                                                                                                            dcName,
                                                                                                            ipAddress,
                                                                                                            dtLogsStack,
                                                                                                            nodeGCInfo,
                                                                                                            kstblNames,
                                                                                                            dtLogStatusStack,
                                                                                                            dtCFStatsStack,
                                                                                                            dtNodeStatsStack));
                                        parsedLogList.TryAdd(ipAddress);
                                    }
                                    else
                                    {
                                        Logger.Instance.DebugFormat("Log Debug File \"{0}\" does not exists.", diagFilePath.Path);
                                    }
                                }
                            }
                        }
                    }
                    #endregion
                    #region Parse Yaml
                    if (ParserSettings.ParsingExcelOptions.ParseYamlFiles.IsEnabled()
                            && (opsCtrDiag
									? element.MakeChild(ParserSettings.ConfCassandraDir).MakeFile(ParserSettings.ConfCassandraFile, out diagFilePath)
									: element.MakeFile(ParserSettings.ConfCassandraFile, out diagFilePath)))

					{
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, diagFilePath.FileName, yamlList);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                        else
                        {
                            Logger.Instance.DebugFormat("File \"{0}\" does not exists.", diagFilePath.Path);
                        }
                    }
                    
                    if (ParserSettings.ParsingExcelOptions.ParseYamlFiles.IsEnabled()
                            && (opsCtrDiag
									? element.MakeChild(ParserSettings.ConfDSEDir).MakeFile(ParserSettings.ConfDSEYamlFile, out diagFilePath)
									: element.MakeFile(ParserSettings.ConfDSEYamlFile, out diagFilePath)))

					{
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, diagFilePath.FileName, yamlList);
                            parsedYamlList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                        else
                        {
                            Logger.Instance.DebugFormat("File \"{0}\" does not exists.", diagFilePath.Path);
                        }
                    }

                    if (ParserSettings.ParsingExcelOptions.ParseYamlFiles.IsEnabled()
                            && (opsCtrDiag
									? element.MakeChild(ParserSettings.ConfDSEDir).MakeFile(ParserSettings.ConfDSEFile, out diagFilePath)
									: element.MakeFile(ParserSettings.ConfDSEFile, out diagFilePath)))
					{
                        if (diagFilePath.Exist())
                        {
                            Program.ConsoleNonLogReadFiles.Increment(diagFilePath);
                            Logger.Instance.InfoFormat("Processing File \"{0}\"", diagFilePath.Path);
                            var yamlList = new List<YamlInfo>();
                            listCYamlStack.Push(yamlList);
                            ProcessFileTasks.ReadYamlFileParseIntoList(diagFilePath, ipAddress, dcName, diagFilePath.FileName, yamlList);
                            parsedYamlList.TryAdd(ipAddress);
                            Program.ConsoleNonLogReadFiles.TaskEnd(diagFilePath);
                        }
                        else
                        {
                            Logger.Instance.DebugFormat("File \"{0}\" does not exists.", diagFilePath.Path);
                        }
                    }
                    #endregion
                });               
            #endregion
            }

            #region Alternative Debug Logs

            if (alternativeDebugLogs.Count > 0)
            {
                foreach (IFilePath element in alternativeDebugLogs)
                {
                    string ipAddress;
                    string dcName;
                    var nodeInfoFound = ProcessFileTasks.DetermineIPDCFromFileName(element.FileName, dtRingInfo, out ipAddress, out dcName);

                    //No Node Info, look at parent directories...
                    if (!nodeInfoFound && element.ParentDirectoryPath != null)
                    {
                        nodeInfoFound = element.ParentDirectoryPath.PathResolved.Replace(diagPath.PathResolved, string.Empty)
                                            .Split(System.IO.Path.DirectorySeparatorChar)
                                            .Any(f => ProcessFileTasks.DetermineIPDCFromFileName(f, dtRingInfo, out ipAddress, out dcName));
                    }

                    if (nodeInfoFound)
                    {
                        if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled() && string.IsNullOrEmpty(dcName))
                        {
                            element.Path.Dump(Logger.DumpType.Warning, "A DataCenter Name was not found in the associated IP Address in the Ring File.");
                            Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                        }

                        var logTask = ProcessFileTasks.DetermineLogFilesAndProcess(alternativeLogTask,
                                                                                    true,
                                                                                    element,
                                                                                    false,
                                                                                    dcName,
                                                                                    ipAddress,
                                                                                    dtLogsStack,
                                                                                    nodeGCInfo,
                                                                                    kstblNames,
                                                                                    dtLogStatusStack,
                                                                                    dtCFStatsStack,
                                                                                    dtNodeStatsStack);
                        logParsingTasks.Add(logTask);
                        parsedLogList.TryAdd(ipAddress);
                    }
                }
            }
            #endregion

            #region cfHistogram

            bool parseCFHistFiles = false;
            IFilePath cfHistogramWildFilePath;
            IFilePath tableHistogramWildFilePath;
            IDirectoryPath tableHistogramDir = null;
            Task<IEnumerable<IFilePath>> cfhistFilesTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask( (IEnumerable<IFilePath>) new IFilePath[0]);

            if (ParserSettings.ParsingExcelOptions.ParseTblHistogramFiles.IsEnabled()
                    && !string.IsNullOrEmpty(ParserSettings.TableHistogramDirPath))
            {
                tableHistogramDir = Common.Path.PathUtils.BuildDirectoryPath(ParserSettings.TableHistogramDirPath);

                if(tableHistogramDir.IsRelativePath)
                {
                    IAbsolutePath absPath;

                    if(diagPath.MakePathFrom((IRelativePath)tableHistogramDir, out absPath))
                    {
                        tableHistogramDir = (IDirectoryPath) absPath;
                    }
                }
            }

            if(tableHistogramDir != null
                && tableHistogramDir.Exist())
            {
                cfhistFilesTask = cfhistFilesTask.ContinueWith(filesMatchesTask =>
                                        {
                                            var files = tableHistogramDir.Children()
                                                                            .Where(file => file.IsFilePath && !ParserSettings.ExcludePathName(file.Name))
                                                                            .Cast<IFilePath>();

                                            return filesMatchesTask.Result.Append(files.ToArray());
                                        },
                                       TaskContinuationOptions.AttachedToParent
                                           | TaskContinuationOptions.OnlyOnRanToCompletion);
                parseCFHistFiles = true;
            }
            else
            {
                if (diagPath.MakeFile(string.Format("*{0}*", ParserSettings.CFHistogramFileName), out cfHistogramWildFilePath))
                {
                    cfhistFilesTask = cfhistFilesTask.ContinueWith(wildFileMatchesTask =>
                                            {
                                                var tblMatches = cfHistogramWildFilePath.GetWildCardMatches()
                                                                                            .Where(p => p.IsFilePath && !ParserSettings.ExcludePathName(p.Name))
                                                                                            .Cast<IFilePath>();

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
                                                var tblMatches = tableHistogramWildFilePath.GetWildCardMatches()
                                                                                                .Where(p => p.IsFilePath && !ParserSettings.ExcludePathName(p.Name))
                                                                                                .Cast<IFilePath>();

                                                return wildFileMatchesTask.Result.Append(tblMatches.ToArray());
                                            },
                                        TaskContinuationOptions.AttachedToParent
                                            | TaskContinuationOptions.OnlyOnRanToCompletion);
                    parseCFHistFiles = true;
                }
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

                                                var dataTable = new DataTable(ParserSettings.ExcelCFHistogramWorkSheet + "-" + ipAddress);
                                                dtCFHistogramsStack.Push(dataTable);

                                                Program.ConsoleNonLogReadFiles.Increment(chFile);

                                                ProcessFileTasks.ReadCFHistogramFileParseIntoDataTable(chFile, ipAddress, dcName, dtRingInfo, dataTable);
                                                parsedCFHistList.TryAdd(ipAddress);
                                                Program.ConsoleNonLogReadFiles.TaskEnd(chFile);
                                            });
                                        }

                                        return dtCFHistogramsStack.MergeIntoOneDataTable();
                                    },
                                    TaskContinuationOptions.AttachedToParent                                        
                                        | TaskContinuationOptions.OnlyOnRanToCompletion);
            }
            else
            {
                Logger.Instance.DebugFormat("TableHistogram Directory \"{0}\" does not exists.", tableHistogramDir?.Path);
            }

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
			#endregion

			#region Tasks

			Task<DataTable> runLogMergedTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();
            Task<Tuple<DataTable, DataTable, DateTimeRange>> runSummaryLogTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<Tuple<DataTable, DataTable, DateTimeRange>>();
            Task<DataTable> runNodeStatsMergedTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();
			Task<Tuple<DataTable, DataTable, DataTable, Common.Patterns.Collections.ThreadSafe.Dictionary<string, string>>> updateRingWYamlInfoTask
								= Common.Patterns.Tasks.CompletionExtensions.CompletedTask(new Tuple<DataTable, DataTable, DataTable, Common.Patterns.Collections.ThreadSafe.Dictionary<string, string>>(dtOSMachineInfo, dtRingInfo, dtYaml, nodeGCInfo));
            Task<DataTable> runCFStatsMergedDDLUpdated = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();
			Task runAntiCompactionTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();
			Task runMemTableFlushTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();
			Task<DataTable> runReadRepairTbl = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();
			Task<DataTable> runStatsLogMerged = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();
			Task runReleaseDependentLogTask;
			Task runConcurrentCompactionFlushTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();
            Task<DataTable> runLogAdditionalInfoTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();
            Task<DataTable> runCompHistMergeTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<DataTable>();

            {
                var runYamlListIntoDTTask = ParserSettings.ParsingExcelOptions.ParseYamlFiles.IsEnabled()
                                                ? Task.Factory.StartNew(() =>
                                                    {
                                                        Program.ConsoleParsingNonLog.Increment("Yaml");
                                                        ProcessFileTasks.ParseYamlListIntoDataTable(listCYamlStack, dtYaml);
                                                        Program.ConsoleParsingNonLog.TaskEnd("Yaml");
                                                    },
                                                     TaskCreationOptions.LongRunning)
                                                : Common.Patterns.Tasks.CompletionExtensions.CompletedTask();

				if (ParserSettings.ParsingExcelOptions.ParseOpsCenterFiles.IsEnabled())
				{
					updateRingWYamlInfoTask = runYamlListIntoDTTask.ContinueWith(task =>
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
														return new Tuple<DataTable, DataTable, DataTable, Common.Patterns.Collections.ThreadSafe.Dictionary<string, string>>(dtOSMachineInfo, dtRingInfo, dtYaml, nodeGCInfo);

													},
												TaskContinuationOptions.AttachedToParent													
													| TaskContinuationOptions.OnlyOnRanToCompletion);
				}

                Task<int> runningLogTask = logParsingTasks.Count == 0
                                            ? Common.Patterns.Tasks.CompletionExtensions.CompletedTask<int>()
                                            : Task<int>
                                                .Factory
                                                .ContinueWhenAll(logParsingTasks.ToArray(), tasks =>
                                                {
                                                    Logger.Instance.InfoFormat("Duplicated Log Events {0:###,###,##0}, Debug Log Events {1:###,###,##0}",
                                                                                    ProcessFileTasks.NbrDuplicatedLogEventsTotal,
                                                                                    ProcessFileTasks.NbrDuplicatedDebugLogEventsTotal);

                                                    ProcessFileTasks.LogLinesHash.Clear();
                                                    return tasks.Sum(t => ((Task<int>)t).Result);
                                                });
				Task<IEnumerable<ProcessFileTasks.RepairLogInfo>> runReadRepairProcess = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<IEnumerable<ProcessFileTasks.RepairLogInfo>>();

                if (ParserSettings.ParsingExcelOptions.ParseCompacationHistFiles.IsEnabled())
                {
                    runCompHistMergeTask = Task.Factory.StartNew<DataTable>( () =>
                    {
                        Program.ConsoleParsingNonLog.Increment("Compaction History Merge");
                        var dtCompHist = dtCompHistStack.MergeIntoOneDataTable(ParserSettings.CompactionHistWorksheetFilterSort);
                        ProcessFileTasks.UpdateTZCompactionHistDataTable(dtCompHist, dtOSMachineInfo); 
                        Program.ConsoleParsingNonLog.TaskEnd("Compaction History Merge");

                        return dtCompHist;
                    });
                }

                if (logParsingTasks.Count > 0)
                {
                    if ((ParserSettings.LogParsingExcelOptions.Parse.IsEnabled() || ParserSettings.LogParsingExcelOptions.ParseArchivedLogs.IsEnabled()))
                    {
                        runLogMergedTask = runningLogTask.ContinueWith(action =>
                                            {
                                                Program.ConsoleLogReadFiles.Terminate();
                                                Logger.Instance.InfoFormat("Log {0}", ProcessFileTasks.LogCassandraMaxMinTimestamp);
                                                Logger.Instance.InfoFormat("Parsed Log Data for {0} Nodes: {1}",
                                                                            parsedLogList.Count,
                                                                            string.Join(", ", parsedLogList.Sort<string>()));
                                            })
                                        .ContinueWith(action =>
                                            {
                                                Program.ConsoleParsingLog.Increment("Update Node Info");
                                                ProcessFileTasks.UpdateRingInfo(dtRingInfo,
                                                                                ProcessFileTasks.LogCassandraNodeMaxMinTimestamps);
                                                Program.ConsoleParsingLog.TaskEnd("Update Node Info");
                                            },
                                            TaskContinuationOptions.AttachedToParent                                               
                                                | TaskContinuationOptions.OnlyOnRanToCompletion)
                                        .ContinueWith(action =>
                                            {
												Program.ConsoleParsingLog.Increment("Log Merge");
												var dtlog = dtLogsStack.MergeIntoOneDataTable(new Tuple<string, string, DataViewRowState>(ParserSettings.LogExcelWorkbookFilter,
                                                                                                                                                    "[Data Center], [Timestamp] DESC",
                                                                                                                                                    DataViewRowState.CurrentRows));
                                                Program.ConsoleParsingLog.TaskEnd("Log Merge");

                                                return dtlog;
                                            },
                                            TaskContinuationOptions.AttachedToParent                                                
                                                | TaskContinuationOptions.OnlyOnRanToCompletion);

						runAntiCompactionTask = Task.Factory.ContinueWhenAll(new Task<DataTable>[]
                                                                                { runLogMergedTask, runCompHistMergeTask},
                                                
                                                logTasks =>
												{
													Program.ConsoleParsingLog.Increment("AntiCompaction Processing");
													ProcessFileTasks.ParseAntiCompactionFromLog(logTasks[0].Result,
                                                                                                logTasks[1].Result,
																								ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled()
																									? dtLogStatusStack
																									: null,
																								ParserSettings.ParsingExcelOptions.ParseCFStatsLogs.IsEnabled()
																									? dtCFStatsStack
																									: null,
																								ParserSettings.IgnoreKeySpaces);

													Program.ConsoleParsingLog.TaskEnd("AntiCompaction Processing");
												});

						runMemTableFlushTask = runLogMergedTask.ContinueWith(logTask =>
												{
													Program.ConsoleParsingLog.Increment("MemTable Flush Processing");
													ProcessFileTasks.ParseMemTblFlushFromLog(logTask.Result,
																								ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled()
																									? dtLogStatusStack
																									: null,
																								ParserSettings.ParsingExcelOptions.ParseCFStatsLogs.IsEnabled()
																									? dtCFStatsStack
																									: null,
																								kstblNames,
																								ParserSettings.IgnoreKeySpaces,
																								ParserSettings.CompactionFlagThresholdInMS,
																								ParserSettings.CompactionFlagThresholdAsIORate);

													Program.ConsoleParsingLog.TaskEnd("MemTable Flush Processing");
												},
												TaskContinuationOptions.AttachedToParent													
													| TaskContinuationOptions.OnlyOnRanToCompletion);

						if (ParserSettings.ParsingExcelOptions.ParseReadRepairs.IsEnabled())
						{
							runReadRepairProcess = Task.Factory.ContinueWhenAll(new Task[] { runLogMergedTask, runAntiCompactionTask, runMemTableFlushTask }, tasks => ((Task<DataTable>)tasks[0]).Result)
																.ContinueWith(logTask =>
																{
																	Program.ConsoleParsingLog.Increment("Read Repair Processing");
																	var readRepairCollection = ProcessFileTasks.ParseReadRepairFromLog(logTask.Result,
																																		ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled()
																																			? dtLogStatusStack
																																			: null,
																																		ParserSettings.ParsingExcelOptions.ParseCFStatsLogs.IsEnabled()
																																			? dtCFStatsStack
																																			: null,
																																		ParserSettings.IgnoreKeySpaces,
																																		ParserSettings.ReadRepairThresholdInMS);

																	Program.ConsoleParsingLog.TaskEnd("Read Repair Processing");

																	return readRepairCollection;
																},
															   TaskContinuationOptions.AttachedToParent																   
																   | TaskContinuationOptions.OnlyOnRanToCompletion);

							if (ParserSettings.ParsingExcelOptions.LoadReadRepairWorkSheets.IsEnabled())
							{
								runReadRepairTbl = runReadRepairProcess.ContinueWith(readRepairTask =>
													{
														Program.ConsoleParsingLog.Increment("Read Repair Table");
														var dtReadRepair = new DataTable("ReadRepair");

														ProcessFileTasks.ReadRepairIntoDataTable(readRepairTask.Result, dtReadRepair);

														Program.ConsoleParsingLog.TaskEnd("Read Repair Table");

														return dtReadRepair;
													},
											   TaskContinuationOptions.AttachedToParent												   
												   | TaskContinuationOptions.OnlyOnRanToCompletion);
							}
						}

						var runContGCTask = Task.Factory.ContinueWhenAll(new Task[] { runLogMergedTask, runMemTableFlushTask, runReadRepairProcess, runAntiCompactionTask }, tasks => ((Task<DataTable>)tasks[0]).Result)
                                                    .ContinueWith(action =>
													{
														Program.ConsoleParsingLog.Increment("Continuous GC");
														var dtTPStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetNodeStats + "-" + "Continuous GC");
														dtNodeStatsStack.Push(dtTPStats);
                                                        //Warning Log data table can be updated!
                                                        ProcessFileTasks.DetectContinuousGCIntoNodeStats(dtTPStats,
                                                                                                            action.Result,
																											ParserSettings.ToleranceContinuousGCInMS,
																											ParserSettings.ContinuousGCNbrInSeries,
																											ParserSettings.GCTimeFrameDetection,
																											ParserSettings.GCTimeFrameDetectionPercentage);
														Program.ConsoleParsingLog.TaskEnd("Continuous GC");
													},
												   TaskContinuationOptions.AttachedToParent
													   | TaskContinuationOptions.LongRunning
													   | TaskContinuationOptions.OnlyOnRanToCompletion);
						runConcurrentCompactionFlushTask = Task.Factory.ContinueWhenAll(new Task[] { runLogMergedTask, runContGCTask, runMemTableFlushTask, runReadRepairProcess, runAntiCompactionTask }, tasks => ((Task<DataTable>)tasks[0]).Result)
																.ContinueWith(logTask =>
																{
																	Program.ConsoleParsingLog.Increment("Concurrent Compaction/Memtable Flush Processing");

                                                                    //Warning Log data table can be updated!
																	ProcessFileTasks.ConcurrentCompactionFlush(ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled()
																													? dtLogStatusStack
																													: null,
																												ParserSettings.ParsingExcelOptions.ParseNodeStatsLogs.IsEnabled()
																													? dtNodeStatsStack
																													: null,
                                                                                                                logTask.Result);

																	Program.ConsoleParsingLog.TaskEnd("Concurrent Compaction/Memtable Flush Processing");
																},
															   TaskContinuationOptions.AttachedToParent																   
																   | TaskContinuationOptions.OnlyOnRanToCompletion);


						runNodeStatsMergedTask = Task.Factory.ContinueWhenAll(new Task[] { runningLogTask, runContGCTask, runConcurrentCompactionFlushTask }, ignoreItem => { })
															.ContinueWith(action =>
															{
																Program.ConsoleParsingLog.Increment("Node Stats Log Merge");
																var dtNodeStatslog = dtNodeStatsStack.MergeIntoOneDataTable(new Tuple<string, string, DataViewRowState>(null, "[Data Center], [Node IPAddress]", DataViewRowState.CurrentRows));
																Program.ConsoleParsingLog.TaskEnd("Node Stats Log Merge");
																return dtNodeStatslog;
															},
															TaskContinuationOptions.AttachedToParent																
																| TaskContinuationOptions.OnlyOnRanToCompletion);
                    }
                    
                    ProcessFileTasks.LogCassandraNodeMaxMinTimestamps.ForEach(nodeLogRanges
                        => Logger.Instance.InfoFormat("Log IP: {0} Range(s): {1}",
                                                        nodeLogRanges.Key,
                                                        string.Join(", ", nodeLogRanges.Value.OrderBy(s => s.LogRange))));
                }
                else
                {
					runNodeStatsMergedTask = Task.Factory.StartNew<DataTable>(() =>
                                            {
												Program.ConsoleParsingLog.Increment("Node Stats Log Merge");
												var dtNodeStatslog = dtNodeStatsStack.MergeIntoOneDataTable(new Tuple<string, string, DataViewRowState>(null, "[Data Center], [Node IPAddress]", DataViewRowState.CurrentRows));
                                                Program.ConsoleParsingLog.TaskEnd("Node Stats Log Merge");
                                                return dtNodeStatslog;
                                            });
                }
                
                tskdtCFHistogram = tskdtCFHistogram.ContinueWith(taskResult =>
                                    {
										Program.ConsoleParsingNonLog.Increment("TableHistogram => CFStats...");
										var dtCFHist = taskResult.Result;

                                        if (dtCFHist.Rows.Count > 0)
                                        {
											var dtCFStat = new DataTable("CFHistogram");

                                            dtCFStatsStack.Push(dtCFStat);
                                            ProcessFileTasks.ProcessCFHistStats(dtCFHist, dtCFStat);
                                        }
										Program.ConsoleParsingNonLog.TaskEnd("TableHistogram => CFStats...");
										return dtCFHist;
                                    },
                                    TaskContinuationOptions.AttachedToParent
                                        | TaskContinuationOptions.LongRunning
                                        | TaskContinuationOptions.OnlyOnRanToCompletion);

                runCFStatsMergedDDLUpdated = Task.Factory
                                            .ContinueWhenAll(new Task[] { tskdtCFHistogram, runningLogTask, runReadRepairProcess, runAntiCompactionTask, runMemTableFlushTask }, ignoreItem => { })
                                            .ContinueWith(action =>
                                                {
                                                    Program.ConsoleParsingLog.Increment("CFStats Merge");
                                                    var dtCFTable = dtCFStatsStack.MergeIntoOneDataTable(new Tuple<string, string, DataViewRowState>(null,
                                                                                                                                                        "[Data Center], [Node IPAddress], [KeySpace], [Table]",
                                                                                                                                                        DataViewRowState.CurrentRows));
                                                    ProcessFileTasks.UpdateTableActiveStatus(dtCFTable);
                                                    Program.ConsoleParsingLog.TaskEnd("CFStats Merge");

                                                    Program.ConsoleParsingLog.Increment("DDL Active Table Update");
                                                    ProcessFileTasks.UpdateCQLDDLTableActiveStatus(dtDDLTable, dtKeySpace);
                                                    Program.ConsoleParsingLog.TaskEnd("DDL Active Table Update");

                                                    return dtCFTable;
                                                },
                                            TaskContinuationOptions.AttachedToParent                                                
                                                | TaskContinuationOptions.OnlyOnRanToCompletion);

                Task.Factory
                       .ContinueWhenAll(new Task[] { tskdtCFHistogram, updateRingWYamlInfoTask, runCFStatsMergedDDLUpdated, runNodeStatsMergedTask },
                                           tasks => Program.ConsoleParsingNonLog.Terminate());
				
				runStatsLogMerged = Task.Factory
											  .ContinueWhenAll<DataTable>(new Task[] { runLogMergedTask,
																						runReadRepairProcess,
																						runAntiCompactionTask,
																						runMemTableFlushTask,
																						runConcurrentCompactionFlushTask},
																  tasks =>
																  {
                                                                      Program.ConsoleParsingLog.Increment("Log Stats Merge");
                                                                      var dtLogStats = dtLogStatusStack.MergeIntoOneDataTable(new Tuple<string, string, DataViewRowState>(ParserSettings.LogExcelWorkbookFilter == null
																																												? ParserSettings.StatsWorkBookFilterSort.Item1
																																												: ParserSettings.LogExcelWorkbookFilter,
																																											ParserSettings.StatsWorkBookFilterSort.Item2,
																																											ParserSettings.StatsWorkBookFilterSort.Item3));
																	  Program.ConsoleParsingLog.TaskEnd("Log Stats Merge");

																	  return dtLogStats;
																  });

                

                runLogAdditionalInfoTask = runLogMergedTask;
                
                if (ParserSettings.LogParsingExcelOptions.Parse.IsEnabled()
                        || ParserSettings.LogParsingExcelOptions.ParseArchivedLogs.IsEnabled())
                {
                    runLogAdditionalInfoTask = ProcessFileTasks.ReviewDropsBlocksThresholdsOtherComponentIssues(runLogMergedTask,
                                                                                        runStatsLogMerged,
                                                                                        new Task[] { runMemTableFlushTask,
                                                                                                        runAntiCompactionTask,
                                                                                                        runReadRepairProcess,
                                                                                                        runNodeStatsMergedTask,
                                                                                                        runConcurrentCompactionFlushTask },
                                                                                        ParserSettings.QueueDroppedBlockedWarningThreshold,
                                                                                        ParserSettings.QueueDroppedBlockedWarningPeriodInMins);


                }
                
                if (ParserSettings.ParsingExcelOptions.ParseSummaryLogs.IsEnabled())
                {
                    runSummaryLogTask = ProcessFileTasks.ParseCassandraLogIntoSummaryDataTable(Task.Factory
                                                                                                    .ContinueWhenAll(new Task[] { runLogAdditionalInfoTask,
                                                                                                                                    runMemTableFlushTask,
                                                                                                                                    runAntiCompactionTask,
                                                                                                                                    runReadRepairProcess},
                                                                                                                        tasks =>
                                                                                                                        {
                                                                                                                            Program.ConsoleParsingLog.Increment("Processing Log Summary");
                                                                                                                            return ((Task<DataTable>)tasks[0]).Result;
                                                                                                                          }),
                                                                                                ParserSettings.ExcelWorkSheetLogCassandra,
                                                                                                ParserSettings.LogSummaryPeriods,
                                                                                                ParserSettings.LogSummaryPeriodRanges,
                                                                                                ParserSettings.ParsingExcelOptions.OnlyOverlappingDateRanges.IsEnabled(),
                                                                                                ProcessFileTasks.LogCassandraNodeMaxMinTimestamps,
                                                                                                ParserSettings.LogSummaryTaskItems,
                                                                                                ParserSettings.LogSummaryIgnoreTaskExceptions);

                    runSummaryLogTask.ContinueWith(action =>
                    {
                        Program.ConsoleParsingLog.TaskEnd("Processing Log Summary");                        
                    });
                }

                runReleaseDependentLogTask = Task.Factory
                                              .ContinueWhenAll(new Task[] { runLogAdditionalInfoTask,
                                                                            runSummaryLogTask,
                                                                            runMemTableFlushTask,
                                                                            runReadRepairProcess,
                                                                            runReadRepairTbl,
                                                                            runNodeStatsMergedTask,
                                                                            runCFStatsMergedDDLUpdated },
                                                                  tasks =>
                                                                  {
                                                                      ProcessFileTasks.ReleaseGlobalLogCollections();
                                                                      Program.ConsoleParsingLog.Terminate();
                                                                  });

            }

            #endregion
            #endregion

            //Care should be taken below since DataTables are released after they are loaded into Excel...
            DTLoadIntoExcel.LoadIntoExcel(runStatsLogMerged,
											runSummaryLogTask,
                                            runLogAdditionalInfoTask,
											runCFStatsMergedDDLUpdated,
											runNodeStatsMergedTask,
											tskdtCFHistogram,
											runReadRepairTbl,
											updateRingWYamlInfoTask,
											dtTokenRange,
											dtKeySpace,
											dtDDLTable,
                                            runCompHistMergeTask,
											runReleaseDependentLogTask)?.Wait();

            Program.ConsoleExcelWorkbook.Terminate();
			GCMonitor.GetInstance().StopGCMonitoring();

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

            if(nbrNodes != parsedItemCounts.Where(cnt => cnt > 0).DefaultIfEmpty().Min())
            {
                var msg = string.Format("Number of components read/parsed should have been {0}, but some components only parsed/read {1}. Review Application Log for Warning/Errors.",
                                            nbrNodes,
                                            parsedItemCounts.Where(cnt => cnt > 0).Min());
                Logger.Instance.Warn(msg);

                ConsoleDisplay.Console.WriteLine(msg);
            }

            ConsoleDisplay.End();
            Logger.Instance.InfoFormat("Completed");

            if(!argResult.Value.NoExitPrompt)
                Common.ConsoleHelper.Prompt("Press Return to Exit", ConsoleColor.Gray, ConsoleColor.DarkRed);            
        }

        private static void Console_CancelKeyPress(object sender, ConsoleCancelEventArgs e)
        {
            Logger.Instance.Warn("Application Aborted");
            Program.ConsoleErrors.Increment("Aborted");

            try
            {
                var rep = Logger.Instance.Log4NetInstance.Logger.Repository;

                rep.GetAppenders().Cast<log4net.Appender.IFlushable>()
                    .ForEach(appender =>
                        {
                            appender.Flush(2000);
                        });
            }
            catch
            {
                Logger.Instance.Error("Log4Net Appender Flush Failed!");
            }
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

		static bool MakeFile(IDirectoryPath directoryPath, string toolFolder, string fileName, bool isOpsCtrStruct, out IFilePath filePath)
		{
			if(isOpsCtrStruct)
			{
				return directoryPath.MakeChild(toolFolder).MakeFile(fileName, out filePath);
			}

			return directoryPath.MakeFile(toolFolder + "." + fileName, out filePath);
		}
    }
}
