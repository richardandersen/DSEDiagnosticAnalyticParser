using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using Common.Patterns.Tasks;
using System.Text.RegularExpressions;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
        static long ThrottleLogReaderCnt = 10;

        static public Task<int> DetermineLogFilesAndProcess(Task<int> continousLogTask,
                                                                bool continousLogTaskRestrictByLogDateRange,
                                                                IFilePath diagFilePath,
                                                                bool allowArchiveParsing,
                                                                string dcName,
                                                                string ipAddress,
                                                                Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogsStack,
                                                                Common.Patterns.Collections.ThreadSafe.Dictionary<string, string> nodeGCInfo,
                                                                List<CKeySpaceTableNames> kstblNames,
                                                                Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                                                Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
                                                                Common.Patterns.Collections.LockFree.Stack<DataTable> dtNodeStatsStack)
        {
            IFilePath[] archivedFilePaths = null;

            #region Extract Zip File
            if (diagFilePath.Exist()
                    && (ParserSettings.ExtractFilesWithExtensions.Contains(diagFilePath.FileExtension)))
            {
                IDirectoryPath extractedDir;
                var logFileTasks = new List<Task<int>>();

                Program.ConsoleLogReadFiles.Increment(string.Format("Getting Files for {0}...", diagFilePath.PathResolved));

                if (ProcessFileTasks.ExtractFileToFolder(diagFilePath, out extractedDir))
                {
                    Logger.Instance.InfoFormat("Extracted file \"{0}\" to directory \"{1}\"",
                                                    diagFilePath.PathResolved,
                                                    extractedDir.PathResolved);

                    foreach (var logFilePath in extractedDir.Children())
                    {
                        if (logFilePath.IsFilePath)
                        {
                            logFileTasks.Add(DetermineLogFilesAndProcess(continousLogTask,
                                                                            continousLogTaskRestrictByLogDateRange,
                                                                            (IFilePath)logFilePath,
                                                                            allowArchiveParsing,
                                                                            dcName,
                                                                            ipAddress,
                                                                            dtLogsStack,
                                                                            nodeGCInfo,
                                                                            kstblNames,
                                                                            dtLogStatusStack,
                                                                            dtCFStatsStack,
                                                                            dtNodeStatsStack));
                        }
                    }
                }
                Program.ConsoleLogReadFiles.TaskEnd(string.Format("Getting Files for {0}...", extractedDir.PathResolved));

                if (logFileTasks.Count == 0)
                {
                    Logger.Instance.InfoFormat("File \"{0}\" Extraction Failed. Skipping File...",
                                                    diagFilePath.PathResolved);

                    return Common.Patterns.Tasks.CompletionExtensions.CompletedTask(0);
                }
                else
                {
                    return Task.Factory.ContinueWhenAll(logFileTasks.ToArray(), items => items.Sum(i => i.Result));
                }
            }
            #endregion

            #region Archive Parsing
            if (allowArchiveParsing && ParserSettings.LogParsingExcelOptions.ParseArchivedLogs.IsEnabled())
            {
                IFilePath archivedFilePath = null;

                if (diagFilePath.ParentDirectoryPath.MakeFile(string.Format(ParserSettings.LogCassandraSystemLogFileArchive, diagFilePath.FileName, diagFilePath.FileNameWithoutExtension),
                                                                out archivedFilePath))
                {
                    Program.ConsoleLogReadFiles.Increment(string.Format("Getting Files for {0}...", archivedFilePath.PathResolved));

                    if (archivedFilePath.HasWildCardPattern())
                    {
                        archivedFilePaths = archivedFilePath.GetWildCardMatches()
                                                                .Where(p => p.IsFilePath
                                                                                && p.PathResolved != diagFilePath.PathResolved
                                                                                && !ParserSettings.ExcludePathName(p.Name))
                                                                .Cast<IFilePath>()
                                                                .ToArray();
                    }
                    else
                    {
                        archivedFilePaths = new IFilePath[] { archivedFilePath };
                    }

                    List<IFilePath> newFiles = new List<IFilePath>();

                    for (int fIdx = 0; fIdx < archivedFilePaths.Length; ++fIdx)
                    {
                        IDirectoryPath extractedDir;

                        if (ProcessFileTasks.ExtractFileToFolder(archivedFilePaths[fIdx], out extractedDir))
                        {
                            Logger.Instance.InfoFormat("Extracted file \"{0}\" to directory \"{1}\"",
                                                            archivedFilePaths[fIdx].PathResolved,
                                                            extractedDir.PathResolved);
                            if (extractedDir.MakeFile(string.Format(ParserSettings.LogCassandraSystemLogFileArchive, diagFilePath.FileName, diagFilePath.FileNameWithoutExtension),
                                                        out archivedFilePath))
                            {
                                if (archivedFilePath.HasWildCardPattern())
                                {
                                    newFiles.AddRange(archivedFilePath.GetWildCardMatches()
                                                                            .Where(p => p.IsFilePath && !ParserSettings.ExcludePathName(p.Name))
                                                                            .Cast<IFilePath>());
                                }
                                else
                                {
                                    newFiles.Add(archivedFilePath);
                                }
                                archivedFilePaths[fIdx] = null;
                            }
                        }
                    }

                    if (newFiles.Count > 0)
                    {
                        newFiles.AddRange(archivedFilePaths.Where(p => p != null));
                        archivedFilePaths = newFiles.ToArray();
                    }

                    if (ParserSettings.MaxNbrAchievedLogFiles > 0)
                    {
                        archivedFilePaths = archivedFilePaths
                                                .OrderByDescending(p => p.GetLastWriteTime())
                                                .ThenBy(p => p.FileName)
                                                .GetRange(0, ParserSettings.MaxNbrAchievedLogFiles)
                                                .ToArray();
                        Logger.Instance.InfoFormat("Node: {0} Achieved Files: {1}",
                                                    ipAddress,
                                                    string.Join(", ", archivedFilePaths.Select(i => i.FileName)));
                    }

                    Program.ConsoleLogReadFiles.TaskEnd(string.Format("Getting Files for {0}...", archivedFilePath.PathResolved));
                }
            }
            #endregion

            return ProcessFileTasks.ProcessLogFileTasks(continousLogTask,
                                                        continousLogTaskRestrictByLogDateRange,
                                                        diagFilePath,
                                                        ParserSettings.ExcelWorkSheetLogCassandra,
                                                        dcName,
                                                        ipAddress,
                                                        ParserSettings.LogStartDate,
                                                        dtLogsStack,
                                                        archivedFilePaths,
                                                        ParserSettings.LogParsingExcelOption,
                                                        ParserSettings.ParsingExcelOption,
                                                        ParserSettings.ExcelWorkSheetStatusLogCassandra,
                                                        nodeGCInfo,
                                                        ParserSettings.IgnoreKeySpaces,
                                                        kstblNames,
                                                        dtLogStatusStack,
                                                        dtCFStatsStack,
                                                        dtNodeStatsStack,
                                                        ParserSettings.GCFlagThresholdInMS,
                                                        ParserSettings.CompactionFlagThresholdInMS,
                                                        ParserSettings.CompactionFlagThresholdAsIORate,
                                                        ParserSettings.SlowLogQueryThresholdInMS);
        }

        static public Task<int> ProcessLogFileTasks(Task<int> continousLogTask,
                                                        bool continousLogTaskRestrictByLogDateRange,
                                                        IFilePath logFilePath,
                                                        string excelWorkSheetLogCassandra,
                                                        string dcName,
                                                        string ipAddress,
                                                        DateTimeRange includeLogEntriesRange,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogsStack,
                                                        IFilePath[] archiveFilePaths, //null disables archive parsing
                                                        ParserSettings.LogParsingExcelOptions parseLogOptions,
                                                        ParserSettings.ParsingExcelOptions parseNonLogOptions,
                                                        string excelWorkSheetStatusLogCassandra,
                                                        Common.Patterns.Collections.ThreadSafe.Dictionary<string, string> nodeGCInfo,
                                                        IEnumerable<string> ignoreKeySpaces,
                                                        List<CKeySpaceTableNames> kstblNames,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtTPStatsStack,
                                                        int gcPausedFlagThresholdInMS,
                                                        int compactionFllagThresholdInMS,
                                                        decimal compactionFlagThresholdAsIORate,
                                                        int slowLogQueryThresholdInMS)
        {
            var dtLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
            Task statusTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();
            Task<int> archTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask(0);

            var logTask = continousLogTask.NewOrContinueWith(ignore =>
                               {
                                   if (ParserSettings.EnableLogReadThrottle && System.Threading.Interlocked.Increment(ref ThrottleLogReaderCnt) > Properties.Settings.Default.LogReadThrottleTaskCount)
                                   {
                                       Program.ConsoleLogReadFiles.Increment("Log Throttled");
                                       System.Threading.Thread.Sleep(Properties.Settings.Default.LogReadThrottleWaitPeriodMS);
                                       Program.ConsoleLogReadFiles.TaskEnd("Log Throttled");
                                   }

                                   Program.ConsoleLogReadFiles.Increment(string.Format("{0} - {1}", ipAddress, logFilePath.FileName));

                                   dtLogsStack.Push(dtLog);

                                   if (continousLogTaskRestrictByLogDateRange)
                                   {
                                       var onlyLogRanges = LogCassandraNodeMaxMinTimestamps.Where(r => r.Key == ipAddress)
                                                                                               .SelectMany(r => r.Value)
                                                                                               .Where(r => !r.IsDebugFile)
                                                                                               .Select(r => r.LogRange);

                                       if (onlyLogRanges.HasAtLeastOneElement())
                                       {
                                           var minTimeFrame = onlyLogRanges.Min(c => c.Min);
                                           var maxTimeFrame = onlyLogRanges.Max(c => c.Max);

                                           includeLogEntriesRange = new DateTimeRange(minTimeFrame, maxTimeFrame);

                                           Logger.Instance.InfoFormat("Processing File \"{0}\" using Detected Range {1}",
                                                                       logFilePath.Path,
                                                                       includeLogEntriesRange);
                                       }
                                       else
                                       {
                                           Logger.Instance.InfoFormat("Processing File \"{0}\" (No Detected Range Found)", logFilePath.Path);
                                       }
                                   }
                                   else
                                   {
                                       Logger.Instance.InfoFormat("Processing File \"{0}\"", logFilePath.Path);
                                   }


                                   var linesRead = ReadCassandraLogParseIntoDataTable(logFilePath,
                                                                                       ipAddress,
                                                                                       dcName,
                                                                                       includeLogEntriesRange,
                                                                                       dtLog,
                                                                                       gcPausedFlagThresholdInMS,
                                                                                       compactionFllagThresholdInMS,
                                                                                       compactionFlagThresholdAsIORate,
                                                                                       slowLogQueryThresholdInMS);

                                   Program.ConsoleLogReadFiles.TaskEnd(string.Format("{0} - {1}", ipAddress, logFilePath.FileName));

                                   return linesRead;
                               });

            if (ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled()
                || ParserSettings.ParsingExcelOptions.ParseNodeStatsLogs.IsEnabled()
                || ParserSettings.ParsingExcelOptions.ParseCFStatsLogs.IsEnabled())
            {
                statusTask = logTask.ContinueWith(taskResult =>
                            {
                                var dtStatusLog = new System.Data.DataTable(excelWorkSheetStatusLogCassandra + "-" + ipAddress);
                                var dtCFStats = parseNonLogOptions.CheckEnabled(ParserSettings.ParsingExcelOptions.ParseCFStatsLogs) ? new DataTable("CFStats-Logs" + "-" + ipAddress) : null;
                                var dtTPStats = parseNonLogOptions.CheckEnabled(ParserSettings.ParsingExcelOptions.ParseNodeStatsLogs) ? new DataTable("TPStats-Logs" + "-" + ipAddress) : null;

                                if (ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled())
                                {
                                    dtLogStatusStack.Push(dtStatusLog);
                                }

                                if (dtCFStats != null) dtCFStatsStack.Push(dtCFStats);
                                if (dtTPStats != null) dtTPStatsStack.Push(dtTPStats);

                                Logger.Instance.InfoFormat("Status Log Processing File \"{0}\"", logFilePath.Path);
                                Program.ConsoleParsingLog.Increment(string.Format("Status {0}", dtLog.TableName));

                                ParseCassandraLogIntoStatusLogDataTable(dtLog,
                                                                        dtStatusLog,
                                                                        dtCFStats,
                                                                        dtTPStats,
                                                                        nodeGCInfo,
                                                                        ipAddress,
                                                                        dcName,
                                                                        ignoreKeySpaces,
                                                                        kstblNames);

                                Program.ConsoleParsingLog.TaskEnd(string.Format("Status {0}", dtLog.TableName));
                            },
                            TaskContinuationOptions.AttachedToParent
                                | TaskContinuationOptions.OnlyOnRanToCompletion);
            }

            if (archiveFilePaths != null
                        && ParserSettings.LogParsingExcelOptions.ParseArchivedLogs.IsEnabled())
            {
                foreach (IFilePath archiveElement in archiveFilePaths)
                {
                    if (archiveElement.PathResolved != logFilePath.PathResolved)
                    {
                        archTask = ProcessLogFileTasks(logTask,
                                                            continousLogTaskRestrictByLogDateRange,
                                                            archiveElement,
                                                            excelWorkSheetLogCassandra,
                                                            dcName,
                                                            ipAddress,
                                                            includeLogEntriesRange,
                                                            dtLogsStack,
                                                            null,
                                                            parseLogOptions,
                                                            parseNonLogOptions,
                                                            excelWorkSheetStatusLogCassandra,
                                                            nodeGCInfo,
                                                            ignoreKeySpaces,
                                                            kstblNames,
                                                            dtLogStatusStack,
                                                            dtCFStatsStack,
                                                            dtTPStatsStack,
                                                            gcPausedFlagThresholdInMS,
                                                            compactionFllagThresholdInMS,
                                                            compactionFlagThresholdAsIORate,
                                                            slowLogQueryThresholdInMS);
                    }
                }
            }


            return Task<int>
                    .Factory
                    .ContinueWhenAll(new Task[] { logTask, statusTask, archTask }, tasks =>
                    {
                        if (ParserSettings.EnableLogReadThrottle)
                        {
                            System.Threading.Interlocked.Decrement(ref ThrottleLogReaderCnt);
                        }
                        return logTask.Result + archTask.Result;
                    });
        }

        public static Task<Tuple<DataTable, DataTable, DateTimeRange>> ParseCassandraLogIntoSummaryDataTable(Task<DataTable> logTask,
                                                                                                                string excelWorkSheetLogCassandra,
                                                                                                                Tuple<DateTime, TimeSpan>[] logSummaryPeriods,
                                                                                                                Tuple<TimeSpan, TimeSpan>[] logSummaryPeriodRanges,
                                                                                                                bool summarizeOnlyOverlappingDateRangesForNodes,
                                                                                                                IDictionary<string, List<LogCassandraNodeMaxMinTimestamp>> nodeLogDateRanges,
                                                                                                                string[] logAggregateAdditionalTaskExceptionItems,
                                                                                                                string[] logSummaryIgnoreTaskExceptions)
        {
            Task<Tuple<DataTable, DataTable, DateTimeRange>> summaryTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask<Tuple<DataTable, DataTable, DateTimeRange>>();

            if ((logSummaryPeriods != null && logSummaryPeriods.Length > 0)
                            || (logSummaryPeriodRanges != null && logSummaryPeriodRanges.Length > 0))
            {
                summaryTask = logTask.ContinueWith(taskResult =>
                                {
                                    var maxminLogDate = new DateTimeRange(ProcessFileTasks.LogCassandraMaxMinTimestamp);

                                    if (summarizeOnlyOverlappingDateRangesForNodes)
                                    {
                                        foreach (var nodeLogRanges in nodeLogDateRanges)
                                        {
                                            DateTimeRange nodeInnerRange = new DateTimeRange();

                                            nodeLogRanges.Value.Where(r => !r.IsDebugFile)
                                                                .Select(r => r.LogRange)
                                                                .ForEach(range =>
                                            {
                                                nodeInnerRange.SetMinMax(range.Min);
                                                nodeInnerRange.SetMinMax(range.Max);
                                            });

                                            maxminLogDate.SetMinimal(maxminLogDate.MaximumMinDateTime(nodeInnerRange));
                                            maxminLogDate.SetMaximum(maxminLogDate.MinimalMaxDateTime(nodeInnerRange));
                                        }
                                    }

                                    DataTable dtLog = taskResult.Result;
                                    DataTable dtSummaryLog = new DataTable(dtLog.TableName + "Summary");
                                    DataTable dtExceptionSummaryLog = new DataTable(dtLog.TableName + "Exception Summary");

                                    Program.ConsoleParsingLog.Increment(string.Format("Summary {0}", dtLog.TableName));

                                    Tuple<DateTime, TimeSpan>[] summaryPeriods = logSummaryPeriods;

                                    if (logSummaryPeriods == null || logSummaryPeriods.Length == 0)
                                    {
                                        var summaryPeriodList = new List<Tuple<DateTime, TimeSpan>>();

                                        var currentRange = maxminLogDate.Max.RoundUp(logSummaryPeriodRanges.First().Item2);

                                        for (int nIndex = 0; nIndex < logSummaryPeriodRanges.Length; ++nIndex)
                                        {
                                            summaryPeriodList.Add(new Tuple<DateTime, TimeSpan>(currentRange,
                                                                                                    logSummaryPeriodRanges[nIndex].Item2));

                                            if (currentRange <= maxminLogDate.Min)
                                            {
                                                break;
                                            }

                                            currentRange = currentRange - logSummaryPeriodRanges[nIndex].Item1;
                                        }

                                        summaryPeriods = summaryPeriodList.ToArray();
                                    }
                                    else
                                    {
                                        maxminLogDate.SetMinimal(DateTime.MinValue);
                                        maxminLogDate.SetMaximum(logSummaryPeriods[0].Item1);
                                    }

                                    ParseCassandraLogIntoSummaryDataTable(dtLog,
                                                                            dtSummaryLog,
                                                                            dtExceptionSummaryLog,
                                                                            maxminLogDate,
                                                                            logAggregateAdditionalTaskExceptionItems,
                                                                            logSummaryIgnoreTaskExceptions,
                                                                            summaryPeriods);

                                    Program.ConsoleParsingLog.TaskEnd(string.Format("Summary {0}", dtLog.TableName));

                                    return new Tuple<DataTable, DataTable, DateTimeRange>(dtSummaryLog, dtExceptionSummaryLog, maxminLogDate);
                                },
                                TaskContinuationOptions.AttachedToParent
                                    | TaskContinuationOptions.OnlyOnRanToCompletion);
            }

            return summaryTask;
        }

        static public DateTimeRange LogCassandraMaxMinTimestamp = new Common.DateTimeRange();

        public class LogCassandraNodeMaxMinTimestamp
        {
            private LogCassandraNodeMaxMinTimestamp() { }
            public LogCassandraNodeMaxMinTimestamp(DateTimeRange range, bool isDebugFile)
            {
                this.IsDebugFile = isDebugFile;
                this.LogRange = range;
            }

            public LogCassandraNodeMaxMinTimestamp(DateTime minDate, DateTime maxDate, bool isDebugFile)
            {
                this.IsDebugFile = isDebugFile;
                this.LogRange = new DateTimeRange(minDate, maxDate);
            }

            public DateTimeRange LogRange { get; }
            public bool IsDebugFile { get; }

            public override string ToString()
            {
                return string.Format("{0}<{1}>",
                                        this.IsDebugFile ? "DebugLog" : "SystemLog",
                                        this.LogRange);
            }
        }

        static public Common.Patterns.Collections.ThreadSafe.Dictionary<string, List<LogCassandraNodeMaxMinTimestamp>> LogCassandraNodeMaxMinTimestamps = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, List<LogCassandraNodeMaxMinTimestamp>>();

        static readonly Regex RegExCLogCL = new Regex(@".*Cannot achieve consistency level\s+(\w+)\s*",
                                                        RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static readonly Regex RegExCLogTO = new Regex(@".*(?:No response after timeout:|Operation timed out - received only).*\s+(\d+)\s*",
                                                        RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static readonly Regex RegExExpErrClassName = new Regex(@"^[^\[\(\']\S+(exception|error)",
                                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //INFO[SharedPool - Worker - 1] 2016 - 09 - 24 16:33:58,099  Message.java:532 - Unexpected exception during request; channel = [id: 0xa6a28fb0, / 10.14.50.24:44796 => / 10.14.50.24:9042]
        //io.netty.handler.ssl.NotSslRecordException: not an SSL / TLS record: 0300000001000000160001000b43514c5f56455253494f4e0005332e302e30

        static readonly Regex RegExExceptionDesc = new Regex(@"(.+?)(?:(\/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\:?\d{1,5})|(?:\:)|(?:\;)|(?:\$)|(?:\#)|(?:\[\G\])|(?:\(\G\))|(?:0x\w+)|(?:\w+\-\w+\-\w+\-\w+\-\w+)|(\'.+\')|(?:\s+\-?\d+\s+)|(?:[0-9a-zA-z]{12,}))",
                                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //returns only numeric values in string via Matches
        static readonly Regex RegExNumerics = new Regex(@"([0-9-.,]+)",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static readonly Regex RegExSolrSecondaryIndex = new Regex(@"SolrSecondaryIndex\s+([^\s]+\.[^\s]+)\s+(.+)",
                                                                    RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static readonly Regex RegExSolrFlushIndexNames = new Regex("^Cql3SolrSecondaryIndex\\{columnDefs=\\[(?:\\s*ColumnDefinition\\{(?:\\s*(?:indexname\\=(?<indexname>[a-z0-9\\\\-_$%+!?<>^*&@]+)|[^,}]+)\\,?\\s*)+\\s*\\}\\,?)+",
                                                                    RegexOptions.IgnoreCase | RegexOptions.Compiled);
        public static readonly Regex RegExIsDebugFile = new Regex(Properties.Settings.Default.DebugLogFileRegExMatch,
                                                                        RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public enum LogFlagStatus
        {
            None = 0,
            Exception = 1, //Includes Errors, Warns, etc. (Summary only)
            Stats = 2, //Stats and Summary
            ReadRepair = 3,
            StatsOnly = 4, //Only Stats (no summary)
            MemTblFlush = 5,
            Ignore = 999
        }


        static public readonly Common.Patterns.Collections.ThreadSafe.ReaderWriter.HashSet<string> LogLinesHash = new Common.Patterns.Collections.ThreadSafe.ReaderWriter.HashSet<string>();
        static public long NbrDuplicatedLogEventsTotal = 0;
        static public long NbrDuplicatedDebugLogEventsTotal = 0;

        public static bool AddRowToLogDataTable(DataTable dtCLog, DataRow dataRow, bool isDebugLog, bool forceAdd = false)
        {
            bool rowAlreadyAdded = !forceAdd;

            if (!forceAdd)
            {
                var strLogLine = new StringBuilder();

                strLogLine.Append(dataRow.ItemArray[1] == null ? string.Empty : dataRow.ItemArray[1]);
                strLogLine.Append('|');
                strLogLine.Append(dataRow.ItemArray[2] == null ? string.Empty : dataRow.ItemArray[2]);
                strLogLine.Append('|');
                strLogLine.Append(dataRow.ItemArray[3] == null ? string.Empty : ((DateTime)dataRow.ItemArray[3]).ToString("yyyy-MM-dd HH:mm:ss.fff"));
                strLogLine.Append('|');

                for (int nIdx = 5; nIdx <= 9; ++nIdx)
                {
                    strLogLine.Append(dataRow.ItemArray[nIdx] == null ? string.Empty : dataRow.ItemArray[nIdx]);
                    strLogLine.Append('|');
                }

                //strLogLine.Append(dataRow.ItemArray[10] == null ? string.Empty : dataRow.ItemArray[10]);
                //strLogLine.Append('|');
                strLogLine.Append(dataRow.ItemArray[12] == null ? string.Empty : dataRow.ItemArray[12]);

                rowAlreadyAdded = LogLinesHash.Contains(strLogLine.ToString());

                if (!rowAlreadyAdded)
                {
                    LogLinesHash.Add(strLogLine.ToString());
                }

            }

            if (!rowAlreadyAdded)
            {
                dtCLog.Rows.Add(dataRow);
                return true;
            }

            if (isDebugLog)
            {
                System.Threading.Interlocked.Increment(ref NbrDuplicatedDebugLogEventsTotal);
            }
            else
            {
                System.Threading.Interlocked.Increment(ref NbrDuplicatedLogEventsTotal);
            }

            return false;
        }

        static void CreateCassandraLogDataTable(System.Data.DataTable dtCLog, bool includeGroupIndiator = false)
        {
            if (dtCLog.Columns.Count == 0)
            {
                if (includeGroupIndiator)
                {
                    dtCLog.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;
                }
                dtCLog.Columns.Add("Source", typeof(string));
                dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCLog.Columns.Add("Node IPAddress", typeof(string));
                dtCLog.Columns.Add("Timestamp", typeof(DateTime)).AllowDBNull = false;
                dtCLog.Columns.Add("Indicator", typeof(string));
                dtCLog.Columns.Add("Task", typeof(string));
                dtCLog.Columns.Add("TaskId", typeof(int)).AllowDBNull = true;
                dtCLog.Columns.Add("Item", typeof(string));
                dtCLog.Columns.Add("Exception", typeof(string)).AllowDBNull = true;
                dtCLog.Columns.Add("Exception Description", typeof(string)).AllowDBNull = true;
                dtCLog.Columns.Add("Associated Item", typeof(string)).AllowDBNull = true;
                dtCLog.Columns.Add("Associated Value", typeof(object)).AllowDBNull = true;
                dtCLog.Columns.Add("Description", typeof(string));
                dtCLog.Columns.Add("Flagged", typeof(int)).AllowDBNull = true;
            }
        }

        static int ReadCassandraLogParseIntoDataTable(IFilePath clogFilePath,
                                                        string ipAddress,
                                                        string dcName,
                                                        DateTimeRange onlyEntriesAllowedRange,
                                                        System.Data.DataTable dtCLog,
                                                        int gcPausedFlagThresholdInMS,
                                                        int compactionFlagThresholdInMS,
                                                        decimal compactionFlagThresholdAsIORate,
                                                        int slowLogQueryThresholdInMS)
        {
            if (ParserSettings.IgnoreLogFileExtensions.Contains(clogFilePath.FileExtension))
            {
                return 0;
            }

            CreateCassandraLogDataTable(dtCLog);

            string fileName = clogFilePath.FileName;
            string line;
            string readLine = null;
            string readNextLine = null;
            bool skipNextRead = false;
            List<string> parsedValues;
            DataRow dataRow;
            DataRow lastRow = null;
            DateTime lineDateTime;
            var minmaxDate = new Common.DateTimeRange();
            string lineIPAddress;
            string tableItem = null;
            int tableItemPos = -1;
            int nbrRows = 0;
            DateTimeRange ignoredTimeRange = new DateTimeRange();
            bool exceptionOccurred = false;
            bool assertError = false;
            bool skippingLineEarlyDateRange = false;
            int consumeNextLine = 0;
            string currentSolrSchemaName = null;
            bool allowRange = !onlyEntriesAllowedRange.IsEmpty();
            bool isDebugLogFile = RegExIsDebugFile.IsMatch(clogFilePath.FileName);

            //int tableItemValuePos = -1;

            using (var readStream = clogFilePath.StreamReader())
            {
                readNextLine = readStream.ReadLine();

                #region Check to see if log file is within time frame
                if (allowRange && readNextLine != null)
                {
                    var testLine = Common.StringFunctions.Split(readNextLine,
                                                                ' ',
                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                Common.StringFunctions.SplitBehaviorOptions.Default
                                                                    | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries
                                                                    | StringFunctions.SplitBehaviorOptions.IgnoreMismatchedWithinDelimiters);
                    if (testLine.Count >= 6)
                    {
                        if (DateTime.TryParse(testLine[ParserSettings.CLogLineFormats.TimeStampPos] + ' ' + testLine[ParserSettings.CLogLineFormats.TimeStampPos + 1].Replace(',', '.'), out lineDateTime))
                        {
                            if (lineDateTime < onlyEntriesAllowedRange.Min)
                            {
                                DateTime lastDateTime = DateTime.MaxValue;

                                if (readStream.BaseStream.Length > 1024)
                                {
                                    readStream.DiscardBufferedData();
                                    readStream.BaseStream.Seek(-1024, System.IO.SeekOrigin.End);
                                }

                                while ((readLine = readStream.ReadLine()) != null)
                                {
                                    testLine = Common.StringFunctions.Split(readLine,
                                                            ' ',
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default
                                                                | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries
                                                                | StringFunctions.SplitBehaviorOptions.IgnoreMismatchedWithinDelimiters);
                                    if (testLine.Count >= 6)
                                    {
                                        if (DateTime.TryParse(testLine[ParserSettings.CLogLineFormats.TimeStampPos] + ' ' + testLine[ParserSettings.CLogLineFormats.TimeStampPos + 1].Replace(',', '.'), out lineDateTime))
                                        {
                                            lastDateTime = lineDateTime;
                                        }
                                    }
                                }

                                if (lastDateTime < onlyEntriesAllowedRange.Min)
                                {
                                    Logger.Instance.InfoFormat("Log File skipped because it was did not meet Log time range ({1}). Ending timestamp is {2} for Log \"{0}\"",
                                                                    clogFilePath.PathResolved,
                                                                    onlyEntriesAllowedRange,
                                                                    lastDateTime);
                                    return 0;
                                }

                                readStream.DiscardBufferedData();
                                readStream.BaseStream.Seek(readNextLine.Length + 1, System.IO.SeekOrigin.Begin);
                            }
                            else if (lineDateTime > onlyEntriesAllowedRange.Max)
                            {
                                Logger.Instance.InfoFormat("Log File skipped because it was did not meet Log time range ({1}). Starting timestamp is {2} for Log \"{0}\"",
                                                                    clogFilePath.PathResolved,
                                                                    onlyEntriesAllowedRange,
                                                                    lineDateTime);
                                return 0;
                            }
                        }
                    }
                }
                #endregion

                while (readNextLine != null)
                {
                    readLine = readNextLine;
                    readNextLine = readStream.ReadLine();

                    if (skipNextRead)
                    {
                        skipNextRead = false;
                        continue;
                    }

                    line = readLine.Trim();

                    if (string.IsNullOrEmpty(line)
                            || line.Length < 3
                            || line.StartsWith("---")
                            || line[0] == '|')
                    {
                        continue;
                    }

                    if ((line[0] == '/' && line.Contains(":[")) // /10.14.148.34:[
                            || (!assertError && line.Length >= 3 && line.Substring(0, 3).ToLower() == "at ")
                            || (line.Length >= 4 && line.Substring(0, 4) == "... "))
                    {
                        continue;
                    }

                    Program.ConsoleLogCount.Increment();

                    if (consumeNextLine > 0)
                    {
                        --consumeNextLine;
                        if (lastRow != null)
                        {
                            lastRow["Description"] = ((string)lastRow["Description"]) + @"\" + line.TrimEnd();
                        }
                        continue;
                    }

                    parsedValues = Common.StringFunctions.Split(line,
                                                                ' ',
                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                Common.StringFunctions.SplitBehaviorOptions.Default
                                                                    | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries
                                                                    | StringFunctions.SplitBehaviorOptions.IgnoreMismatchedWithinDelimiters);

                    //INFO  [CompactionExecutor:9928] 2016-07-25 04:23:34,819  CompactionTask.java:274 - Compacted 4 sstables to [/data/system/peer_events-59dfeaea8db2334191ef109974d81484/system-peer_events-ka-77,].  35,935 bytes to 35,942 (~100% of original) in 40ms = 0.856924MB/s.  20 total partitions merged to 5.  Partition merge counts were {4:5, }
                    //		INFO [SharedPool-Worker-2] 2016-07-25 04:25:35,919  Message.java:532 - Unexpected exception during request; channel = [id: 0x40c292ba, / 10.160.139.242:42705 :> / <1ocal node>:9042]
                    //		java.io.IOException: Error while read(...): Connection reset by peer
                    //    		at io.netty.channel.epoll.Native.readAddress(Native Method) ~[netty - all - 4.0.23.Final.jar:4.0.23.Final]
                    //    		at io.netty.channel.epoll.EpollSocketChannel$EpollSocketUnsafe.doReadBytes(EpollSocketChannel.java:675) ~[netty - all - 4.0.23.Final.jar:4.0.23.Final]
                    //    		at io.netty.channel.epoll.EpollSocketChannel$EpollSocketUnsafe.epollInReady(EpollSocketChannel.java:714) ~[netty - all - 4.0.23.Final.jar:4.0.23.Final]
                    //		WARN  [HintedHandoffManager:1] 2016-07-25 04:26:10,445  HintedHandoffMetrics.java:79 - /10.170.110.191 has 1711 dropped hints, because node is down past configured hint window.
                    //		ERROR[RMI TCP Connection(7348) - 127.0.0.1] 2016-07-29 23:24:54,576 SolrCore.java(line 2340) IO error while trying to get the size of the Directory
                    //		java.io.FileNotFoundException: _i2v5.nvm
                    //		at org.apache.lucene.store.FSDirectory.fileLength(FSDirectory.java:267)
                    //	WARN [ReadStage:1325219] 2016-07-14 17:41:21,164 SliceQueryFilter.java (line 231) Read 11 live and 1411 tombstoned cells in cma.mls_records_property (see tombstone_warn_threshold). 5000 columns was requested, slices=[-]

                    //INFO [CompactionExecutor:7414] 2016-07-26 23:11:50,335 CompactionController.java (line 191) Compacting large row billing/account_payables:20160726:FMCC (348583137 bytes) incrementally
                    //INFO [ScheduledTasks:1] 2016-07-30 06:32:53,397 GCInspector.java (line 116) GC for ParNew: 394 ms for 1 collections, 13571498424 used; max is 25340346368
                    //WARN [Native-Transport-Requests:30] 2016-08-01 22:58:11,080 BatchStatement.java (line 226) Batch of prepared statements for [clearcore.documents_case] is of size 71809, exceeding specified threshold of 65536 by 6273.
                    //WARN [ReadStage:1907643] 2016-08-01 23:26:42,845 SliceQueryFilter.java (line 231) Read 14 live and 1344 tombstoned cells in cma.mls_records_property (see tombstone_warn_threshold). 5000 columns was requested, slices=[-]
                    //INFO  [Service Thread] 2016-08-10 06:51:10,572  GCInspector.java:258 - G1 Young Generation GC in 264ms.  G1 Eden Space: 3470786560 -> 0; G1 Old Gen: 2689326672 -> 2934172000; G1 Survivor Space: 559939584 -> 35651584;

                    //INFO  [Thread-4] 2016-08-25 20:00:46,363  StorageService.java:2956 - Starting repair command #1, repairing 256 ranges for keyspace system_traces (parallelism=SEQUENTIAL, full=true)
                    //INFO[RMI TCP Connection(66862) - 127.0.0.1] 2016 - 08 - 10 07:08:06,169  StorageService.java:2891 - starting user - requested repair of range[(-2100511606573441819, -2090067312984508524]] for keyspace gamingactivity and column families[membergamingeventaggregate, membergamingevent, membergameswagered, gamingexpectation, memberfundingeventaggregate, schema_current_version, memberfundingevent, schema_version, memberactiveduration, membergamingeventsubaggregate, memberwagergameaggregate]
                    //INFO[Thread - 1616292] 2016 - 08 - 10 07:08:06, 169  StorageService.java:2970 - Starting repair command #9663, repairing 1 ranges for keyspace gamingactivity (parallelism=PARALLEL, full=true)
                    //INFO[AntiEntropySessions: 9665] 2016 - 08 - 10 07:08:06, 218  RepairSession.java:260 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] new session: will sync /10.211.34.150, /10.211.34.167, /10.211.34.165, /10.211.34.164, /10.211.34.158 on range (-2100511606573441819,-2090067312984508524] for gamingactivity.[memberfundingeventaggregate, memberactiveduration, membergamingeventsubaggregate, gamingexpectation, membergamingevent, membergameswagered, schema_version, memberfundingevent, memberwagergameaggregate, membergamingeventaggregate, schema_current_version]
                    //INFO[AntiEntropySessions: 9665] 2016 - 08 - 10 07:08:06, 218  RepairJob.java:163 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] requesting merkle trees for memberfundingeventaggregate (to [/10.211.34.167, /10.211.34.165, /10.211.34.164, /10.211.34.158, /10.211.34.150])
                    //INFO[AntiEntropyStage: 1] 2016 - 08 - 10 07:08:06, 219  RepairSession.java:171 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] Received merkle tree for memberfundingeventaggregate from /10.211.34.150
                    //INFO[AntiEntropyStage: 1] 2016 - 08 - 10 07:08:06, 219  RepairSession.java:171 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] Received merkle tree for memberfundingeventaggregate from /10.211.34.164
                    //INFO[AntiEntropyStage: 1] 2016 - 08 - 10 07:08:06, 219  RepairSession.java:171 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] Received merkle tree for memberfundingeventaggregate from /10.211.34.158
                    //INFO[AntiEntropyStage: 1] 2016 - 08 - 10 07:08:06, 219  RepairSession.java:171 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] Received merkle tree for memberfundingeventaggregate from /10.211.34.165
                    //INFO[AntiEntropyStage: 1] 2016 - 08 - 10 07:08:06, 219  RepairSession.java:171 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] Received merkle tree for memberfundingeventaggregate from /10.211.34.167
                    //INFO[RepairJobTask: 1] 2016 - 08 - 10 07:08:06, 219  Differencer.java:67 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] Endpoints /10.211.34.150 and /10.211.34.164 are consistent for memberfundingeventaggregate
                    //ERROR [AntiEntropySessions:1857] 2016-06-10 21:56:53,281  RepairSession.java:276 - [repair #dc161200-2f4d-11e6-bd0c-93368bf2a346] Cannot proceed on repair because a neighbor (/10.27.34.54) is dead: session failed
                    //INFO  [CompactionExecutor:4657] 2016-06-12 06:26:25,534  CompactionTask.java:274 - Compacted 4 sstables to [/data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/system-size_estimates-ka-11348,]. 2,270,620 bytes to 566,478 (~24% of original) in 342ms = 1.579636MB/s. 40 total partitions merged to 10. Partition merge counts were {4:10, }
                    //WARN  [CompactionExecutor:6] 2016-06-07 06:57:44,146  SSTableWriter.java:240 - Compacting large partition kinesis_events/event_messages:49c023da-0bb8-46ce-9845-111514b43a63 (186949948 bytes)
                    //WARN  [CompactionExecutor:705441] 2016-10-05 13:09:40,934 SSTableWriter.java:241 - Compacting large partition handled_exception1/summary_pt1h:5670561a6c33dc0f00f11443:2016-10-01-14-00-00:total (280256103 bytes)
                    //INFO  [CqlSlowLog-Writer-thread-0] 2016-08-16 17:11:16,429  CqlSlowLogWriter.java:151 - Recording statements with duration of 60001 in slow log
                    //ERROR [SharedPool-Worker-15] 2016-08-16 17:11:16,831  SolrException.java:150 - org.apache.solr.common.SolrException: No response after timeout: 60000
                    //WARN  [CqlSlowLog-Writer-thread-0] 2016-08-17 00:21:05,698  CqlSlowLogWriter.java:245 - Error writing to cql slow log
                    //org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level ONE
                    //java.lang.AssertionError: id=3114 length=3040 docID=2090 maxDoc=3040
                    //  at org.apache.lucene.index.RTSortedDocValues.getOrd(RTSortedDocValues.java:162) ~[solr - uber - with - auth_2.0 - 4.10.3.0.101.jar:na]
                    //Caused by: org.apache.solr.search.SyntaxError: Cannot parse '(((other_id:() AND other_id_type:(PASSPORT)))^1.0 OR phone:(k OR l)^1.0 OR ((street:(CHURCH) AND street:(6835)))^1.0)': Encountered " ")" ") "" at line 1, column 13.
                    //ERROR [SharedPool-Worker-1] 2016-09-28 19:18:25,277  CqlSolrQueryExecutor.java:375 - No response after timeout: 60000
                    //org.apache.solr.common.SolrException: No response after timeout: 60000
                    //java.lang.RuntimeException: org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level LOCAL_ONE
                    //ERROR [SharedPool-Worker-3] 2016-10-01 19:20:14,415  Message.java:538 - Unexpected exception during request; channel = [id: 0xc224c650, /10.16.9.33:49634 => /10.12.50.27:9042]
                    //ERROR [MessagingService-Incoming-/10.12.49.27] 2016-09-28 18:53:54,898  JVMStabilityInspector.java:106 - JVM state determined to be unstable.  Exiting forcefully due to:
                    //java.lang.OutOfMemoryError: Java heap space
                    //WARN  [commitScheduler-4-thread-1] 2016-09-28 18:53:32,436  WorkPool.java:413 - Timeout while waiting for workers when flushing pool Index; current timeout is 300000 millis, consider increasing it, or reducing load on the node.
                    //Failure to flush may cause excessive growth of Cassandra commit log.
                    //SharedPool-Worker-1	DseAuthenticator.java					 Plain text authentication without client / server encryption is strongly discouraged

                    #region Exception Log Info Parsing

                    if (nbrRows > 0 || assertError)
                    {
                        if (assertError && line.Substring(0, 3).ToLower() == "at ")
                        {
                            #region assert Error at
                            //ERROR [LocalShardServer query worker - 3] 2016-09-26 15:33:30,690  ShardServer.java:156 - id=3080 length=2855 docID=2056 maxDoc=2855
                            //java.lang.AssertionError: id = 3080 length = 2855 docID = 2056 maxDoc = 2855
                            //at org.apache.lucene.index.RTSortedDocValues.getOrd(RTSortedDocValues.java:162) ~[solr - uber - with - auth_2.0 - 4.10.3.0.101.jar:na]

                            assertError = false;

                            if (lastRow != null)
                            {
                                var endFuncPos = parsedValues[1].IndexOf('(');

                                if (lastRow["Exception"] == DBNull.Value)
                                {
                                    lastRow["Exception"] = "java.lang.AssertionError(" + (endFuncPos >= 0 ? parsedValues[1].Substring(0, endFuncPos) : parsedValues[0]).Trim() + ")";
                                }
                                else
                                {
                                    lastRow["Exception"] = ((string)lastRow["Exception"]) + "(" + (endFuncPos >= 0 ? parsedValues[1].Substring(0, endFuncPos) : parsedValues[0]).Trim() + ")";
                                }

                                if (lastRow["Flagged"] == DBNull.Value || (int)lastRow["Flagged"] == 0)
                                {
                                    lastRow["Flagged"] = (int)LogFlagStatus.Exception;
                                }
                            }

                            continue;
                            #endregion
                        }
                        else if (parsedValues[0].ToLower().Contains("assertionerror"))
                        {
                            #region assertion error
                            //ERROR [LocalShardServer query worker - 3] 2016-09-26 15:33:30,690  ShardServer.java:156 - id=3080 length=2855 docID=2056 maxDoc=2855
                            //java.lang.AssertionError: id = 3080 length = 2855 docID = 2056 maxDoc = 2855
                            //at org.apache.lucene.index.RTSortedDocValues.getOrd(RTSortedDocValues.java:162) ~[solr - uber - with - auth_2.0 - 4.10.3.0.101.jar:na]
                            if (lastRow == null)
                            {
                                line.Dump(Logger.DumpType.Warning, "assertionerror found but no associated previous log line at in log file \"{0}\"", clogFilePath.PathResolved);
                            }
                            else
                            {
                                var exception = parsedValues[0][parsedValues[0].Length - 1] == ':'
                                                        ? parsedValues[0].Substring(0, parsedValues[0].Length - 1)
                                                        : parsedValues[0];

                                lastRow["Exception Description"] = line;
                                lastRow["Flagged"] = (int)LogFlagStatus.Exception;
                                lastRow["Exception"] = exception;

                                assertError = true;
                                exceptionOccurred = true;
                            }
                            continue;
                            #endregion
                        }
                        else if (parsedValues[0].ToLower().Contains("exception")
                                    || parsedValues[0].ToLower().EndsWith("error:"))
                        {
                            #region Exception

                            if (lastRow == null)
                            {
                                line.Dump(Logger.DumpType.Warning, "exception found but no associated previous log line for log file \"{0}\"", clogFilePath.PathResolved);
                            }
                            else
                            {
                                ParseExceptions(ipAddress, parsedValues[0], lastRow, string.Join(" ", parsedValues.Skip(1)), null);
                                if (lastRow.RowState != DataRowState.Detached) lastRow.AcceptChanges();
                            }

                            exceptionOccurred = true;
                            continue;
                            #endregion
                        }
                        else if (parsedValues[0].ToLower() == "caused")
                        {
                            #region caused

                            if (lastRow == null)
                            {
                                line.Dump(Logger.DumpType.Warning, "caused line found but no associated previous line");
                            }
                            else
                            {
                                ParseExceptions(ipAddress, parsedValues[2], lastRow, string.Join(" ", parsedValues.Skip(2)), null);
                                if (lastRow.RowState != DataRowState.Detached) lastRow.AcceptChanges();
                            }
                            exceptionOccurred = true;
                            continue;

                            #endregion
                        }

                        assertError = false;
                    }

                    #endregion

                    if (parsedValues.Count < 6)
                    {
                        if (lastRow != null && !exceptionOccurred && nbrRows > 0)
                        {
                            line.Dump(Logger.DumpType.Warning, "Invalid Log Line File: {0}", clogFilePath.PathResolved);
                            Program.ConsoleWarnings.Increment("Invalid Log Line:", line);
                        }
                        continue;
                    }

                    #region Timestamp/Number of lines Parsing
                    if (DateTime.TryParse(parsedValues[ParserSettings.CLogLineFormats.TimeStampPos] + ' ' + parsedValues[ParserSettings.CLogLineFormats.TimeStampPos + 1].Replace(',', '.'), out lineDateTime))
                    {
                        if (allowRange)
                        {
                            if (!onlyEntriesAllowedRange.IsBetween(lineDateTime))
                            {
                                Program.ConsoleLogCount.Decrement();

                                if (!skippingLineEarlyDateRange)
                                {
                                    skippingLineEarlyDateRange = true;
                                    Logger.Instance.InfoFormat("Log Lines skipped because it was did not meet Log time range ({1}) at timestamp {2} in Log \"{0}\"",
                                                                        clogFilePath.PathResolved,
                                                                        onlyEntriesAllowedRange,
                                                                        lineDateTime);
                                }
                                continue;
                            }
                            else if (skippingLineEarlyDateRange)
                            {
                                skippingLineEarlyDateRange = false;
                                Logger.Instance.InfoFormat("Log Line begin processing since it meets Log time range ({1}) at timestamp {2} in Log \"{0}\"",
                                                                    clogFilePath.PathResolved,
                                                                    onlyEntriesAllowedRange,
                                                                    lineDateTime);
                            }
                        }
                    }
                    else
                    {
                        if (!exceptionOccurred && nbrRows > 0)
                        {
                            line.Dump(Logger.DumpType.Warning, "Invalid Log Date/Time File: {0}", clogFilePath.PathResolved);
                            Program.ConsoleWarnings.Increment("Invalid Log Date/Time:", line);
                        }
                        continue;
                    }


                    List<LogCassandraNodeMaxMinTimestamp> nodeRangers;
                    if (LogCassandraNodeMaxMinTimestamps.TryGetValue(ipAddress, out nodeRangers))
                    {
                        lock (nodeRangers)
                        {
                            if (nodeRangers.Any(r => r.IsDebugFile == isDebugLogFile && r.LogRange.IsBetween(lineDateTime)))
                            {
                                //nodeRangers.Dump(string.Format("Warning: Log Date \"{1}\" falls between already processed timestamp ranges. Processing of this log file is aborted. Log File is \"{0}\"",
                                //                                    clogFilePath.PathResolved,
                                //                                    lineDateTime));
                                //Program.ConsoleErrors.Increment("Invalid Log Date/Time: " + line.Substring(0, 10) + "...");
                                //break;
                                ignoredTimeRange.SetMinMax(lineDateTime);
                                continue;
                            }
                        }
                    }

                    if (!ignoredTimeRange.IsEmpty())
                    {
                        Logger.Instance.WarnFormat("A Log {0} was ignored because this date range for node {1} was already processed. The ignored range was found in log file \"{2}\".",
                                                        ignoredTimeRange,
                                                        ipAddress,
                                                        clogFilePath.PathResolved);
                        ignoredTimeRange.MakeEmpty();
                    }

                    #endregion

                    exceptionOccurred = false;

                    #region Basic column Info

                    dataRow = dtCLog.NewRow();

                    dataRow["Data Center"] = dcName;
                    dataRow["Node IPAddress"] = ipAddress;
                    dataRow["Timestamp"] = lineDateTime;

                    minmaxDate.SetMinMax(lineDateTime);

                    dataRow["Indicator"] = parsedValues[ParserSettings.CLogLineFormats.IndicatorPos];

                    if (parsedValues[ParserSettings.CLogLineFormats.TaskPos][0] == '[')
                    {
                        string strItem = parsedValues[ParserSettings.CLogLineFormats.TaskPos];
                        int nPos = strItem.IndexOf(':');

                        if (nPos > 2)
                        {
                            var strTaskId = strItem.Substring(nPos + 1, strItem.Length - nPos - 2).Trim();
                            int taskId;

                            if (int.TryParse(strTaskId, out taskId))
                            {
                                dataRow["TaskId"] = taskId;
                            }

                            dataRow["Task"] = strItem.Substring(1, nPos - 1);
                        }
                        else
                        {
                            dataRow["Task"] = strItem.Substring(1, strItem.Length - 2);
                        }
                    }
                    else
                    {
                        dataRow["Task"] = parsedValues[ParserSettings.CLogLineFormats.TaskPos];
                    }

                    if (parsedValues[ParserSettings.CLogLineFormats.ItemPos][parsedValues[ParserSettings.CLogLineFormats.ItemPos].Length - 1] == ')')
                    {
                        var startPos = parsedValues[ParserSettings.CLogLineFormats.ItemPos].IndexOf('(');

                        if (startPos >= 0)
                        {
                            parsedValues[ParserSettings.CLogLineFormats.ItemPos] = parsedValues[ParserSettings.CLogLineFormats.ItemPos].Substring(0, startPos);
                        }
                    }
                    else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos].Contains(":"))
                    {
                        var startPos = parsedValues[ParserSettings.CLogLineFormats.ItemPos].LastIndexOf(':');

                        if (startPos >= 0)
                        {
                            parsedValues[ParserSettings.CLogLineFormats.ItemPos] = parsedValues[ParserSettings.CLogLineFormats.ItemPos].Substring(0, startPos);
                        }
                    }

                    dataRow["Item"] = parsedValues[ParserSettings.CLogLineFormats.ItemPos];

                    if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] != tableItem)
                    {
                        tableItemPos = -1;
                    }

                    #endregion

                    #region Describe Info

                    int itemPos = -1;
                    int itemValuePos = -1;

                    var logDesc = new StringBuilder();
                    var startRange = parsedValues[ParserSettings.CLogLineFormats.DescribePos] == "-" ? ParserSettings.CLogLineFormats.DescribePos + 1 : ParserSettings.CLogLineFormats.DescribePos;
                    bool handled = false;

                    if (parsedValues.Count > startRange && parsedValues[startRange][0] == '(')
                    {
                        ++startRange;
                    }

                    if (((string)dataRow["Task"]).StartsWith("SolrSecondaryIndex "))
                    {
                        var taskItems = RegExSolrSecondaryIndex.Split((string)dataRow["Task"]);

                        if (taskItems.Length > 1)
                        {
                            var splitItems = SplitTableName(taskItems[1]);

                            dataRow["Associated Item"] = splitItems.Item1 + '.' + splitItems.Item2;
                        }
                    }

                    for (int nCell = startRange; nCell < parsedValues.Count; ++nCell)
                    {
                        if (parsedValues[ParserSettings.CLogLineFormats.TaskPos].StartsWith("[MemtablePostFlush")
                                && (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "ColumnFamilyStore.java"
                                        || parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "IndexWriter.java"))
                        {
                            #region Solr Hard Commit
                            if (parsedValues[nCell] == "SecondaryIndex"
                                    && parsedValues.Last().StartsWith("Cql3SolrSecondaryIndex"))
                            {
                                var indexNameMatch = RegExSolrFlushIndexNames.Match(parsedValues.Last());

                                if (indexNameMatch.Success)
                                {
                                    var solrIndexes = new List<string>();

                                    foreach (Capture indexNameGrpCapture in indexNameMatch.Groups["indexname"].Captures)
                                    {
                                        if (!solrIndexes.Exists(i => i == indexNameGrpCapture.Value))
                                        {
                                            solrIndexes.Add(indexNameGrpCapture.Value);
                                        }
                                    }
                                    dataRow["Associated Item"] = string.Join(", ", solrIndexes);
                                    dataRow["Flagged"] = (int)LogFlagStatus.StatsOnly;
                                    handled = true;
                                }
                            }
                            else if (parsedValues[nCell].StartsWith("commitInternal"))
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.StatsOnly;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.TaskPos].StartsWith("[StreamReceiveTask")
                                    && (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "SecondaryIndexManager.java"
                                            || parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "IndexWriter.java"))
                        {
                            #region Solr Hard Commit DSE 5.x
                            //INFO  [StreamReceiveTask:832] 2017-06-29 10:18:31,883  SecondaryIndexManager.java:359 - Submitting index build of ckspsocp1_pymtsocp_solr_query_index

                            if (parsedValues[nCell] == "Submitting"
                                    && parsedValues[nCell + 1] == "index")
                            {
                                dataRow["Associated Item"] = parsedValues[nCell + 4];
                                dataRow["Flagged"] = (int)LogFlagStatus.StatsOnly;
                                handled = true;
                            }
                            else if (parsedValues[nCell].StartsWith("commitInternal"))
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.StatsOnly;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "CompactionController.java"
                                || parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "BigTableWriter.java")
                        {
                            #region CompactionController.java || BigTableWriter.java
                            //Compacting large row billing/account_payables:20160726:FMCC (348583137 bytes)
                            //Writing large partition oats/order_cycles:nasusnop01:2017-01-31 (139296027 bytes)

                            if (itemPos == nCell)
                            {
                                var ksTableName = parsedValues[nCell];
                                var keyDelimatorPos = ksTableName.IndexOf(':');

                                if (keyDelimatorPos > 0)
                                {
                                    ksTableName = ksTableName.Substring(0, keyDelimatorPos);
                                }

                                var splitItems = SplitTableName(ksTableName);

                                dataRow["Associated Item"] = splitItems.Item1 + '.' + splitItems.Item2;

                            }
                            if (nCell >= itemPos && parsedValues[nCell][parsedValues[nCell].Length - 1] == ')')
                            {
                                var firstParan = parsedValues[nCell].IndexOf('(');

                                if (firstParan >= 0)
                                {
                                    dataRow["Associated Value"] = ConvertInToMB(parsedValues[nCell].Substring(firstParan + 1, parsedValues[nCell].Length - firstParan - 2));
                                }
                            }

                            if (parsedValues[nCell] == "large" && parsedValues.ElementAtOrDefault(nCell + 1) == "row")
                            {
                                itemPos = nCell + 2;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = parsedValues[nCell - 1] + " large row";
                                handled = true;
                            }
                            else if (parsedValues[nCell] == "large" && parsedValues.ElementAtOrDefault(nCell + 1) == "partition")
                            {
                                itemPos = nCell + 2;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = parsedValues[nCell - 1] + " large partition";
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "ColumnFamilyStore.java")
                        {
                            #region ColumnFamilyStore.java
                            //INFO[SlabPoolCleaner] 2016-09-11 16:44:55,289  ColumnFamilyStore.java:1211 - Flushing largest CFS(Keyspace= 'homeKS', ColumnFamily= 'homebase_tasktracking_ops_l3') to free up room.Used total: 0.33/0.00, live: 0.33/0.00, flushing: 0.00/0.00, this: 0.07/0.07
                            //INFO[SlabPoolCleaner] 2016-09-11 16:44:55,289  ColumnFamilyStore.java:905 - Enqueuing flush of homebase_tasktracking_ops_l3: 315219514 (7%) on-heap, 0 (0%) off-heap
                            //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:55,290  Memtable.java:347 - Writing Memtable-homebase_tasktracking_ops_l3@994827943(53.821MiB serialized bytes, 857621 ops, 7%/0% of on/off-heap limit)
                            //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:56,558  Memtable.java:382 - Completed flushing /mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3-tmp-ka-15175-Data.db (11.901MiB) for commitlog position ReplayPosition(segmentId= 1473433813485, position= 31065241)

                            if (parsedValues[nCell] == "Flushing"
                                    || (parsedValues[nCell] == "Enqueuing" && parsedValues[nCell + 1] == "flush"))
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.MemTblFlush;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "Memtable.java")
                        {
                            #region Memtable.java
                            //INFO[SlabPoolCleaner] 2016-09-11 16:44:55,289  ColumnFamilyStore.java:1211 - Flushing largest CFS(Keyspace= 'homeKS', ColumnFamily= 'homebase_tasktracking_ops_l3') to free up room.Used total: 0.33/0.00, live: 0.33/0.00, flushing: 0.00/0.00, this: 0.07/0.07
                            //INFO[SlabPoolCleaner] 2016-09-11 16:44:55,289  ColumnFamilyStore.java:905 - Enqueuing flush of homebase_tasktracking_ops_l3: 315219514 (7%) on-heap, 0 (0%) off-heap
                            //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:55,290  Memtable.java:347 - Writing Memtable-homebase_tasktracking_ops_l3@994827943(53.821MiB serialized bytes, 857621 ops, 7%/0% of on/off-heap limit)
                            //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:56,558  Memtable.java:382 - Completed flushing /mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3-tmp-ka-15175-Data.db (11.901MiB) for commitlog position ReplayPosition(segmentId= 1473433813485, position= 31065241)

                            //INFO [FlushWriter:262] 2017-03-06 17:37:15,303 Memtable.java (line 362) Writing Memtable-readerdata@354586264(3332/33320 serialized/live bytes, 70 ops)
                            //INFO[FlushWriter: 262] 2017 - 03 - 06 17:37:15,320 Memtable.java(line 402) Completed flushing / var / lib / cassandra / data / ampdata / readerdata / ampdata - readerdata - jb - 116694 - Data.db(4876 bytes) for commitlog position ReplayPosition(segmentId = 1488732865093, position = 30876408)

                            //DEBUG [MemtableFlushWriter:23] 2017-08-11 13:03:04,864  Memtable.java:364 - Writing Memtable-logmnemonicrecentvalue@1160967395(268.396KiB serialized bytes, 3174 ops, 0%/0% of on/off-heap limit)
                            //DEBUG [MemtableFlushWriter:23] 2017-08-11 13:03:04,872  Memtable.java:397 - Completed flushing / d5 / data / rts_data / logmnemonicrecentvalue - bb61e781f7d111e692c82747a9704109 / mc - 7750112 - big - Data.db(95.876KiB) for commitlog position ReplayPosition(segmentId = 1502112368275, position = 18085469)


                            if ((parsedValues[ParserSettings.CLogLineFormats.TaskPos].Contains("MemtableFlushWriter")
                                    || (parsedValues[ParserSettings.CLogLineFormats.TaskPos].Contains("FlushWriter")))
                                            && (parsedValues[nCell] == "Writing" || parsedValues[nCell] == "Completed"))
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.MemTblFlush;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "SSTableWriter.java")
                        {
                            #region SSTableWriter.java
                            //WARN  [CompactionExecutor:6] 2016-06-07 06:57:44,146  SSTableWriter.java:240 - Compacting large partition kinesis_events/event_messages:49c023da-0bb8-46ce-9845-111514b43a63 (186949948 bytes)
                            //WARN  [CompactionExecutor:705441] 2016-10-05 13:09:40,934 SSTableWriter.java:241 - Compacting large partition handled_exception1/summary_pt1h:5670561a6c33dc0f00f11443:2016-10-01-14-00-00:total (280256103 bytes)

                            if (itemPos == nCell)
                            {
                                var ksTableName = parsedValues[nCell];
                                var keyDelimatorPos = ksTableName.IndexOf(':');

                                if (keyDelimatorPos > 0)
                                {
                                    ksTableName = ksTableName.Substring(0, keyDelimatorPos);
                                }

                                var splitItems = SplitTableName(ksTableName);

                                dataRow["Associated Item"] = splitItems.Item1 + '.' + splitItems.Item2;

                            }
                            if (nCell >= itemPos && parsedValues[nCell][parsedValues[nCell].Length - 1] == ')')
                            {
                                var firstParan = parsedValues[nCell].IndexOf('(');

                                if (firstParan >= 0)
                                {
                                    dataRow["Associated Value"] = ConvertInToMB(parsedValues[nCell].Substring(firstParan + 1, parsedValues[nCell].Length - firstParan - 2));
                                }
                            }

                            if (parsedValues[nCell] == "large" && parsedValues.ElementAtOrDefault(nCell + 1) == "partition")
                            {
                                itemPos = nCell + 2;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Compacting large partition";
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "GCInspector.java")
                        {
                            #region GCInspector.java
                            //GCInspector.java (line 116) GC for ParNew: 394 ms for 1 collections, 13571498424 used; max is 25340346368
                            //GCInspector.java (line 119) GC forConcurrentMarkSweep: 15132 ms for 2 collections, 4229845696 used; max is 25125584896
                            // ConcurrentMarkSweep GC in 2083ms. CMS Old Gen: 8524829104 -> 8531031448; CMS Perm Gen: 68555136 -> 68555392; Par Eden Space: 1139508352 -> 47047616; Par Survivor Space: 35139688 -> 45900968
                            //GCInspector.java:258 - G1 Young Generation GC in 264ms.  G1 Eden Space: 3470786560 -> 0; G1 Old Gen: 2689326672 -> 2934172000; G1 Survivor Space: 559939584 -> 35651584;
                            //WARN [ScheduledTasks:1] 2013-04-10 10:18:14,403 GCInspector.java (line 145) Heap is 0.9610030442856479 full.  You may need to reduce memtable and/or cache sizes.  Cassandra will now flush up to the two largest memtables to free up memory.  Adjust flush_largest_memtables_at threshold in cassandra.yaml if you don't want Cassandra to do this automatically

                            if (nCell == itemPos)
                            {
                                var time = DetermineTime(parsedValues[nCell]);

                                if (time is int && (int)time >= gcPausedFlagThresholdInMS)
                                {
                                    dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                    dataRow["Exception"] = "GC Threshold";
                                    handled = true;
                                }
                                dataRow["Associated Value"] = time;
                            }
                            if (parsedValues[nCell] == "ParNew:"
                                    || parsedValues[nCell] == "forConcurrentMarkSweep:"
                                    || parsedValues[nCell] == "ConcurrentMarkSweep:")
                            {
                                itemPos = nCell + 1;
                            }
                            else if (parsedValues[nCell] == "ConcurrentMarkSweep" && parsedValues[nCell + 1] == "GC")
                            {
                                itemPos = nCell + 3;
                            }
                            else if (parsedValues[nCell] == "Young")
                            {
                                itemPos = nCell + 4;
                            }
                            else if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN" && parsedValues[nCell] == "Heap" && parsedValues[nCell + 3] == "full")
                            {
                                decimal numValue;

                                if (decimal.TryParse(parsedValues[nCell], out numValue))
                                {
                                    dataRow["Associated Value"] = numValue;
                                }

                                //dataRow["Associated Item"] = "Heap Full";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Heap Full";
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "FailureDetector.java")
                        {
                            #region FailureDetector.java
                            //Not marking nodes down due to local pause of 12817405727 > 5000000000
                            if (itemPos == nCell)
                            {
                                long nbr;

                                if (long.TryParse(parsedValues[nCell], out nbr))
                                {
                                    dataRow["Associated Value"] = nbr;
                                }

                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Pause(FailureDetector)";
                                handled = true;
                            }

                            if (parsedValues[nCell] == "marking" && parsedValues.ElementAtOrDefault(nCell + 2) == "down" && parsedValues.ElementAtOrDefault(nCell + 6) == "pause")
                            {
                                itemPos = nCell + 8;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "BatchStatement.java")
                        {
                            #region BatchStatement.java
                            //BatchStatement.java (line 226) Batch of prepared statements for [clearcore.documents_case] is of size 71809, exceeding specified threshold of 65536 by 6273.
                            //WARN  [SharedPool-Worker-3] 2016-12-03 00:11:32,802  BatchStatement.java:252 - Batch of prepared statements for [Sandy.referral_source] is of size 71016, exceeding specified threshold of 65536 by 5480.
                            //WARN  [SharedPool-Worker-4] 2016-10-31 22:49:07,808  BatchStatement.java:253 - Batch of prepared statements for [usprodofrs.newoffer_3_2_10, usprodofrs.identificationoffer_3_0_1, usprodofrs.promotooffer_3_0_1] is of size 68486, exceeding specified threshold of 65536 by 2950.
                            if (nCell == itemPos)
                            {
                                if (parsedValues[nCell].Contains(','))
                                {
                                    var splitKSTbls = parsedValues[nCell].Split(',');

                                    dataRow["Associated Item"] = string.Join(",", splitKSTbls.Select(i =>
                                                                                {
                                                                                    var splitItems = SplitTableName(parsedValues[nCell]);

                                                                                    return splitItems.Item1 + '.' + splitItems.Item2;
                                                                                }));
                                }
                                else
                                {
                                    var splitItems = SplitTableName(parsedValues[nCell]);

                                    dataRow["Associated Item"] = splitItems.Item1 + '.' + splitItems.Item2;
                                }
                            }
                            if (nCell == itemValuePos)
                            {
                                int batchSize;

                                if (int.TryParse(parsedValues[nCell].Replace(",", string.Empty), out batchSize))
                                {
                                    dataRow["Associated Value"] = batchSize;
                                }
                            }
                            if (parsedValues[nCell] == "Batch")
                            {
                                itemPos = nCell + 5;
                                itemValuePos = nCell + 9;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Batch Size Exceeded";
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN" && parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "NoSpamLogger.java")
                        {
                            #region NoSpamLogger.java
                            //NoSpamLogger.java:94 - Unlogged batch covering 80 partitions detected against table[hlservicing.lvl1_bkfs_invoicechronology]. You should use a logged batch for atomicity, or asynchronous writes for performance.
                            //NoSpamLogger.java:94 - Unlogged batch covering 94 partitions detected against tables [hl_data_commons.l3_heloc_fraud_score_hist, hl_data_commons.l3_heloc_fraud_score]. You should use a logged batch for atomicity, or asynchronous writes for performance.
                            //Maximum memory usage reached (536870912 bytes), cannot allocate chunk of 1048576 bytes
                            //
                            if (nCell == itemPos)
                            {
                                if (parsedValues[nCell] == "table" || parsedValues[nCell] == "tables")
                                {
                                    ++itemPos;
                                }
                                else
                                {
                                    var bracketPos = parsedValues[nCell].IndexOf('[');
                                    var ksTblNames = parsedValues[nCell].Substring(bracketPos < 0 ? 0 : bracketPos + 1, parsedValues[nCell].Length - (bracketPos < 0 ? 0 : bracketPos + 3))
                                                            .Split(',').Select(item => item.Trim()).Sort();

                                    dataRow["Associated Item"] = string.Join(", ", ksTblNames);
                                }
                            }
                            if (nCell == itemValuePos)
                            {
                                var strInt = parsedValues[nCell];
                                int batchSize;

                                if (strInt[0] == '(')
                                {
                                    strInt = strInt.Substring(1, strInt.Length - 2);
                                }

                                if (int.TryParse(strInt, out batchSize))
                                {
                                    dataRow["Associated Value"] = batchSize;
                                }
                            }
                            if (parsedValues[nCell].EndsWith("logged") && parsedValues[nCell + 1] == "batch" && parsedValues[nCell + 2] == "covering")
                            {
                                itemPos = nCell + 7;
                                itemValuePos = nCell + 3;
                                dataRow["Exception"] = System.Globalization.CultureInfo.CurrentCulture.TextInfo.ToTitleCase(parsedValues[nCell]) + " Batch Partitions";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;
                            }
                            else if (parsedValues[nCell] == "Maximum" && parsedValues[nCell + 1] == "memory" && parsedValues[nCell + 2] == "reached")
                            {
                                itemValuePos = nCell + 9;
                                dataRow["Exception"] = "Maximum Memory Reached cannot Allocate";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "SliceQueryFilter.java")
                        {
                            #region SliceQueryFilter.java
                            //SliceQueryFilter.java (line 231) Read 14 live and 1344 tombstone cells in cma.mls_records_property (see tombstone_warn_threshold). 5000 columns was requested, slices=[-]
                            // Scanned over 100000 tombstones in homeKS.homebase_he_operations_pt; query aborted (see tombstone_failure_threshold)
                            if (nCell == itemPos)
                            {
                                var splitItems = SplitTableName(parsedValues[nCell]);
                                string tableName = splitItems.Item2;

                                if (tableName[tableName.Length - 1] == ';')
                                {
                                    tableName = tableName.Substring(0, tableName.Length - 1);
                                }

                                dataRow["Associated Item"] = splitItems.Item1 + '.' + tableName;
                            }
                            if (nCell == itemValuePos)
                            {
                                object tbNum;
                                int tombStones = 0;
                                int reads = 0;

                                if (StringFunctions.ParseIntoNumeric(parsedValues[nCell], out tbNum))
                                {
                                    tombStones = (int)tbNum;
                                }
                                if (StringFunctions.ParseIntoNumeric(parsedValues[nCell - 3], out tbNum))
                                {
                                    reads = (int)tbNum;
                                }

                                if (tombStones > reads)
                                {
                                    dataRow["Associated Value"] = tombStones;
                                    dataRow["Exception"] = "Query Tombstones Warning";
                                }
                                else
                                {
                                    dataRow["Associated Value"] = reads;
                                    dataRow["Exception"] = "Query Reads Warning";
                                }
                            }
                            if (parsedValues[nCell] == "Read")
                            {
                                itemPos = nCell + 8;
                                itemValuePos = nCell + 4;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;
                            }
                            else if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "ERROR" && parsedValues[nCell] == "Scanned" && parsedValues[nCell + 1] == "over")
                            {
                                itemPos = nCell + 5;
                                itemValuePos = 2;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Query Tombstones Aborted";
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "ReadCommand.java")
                        {
                            #region ReadCommand.java
                            // ReadCommand.java:509 - Read 1000 live rows and 2506 tombstone cells for query SELECT * FROM ods.d_account WHERE token(accnt_id, tenant_id) > token(1:2017853945, 1) AND token(accnt_id, tenant_id) <= 2028415374372988170 AND () = () LIMIT 1000 (see tombstone_warn_threshold)
                            // ReadCommand.java:509 - Read 16882 live rows and 1256 tombstone cells for query SELECT *FROM product_v2.clientproducts_by_client_class_v2 WHERE token(client_id, class_id) > -58434642643959632 AND token(client_id, class_id) <= 170473102188219941 LIMIT 100000(see tombstone_warn_threshold)

                            if (nCell > itemPos && parsedValues[nCell].ToLower() == "from")
                            {
                                var splitItems = SplitTableName(parsedValues[nCell + 1]);
                                string tableName = splitItems.Item2;

                                if (tableName[tableName.Length - 1] == ';')
                                {
                                    tableName = tableName.Substring(0, tableName.Length - 1);
                                }

                                dataRow["Associated Item"] = splitItems.Item1 + '.' + tableName;
                            }
                            else if (parsedValues[nCell] == "tombstone")
                            {
                                object tbNum;
                                int tombStones = 0;
                                int reads = 0;

                                if (StringFunctions.ParseIntoNumeric(parsedValues[nCell - 1], out tbNum))
                                {
                                    tombStones = (int)tbNum;
                                }
                                if (StringFunctions.ParseIntoNumeric(parsedValues[nCell - 5], out tbNum))
                                {
                                    reads = (int)tbNum;
                                }

                                if (tombStones > reads)
                                {
                                    dataRow["Associated Value"] = tombStones;
                                    dataRow["Exception"] = "Query Tombstones Warning";
                                }
                                else
                                {
                                    dataRow["Associated Value"] = reads;
                                    dataRow["Exception"] = "Query Reads Warning";
                                }

                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                itemPos = nCell + 1;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "HintedHandoffMetrics.java" || parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "HintedHandOffManager.java")
                        {
                            #region HintedHandoffMetrics.java
                            //		WARN  [HintedHandoffManager:1] 2016-07-25 04:26:10,445  HintedHandoffMetrics.java:79 - /10.170.110.191 has 1711 dropped hints, because node is down past configured hint window.
                            //INFO  [HintedHandoff:2] 2016-10-29 09:04:51,254  HintedHandOffManager.java:486 - Timed out replaying hints to /10.12.51.20; aborting (0 delivered)
                            if (parsedValues[nCell] == "dropped")
                            {
                                //dataRow["Associated Item"] = "Dropped Hints";
                                dataRow["Exception"] = "Dropped Hints (node down)";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Associated Value"] = int.Parse(parsedValues[nCell - 1]);
                                handled = true;

                                if (LookForIPAddress(parsedValues[nCell - 3], ipAddress, out lineIPAddress))
                                {
                                    dataRow["Associated Item"] = lineIPAddress;
                                    string downDCName;

                                    if (ProcessFileTasks.DetermineDataCenterFromIPAddress(lineIPAddress, out downDCName))
                                    {
                                        var nodedownDR = dtCLog.NewRow();
                                        nodedownDR["Data Center"] = downDCName;
                                        nodedownDR["Node IPAddress"] = lineIPAddress;
                                        nodedownDR["Timestamp"] = dataRow["Timestamp"];
                                        nodedownDR["Indicator"] = dataRow["Indicator"];
                                        nodedownDR["Task"] = dataRow["Task"];
                                        nodedownDR["TaskId"] = dataRow["TaskId"];
                                        nodedownDR["Item"] = dataRow["Item"];
                                        nodedownDR["Exception"] = "Node Marked Down (Dropped Hints)";
                                        nodedownDR["Flagged"] = dataRow["Flagged"];
                                        nodedownDR["Associated Item"] = dataRow["Node IPAddress"];
                                        nodedownDR["Associated Value"] = dataRow["Associated Value"];
                                        nodedownDR["Description"] = string.Format("{0} marked this node down. {1} dropped hints", dataRow["Node IPAddress"].ToString(), dataRow["Associated Value"].ToString());

                                        AddRowToLogDataTable(dtCLog, nodedownDR, isDebugLogFile);
                                    }
                                }
                            }
                            else if (parsedValues[nCell] == "Timed"
                                        && parsedValues[nCell + 1] == "out")
                            {
                                dataRow["Exception"] = "Hints (timeout)";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;

                                var matchNbrs = RegExNumerics.Matches(parsedValues[nCell + 7]);

                                if (matchNbrs.Count > 0)
                                {
                                    dataRow["Associated Value"] = int.Parse(matchNbrs[0].Value);
                                }
                                else
                                {
                                    dataRow["Associated Value"] = 0;
                                }

                                if (LookForIPAddress(parsedValues[nCell + 5].Replace(";", string.Empty), ipAddress, out lineIPAddress))
                                {
                                    dataRow["Associated Item"] = lineIPAddress;

                                    string timeoutDCName;

                                    if (ProcessFileTasks.DetermineDataCenterFromIPAddress(lineIPAddress, out timeoutDCName))
                                    {
                                        var nodetimeoutDR = dtCLog.NewRow();
                                        nodetimeoutDR["Data Center"] = timeoutDCName;
                                        nodetimeoutDR["Node IPAddress"] = lineIPAddress;
                                        nodetimeoutDR["Timestamp"] = dataRow["Timestamp"];
                                        nodetimeoutDR["Indicator"] = dataRow["Indicator"];
                                        nodetimeoutDR["Task"] = dataRow["Task"];
                                        nodetimeoutDR["TaskId"] = dataRow["TaskId"];
                                        nodetimeoutDR["Item"] = dataRow["Item"];
                                        nodetimeoutDR["Exception"] = "Node Not Responding (Timed Out Hints)";
                                        nodetimeoutDR["Flagged"] = dataRow["Flagged"];
                                        nodetimeoutDR["Associated Item"] = dataRow["Node IPAddress"];
                                        nodetimeoutDR["Associated Value"] = dataRow["Associated Value"];
                                        nodetimeoutDR["Description"] = string.Format("{0} tried to replay hints to this node but replay timed out and was aborted. {1} delivered", dataRow["Node IPAddress"].ToString(), dataRow["Associated Value"].ToString());

                                        AddRowToLogDataTable(dtCLog, nodetimeoutDR, isDebugLogFile);
                                    }
                                }
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "StorageService.java")
                        {
                            #region StorageService.java
                            //	WARN [ScheduledTasks:1] 2013-04-10 10:18:12,042 StorageService.java (line 2645) Flushing CFS(Keyspace='Company', ColumnFamily='01_Meta') to relieve memory pressure
                            //INFO  [main] 2016-10-08 15:48:11,974  StorageService.java:1715 - Node /192.168.247.61 state jump to NORMAL
                            //INFO[StorageServiceShutdownHook] 2016 - 10 - 08 15:45:53,400  StorageService.java:1715 - Node / 192.168.247.61 state jump to shutdown
                            //INFO  [main] 2016-10-08 15:48:11,665  StorageService.java:622 - Cassandra version: 2.1.14.1272
                            //INFO[main] 2016 - 10 - 08 15:48:11,665  StorageService.java:623 - Thrift API version: 19.39.0
                            //INFO[main] 2016 - 10 - 08 15:48:11,665  StorageService.java:624 - CQL supported versions: 2.0.0,3.2.1(default: 3.2.1)
                            //INFO  [ACCEPT-vmdse0408c1.andersen.local/192.168.247.60] 2016-10-26 22:15:56,042  MessagingService.java:1018 - MessagingService has terminated the accept() thread
                            //INFO  [RMI TCP Connection(20943)-10.12.50.10] 2016-10-17 15:50:56,821  StorageService.java:2891 - starting user-requested repair of range [(-1537228672809129313,-1531223873305968652]] for keyspace prod_fcra and column families [ifps_inquiry, ifps_contribution, rtics_contribution, rtics_inquiry, bics_contribution, avlo_inquiry, bics_inquiry]
                            //INFO[Thread - 361905] 2016 - 10 - 17 15:50:56,822  StorageService.java:2970 - Starting repair command #9, repairing 1 ranges for keyspace prod_fcra (parallelism=PARALLEL, full=true)

                            if (nCell >= itemValuePos && parsedValues[nCell].Contains("Keyspace="))
                            {
                                nCell = -1;
                                var kstblValues = Common.StringFunctions.Split(parsedValues[nCell],
                                                                                new char[] { ' ', ',', '=', '(', ')' },
                                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                                Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
                                string ksName = null;
                                string tblName = null;

                                for (int nIndex = 0; nIndex < kstblValues.Count; ++nIndex)
                                {
                                    if (kstblValues[nIndex] == "Keyspace")
                                    {
                                        ksName = kstblValues[++nIndex];
                                    }
                                    else if (kstblValues[nIndex] == "ColumnFamily")
                                    {
                                        tblName = kstblValues[++nIndex];
                                    }
                                }

                                dataRow["Associated Item"] = ksName + "." + tblName;
                            }

                            if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN" && parsedValues[nCell] == "Flushing")
                            {
                                //dataRow["Associated Item"] = "Flushing CFS";
                                dataRow["Exception"] = "CFS Flush";
                                itemValuePos = nCell + 1;
                                handled = true;
                            }
                            else if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "INFO" && parsedValues[nCell] == "Node")
                            {
                                itemValuePos = nCell + 3;
                            }
                            else if ((parsedValues[nCell] == "starting"
                                        && parsedValues[nCell + 1] == "user-requested"
                                        && parsedValues[nCell + 2] == "repair")
                                    || (parsedValues[nCell] == "Starting"
                                            && parsedValues[nCell + 1] == "repair"
                                            && parsedValues[nCell + 2] == "command"))
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.ReadRepair;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "StatusLogger.java")
                        {
                            #region StatusLogger.java
                            //StatusLogger.java:51 - Pool Name                    Active   Pending      Completed   Blocked  All Time Blocked
                            //StatusLogger.java:66 - MutationStage                     4         0     2383662788         0                 0
                            //StatusLogger.java:75 - CompactionManager                 2         3
                            //StatusLogger.java:87 - MessagingService                n/a       0/1
                            //
                            //StatusLogger.java:97 - Cache Type                     Size                 Capacity               KeysToSave
                            //StatusLogger.java:99 - KeyCache                   95002384                104857600                      all
                            //
                            //StatusLogger.java:112 - ColumnFamily                Memtable ops,data
                            //StatusLogger.java:115 - dse_perf.node_slow_log           8150,3374559

                            if (parsedValues[nCell] == "ColumnFamily" || parsedValues[nCell] == "Table")
                            {
                                tableItem = parsedValues[ParserSettings.CLogLineFormats.ItemPos];
                                tableItemPos = nCell;
                            }
                            else if (parsedValues[nCell] == "Pool")
                            {
                                tableItem = null;
                                tableItemPos = -1;
                            }
                            else if (parsedValues[nCell] == "Cache")
                            {
                                tableItem = null;
                                tableItemPos = -1;
                            }
                            else if (nCell == tableItemPos)
                            {
                                var splitItems = SplitTableName(parsedValues[nCell], null);

                                dataRow["Associated Item"] = splitItems.Item1 + '.' + splitItems.Item2;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "MessagingService.java")
                        {
                            #region MessagingService.java
                            //MessagingService.java --  MUTATION messages were dropped in last 5000 ms: 43 for internal timeout and 0 for cross node timeout
                            //INFO  [ACCEPT-vmdse0408c1.andersen.local/192.168.247.60] 2016-10-26 22:15:56,042  MessagingService.java:1018 - MessagingService has terminated the accept() thread

                            if (nCell == itemPos)
                            {
                                var valueDR = dataRow["Associated Value"];
                                int nbrDrops = 0;

                                int.TryParse(parsedValues[nCell], out nbrDrops);

                                if (valueDR == DBNull.Value)
                                {
                                    dataRow["Associated Value"] = nbrDrops;
                                    itemPos = nCell + 5;
                                }
                                else
                                {
                                    int? currentDrops = valueDR as int?;

                                    if (currentDrops.HasValue)
                                    {
                                        dataRow["Associated Value"] = nbrDrops + currentDrops.Value;
                                    }
                                    else
                                    {
                                        dataRow["Associated Value"] = nbrDrops;
                                    }

                                }
                            }
                            if (parsedValues[nCell] == "MUTATION")
                            {
                                //dataRow["Associated Item"] = "Dropped Mutations";
                                dataRow["Exception"] = "Dropped Mutations";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;
                                itemPos = nCell + 8;
                            }
                            else if (parsedValues[nCell] == "terminated"
                                        && parsedValues[nCell + 2].StartsWith("accept")
                                        && parsedValues[nCell + 3] == "thread")
                            {
                                dataRow["Exception"] = "Node Shutdown";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "CompactionTask.java")
                        {
                            #region CompactionTask.java
                            //INFO  [CompactionExecutor:4657] 2016-06-12 06:26:25,534  CompactionTask.java:274 - Compacted 4 sstables to [/data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/system-size_estimates-ka-11348,]. 2,270,620 bytes to 566,478 (~24% of original) in 342ms = 1.579636MB/s. 40 total partitions merged to 10. Partition merge counts were {4:10, }
                            //DEBUG	CompactionExecutor	CompactionTask.java	 Compacted (aa83aec0-6a0b-11e6-923c-7d02e3681807) 4 sstables to [/var/lib/cassandra/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/mb-217-big,] to level=0. 64,352 bytes to 62,408 (~96% of original) in 1,028ms = 0.057896MB/s. 0 total partitions merged to 1,428. Partition merge counts were {1:1456, }

                            if (itemPos > 0
                                    && parsedValues[nCell].EndsWith("of original)")
                                    && parsedValues[nCell + 1] == "in")
                            {
                                object time = DetermineTime(parsedValues[nCell + 2]);
                                object rate = null;

                                if (Common.StringFunctions.ParseIntoNumeric(parsedValues[nCell + 4].Substring(0, parsedValues[nCell + 4].Length - 4), out rate)
                                    && compactionFlagThresholdAsIORate > 0)
                                {
                                    if ((dynamic)rate < compactionFlagThresholdAsIORate)
                                    {
                                        dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                        dataRow["Exception"] = "Compaction IO Rate Warning";
                                        handled = true;
                                    }
                                }

                                if (!handled
                                        && compactionFlagThresholdInMS >= 0
                                        && time is int
                                        && (int)time >= compactionFlagThresholdInMS)
                                {
                                    dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                    dataRow["Exception"] = "Compaction Latency Warning";
                                    handled = true;
                                }

                                dataRow["Associated Value"] = string.Format("{0} ms; {1} MB/sec", time, rate);
                            }
                            else if (parsedValues[nCell] == "Compacted")
                            {
                                itemPos = nCell + 1;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "RepairSession.java" || parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "RepairJob.java")
                        {
                            #region RepairSession.java RepairJob.java
                            //ERROR [AntiEntropySessions:1857] 2016-06-10 21:56:53,281  RepairSession.java:276 - [repair #dc161200-2f4d-11e6-bd0c-93368bf2a346] Cannot proceed on repair because a neighbor (/10.27.34.54) is dead: session failed
                            //INFO[AntiEntropySessions: 9665] 2016 - 08 - 10 07:08:06, 218  RepairJob.java:163 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] requesting merkle trees for memberfundingeventaggregate (to [/10.211.34.167, /10.211.34.165, /10.211.34.164, /10.211.34.158, /10.211.34.150])
                            //ERROR [AntiEntropySessions:1] 2016-10-19 10:42:49,900  RepairSession.java:303 - [repair #ff8f8db0-95eb-11e6-8ab5-71e7251f2ea8] session completed with the following error

                            if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "ERROR")
                            {
                                if (parsedValues[nCell].ToLower() == "failed")
                                {
                                    dataRow["Exception"] = "Read Repair Failed";
                                    itemPos = 0;
                                }
                                else if (parsedValues[nCell].ToLower() == "session" && parsedValues[nCell + 1] == "completed")
                                {
                                    dataRow["Exception"] = "Read Repair Session completed with Error";
                                    itemPos = 0;
                                }
                                else if (itemPos == -1 && dataRow["Associated Item"] == DBNull.Value)
                                {
                                    dataRow["Exception"] = "Read Repair Error";
                                }
                            }

                            if (parsedValues[nCell].StartsWith("[repair "))
                            {
                                dataRow["Associated Value"] = parsedValues[nCell];
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "CqlSlowLogWriter.java")
                        {
                            #region CqlSlowLogWriter.java
                            //INFO  [CqlSlowLog-Writer-thread-0] 2016-08-16 01:42:34,277  CqlSlowLogWriter.java:151 - Recording statements with duration of 60248 in slow log
                            //WARN  [CqlSlowLog-Writer-thread-0] 2016-09-26 15:48:23,806  CqlSlowLogWriter.java:245 - Error writing to cql slow log
                            if (nCell == itemValuePos)
                            {
                                var queryTime = int.Parse(parsedValues[nCell]);

                                if (queryTime >= slowLogQueryThresholdInMS)
                                {
                                    dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                    handled = true;
                                    //dataRow["Associated Item"] = "Compaction Pause";
                                    dataRow["Exception"] = "Slow Query";
                                }
                                dataRow["Associated Value"] = queryTime;
                            }
                            else if (parsedValues[nCell] == "Recording")
                            {
                                itemValuePos = nCell + 5;
                                dataRow["Associated Item"] = "Slow Query Writing to dse_perf.node_slow_log table";
                            }
                            else if (parsedValues[nCell] == "Error")
                            {
                                dataRow["Associated Item"] = string.Join(" ", parsedValues.GetRange(nCell, parsedValues.Count - nCell));
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "CqlSolrQueryExecutor.java")
                        {
                            #region CqlSolrQueryExecutor.java
                            //ERROR [SharedPool-Worker-1] 2016-08-29 16:28:03,882  CqlSolrQueryExecutor.java:409 - No response after timeout: 60000
                            if (parsedValues[nCell] == "No" && parsedValues[nCell + 1] == "response" && parsedValues[nCell + 3] == "timeout")
                            {
                                dataRow["Exception"] = "Solr Timeout";
                                dataRow["Associated Value"] = int.Parse(parsedValues[nCell + 4]);
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "SolrCore.java")
                        {
                            #region SolrCore.java
                            //WARN  [SolrSecondaryIndex ks_invoice.invoice index initializer.] 2016-08-17 00:36:22,480  SolrCore.java:1726 - [ks_invoice.invoice] PERFORMANCE WARNING: Overlapping onDeckSearchers=2

                            if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN" && parsedValues[nCell] == "PERFORMANCE" && parsedValues[nCell + 1] == "WARNING:")
                            {
                                var splitItems = SplitTableName(parsedValues[nCell - 1]);
                                var ksTableName = splitItems.Item1 + '.' + splitItems.Item2;

                                dataRow["Exception"] = "Solr Performance Warning";
                                dataRow["Associated Item"] = ksTableName;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "IndexSchema.java")
                        {
                            #region IndexSchema.java
                            //INFO  [http-10.203.40.61-8983-1] 2017-03-11 07:00:39,426  IndexSchema.java:470 - [null] Schema name=autoSolrSchema
                            //WARN  [http-10.203.40.61-8983-1] 2017-03-11 07:00:39,432  IndexSchema.java:745 - Field q is not multivalued and destination for multiple copyFields (15)
                            if (parsedValues[nCell] == "Schema" && parsedValues[nCell + 1].StartsWith("name"))
                            {
                                currentSolrSchemaName = parsedValues[nCell] + " " + parsedValues[nCell + 1];
                            }
                            else if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN")
                            {
                                dataRow["Exception"] = "Solr Warning";
                                dataRow["Associated Item"] = currentSolrSchemaName;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "CassandraIndexSchema.java")
                        {
                            #region CassandraIndexSchema.java
                            //WARN[http - 10.203.40.61 - 8983 - 1] 2017 - 03 - 11 07:00:39,433  CassandraIndexSchema.java:534 - No Cassandra column found for field: transactions_contentRef
                            //Unless it's a non stored copy field, Cassandra columns must have the same case or be quoted in order to be correctly mapped.
                            if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN"
                                    && parsedValues[nCell] == "No"
                                    && parsedValues.ElementAtOrDefault(nCell + 2) == "column"
                                    && parsedValues.ElementAtOrDefault(nCell + 3) == "found")
                            {
                                dataRow["Exception"] = "Solr Warning: Cassandra Column Not Found";
                                dataRow["Associated Item"] = currentSolrSchemaName;
                                handled = true;
                                consumeNextLine = 1;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "JVMStabilityInspector.java")
                        {
                            #region JVMStabilityInspector.java
                            //ERROR [MessagingService-Incoming-/10.12.49.27] 2016-09-28 18:53:54,898  JVMStabilityInspector.java:106 - JVM state determined to be unstable.  Exiting forcefully due to:
                            //java.lang.OutOfMemoryError: Java heap space

                            if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "ERROR" && parsedValues[nCell] == "JVM" && parsedValues.ElementAtOrDefault(nCell + 5) == "unstable")
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Associated Item"] = string.Join(" ", parsedValues.Skip(nCell + 5));
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "WorkPool.java")
                        {
                            #region WorkPool.java
                            //WARN  [commitScheduler-4-thread-1] 2016-09-28 18:53:32,436  WorkPool.java:413 - Timeout while waiting for workers when flushing pool Index; current timeout is 300000 millis, consider increasing it, or reducing load on the node.
                            //Failure to flush may cause excessive growth of Cassandra commit log.

                            if (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN" && parsedValues[nCell] == "Timeout"
                                    && parsedValues.ElementAtOrDefault(nCell + 4) == "workers"
                                    && parsedValues.ElementAtOrDefault(nCell + 6) == "flushing")
                            {
                                var nxtLine = readNextLine?.Trim();
                                if (nxtLine != null
                                        && nxtLine.StartsWith("Failure")
                                        && nxtLine.Contains("commit log"))
                                {
                                    dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                    handled = true;
                                    dataRow["Associated Item"] = nxtLine;
                                    dataRow["Exception"] = "CommitLogFlushFailure";
                                    skipNextRead = true;
                                }
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "QueryProcessor.java")
                        {
                            #region QueryProcessor.java
                            ////QueryProcessor.java:139 - 21 prepared statements discarded in the last minute because cache limit reached (66270208 bytes)
                            //ERROR [SharedPool-Worker-1] 2017-05-31 23:59:50,096  QueryProcessor.java:545 - The statement: [SELECT bre_grid_txt FROM rapid_direct.dirct_app WHERE app_id = 129712281

                            if (parsedValues[nCell] == "prepared" && parsedValues.ElementAtOrDefault(nCell + 1) == "statements" && parsedValues.ElementAtOrDefault(nCell + 2) == "discarded")
                            {
                                int preparedSize;

                                if (int.TryParse(parsedValues[nCell - 1], out preparedSize))
                                {
                                    dataRow["Exception"] = "Prepared Discarded";
                                    dataRow["Associated Value"] = preparedSize;
                                    handled = true;
                                }
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "ThriftServer.java")
                        {
                            #region ThriftServer.java
                            //INFO  [Thread-2] 2016-10-26 22:19:23,040  ThriftServer.java:136 - Listening for thrift clients...
                            if (parsedValues[nCell] == "Listening"
                                    && parsedValues[nCell + 3].StartsWith("clients"))
                            {
                                dataRow["Exception"] = "Node Startup";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "DseAuthenticator.java" && parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN")
                        {
                            #region DseAuthenticator.java
                            //WARN	SharedPool-Worker-1	DseAuthenticator.java					 Plain text authentication without client / server encryption is strongly discouraged
                            if (parsedValues[nCell] == "Plain"
                                    && parsedValues[nCell + 2] == "authentication"
                                    && parsedValues[nCell + 3] == "without")
                            {
                                dataRow["Exception"] = "Plain Text Authentication";
                                //dataRow["Flagged"] = (int) LogFlagStatus;
                                handled = true;
                            }
                            #endregion
                        }
                        else if ((parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "PasswordAuthenticator.java"
                                    || parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "Auth.java")
                                && parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN")
                        {
                            #region PasswordAuthenticator|Auth.java
                            //Auth.java					 Skipped default superuser setup: some nodes were not ready
                            //PasswordAuthenticator.java PasswordAuthenticator skipped default user setup: some nodes were not ready
                            if ((parsedValues[nCell] == "PasswordAuthenticator" && parsedValues[nCell + 1] == "skipped")
                                    || (parsedValues[nCell] == "Skipped" && parsedValues[nCell + 2] == "superuser"))
                            {
                                var lastOccurence = (from dr in dtCLog.AsEnumerable().TakeLast(10)
                                                     where dr.Field<string>("Item") == (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "PasswordAuthenticator.java"
                                                                                         ? "Auth.java"
                                                                                         : "PasswordAuthenticator.java")
                                                     select new { Timestamp = dr.Field<DateTime>("Timestamp") }).LastOrDefault();

                                if (lastOccurence == null
                                        || lastOccurence.Timestamp == DateTime.MinValue
                                        || lastOccurence.Timestamp.AddSeconds(1) < lineDateTime)
                                {
                                    dataRow["Exception"] = "Possible Authenticator conflict between nodes";
                                    //dataRow["Flagged"] = true;
                                }
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "AbstractSolrSecondaryIndex.java")
                        {
                            #region AbstractSolrSecondaryIndex
                            //INFO  [SolrSecondaryIndex prod_fcra.rtics_contribution index reloader.] 2016-10-12 21:52:23,011  AbstractSolrSecondaryIndex.java:1566 - Finished reindexing on keyspace prod_fcra and column family rtics_contribution
                            //INFO  [SolrSecondaryIndex prod_fcra.rtics_contribution index reloader.] 2016-10-12 21:12:02,937  AbstractSolrSecondaryIndex.java:1539 - Reindexing on keyspace prod_fcra and column family rtics_contribution
                            //INFO  [SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016-10-18 23:03:09,999  AbstractSolrSecondaryIndex.java:546 - Reindexing 1117 commit log updates for core prod_fcra.bics_contribution
                            //INFO[SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016-10-18 23:03:10,567  AbstractSolrSecondaryIndex.java:1133 - Executing hard commit on index prod_fcra.bics_contribution
                            //INFO[SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016-10-18 23:03:13,616  AbstractSolrSecondaryIndex.java:581 - Reindexed 1117 commit log updates for core prod_fcra.bics_contribution
                            //INFO[SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016-10-18 23:03:13, 618  AbstractSolrSecondaryIndex.java:584 - Truncated commit log for core prod_fcra.bics_contribution

                            if (parsedValues[nCell] == "Reindexing"
                                || (parsedValues[nCell] == "Finished" && parsedValues[nCell + 1] == "reindexing")
                                || (parsedValues[nCell] == "Executing" && parsedValues[nCell + 1] == "hard")
                                || (parsedValues[nCell] == "Truncated" && parsedValues[nCell + 1] == "commit"))
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.StatsOnly;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "Gossiper.java"
                                    && parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN")
                        {
                            #region Gossiper
                            //WARN  [GossipTasks:1] 2017-05-20 03:38:59,863  Gossiper.java:751 - Gossip stage has 1 pending tasks; skipping status check (no nodes will be marked down)

                            if (parsedValues[nCell] == "Gossip"
                                    && parsedValues[nCell + 1] == "stage"
                                    && parsedValues[nCell + 6] == "skipping")
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.Ignore;
                                break;
                            }
                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "MigrationManager.java")
                        {
                            #region MigrationManager.java (change in keyspace)
                            //INFO  [SharedPool-Worker-1] 2017-05-12 18:02:58,880  MigrationManager.java:279 - Update Keyspace 'OpsCenter' From KSMetaData{name=OpsCenter, strategyClass=NetworkTopologyStrategy, strategyOptions={us-east-dsestorage=1, us-west-2-dsestorage=1, us-west-2-2-dsestorage=1},
                            //INFO  [SharedPool-Worker-1] 2017-05-16 16:04:53,678  MigrationManager.java:249 - Create new ColumnFamily: org.apache.cassandra.config.CFMetaData@6d3c6562[cfId=ed1b46c1-3a72-11e7-9ff5-a12e162c4992,ksName=loan_servicing,cfName=bh_skillset_look_up,cfType=Standard,comparator=org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type),comment=,readRepairChance=0.0,dcLocalReadRepairChance=0.1,gcGraceSeconds=172800,defaultValidator=org.apache.cassandra.db.marshal.BytesType,keyValidator=org.apache.cassandra.db.marshal.Int32Type,minCompactionThreshold=4,maxCompactionThreshold=32,columnMetadata=[ColumnDefinition{name=workgroup, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=2, indexName=null, indexType=null}, ColumnDefinition{name=substrm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=2, indexName=null, indexType=null}, ColumnDefinition{name=end_dt_txt, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=2, indexName=null, indexType=null}, ColumnDefinition{name=skillset_name, type=org.apache.cassandra.db.marshal.UTF8Type, kind=CLUSTERING_COLUMN, componentIndex=0, indexName=null, indexType=null}, ColumnDefinition{name=id, type=org.apache.cassandra.db.marshal.Int32Type, kind=PARTITION_KEY, componentIndex=null, indexName=null, indexType=null}, ColumnDefinition{name=start_dt_txt, type=org.apache.cassandra.db.marshal.UTF8Type, kind=CLUSTERING_COLUMN, componentIndex=1, indexName=null, indexType=null}],compactionStrategyClass=class org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy,compactionStrategyOptions={},compressionParameters={sstable_compression=org.apache.cassandra.io.compress.LZ4Compressor},bloomFilterFpChance=0.01,memtableFlushPeriod=0,caching={"keys":"ALL", "rows_per_partition":"NONE"},defaultTimeToLive=0,minIndexInterval=128,maxIndexInterval=2048,speculativeRetry=99.0PERCENTILE,droppedColumns={},triggers=[],isDense=false]
                            //INFO  [SharedPool-Worker-2] 2017-05-12 15:59:26,902  MigrationManager.java:298 - Update ColumnFamily 'loan_servicing/ewfm' From org.apache.cassandra.config.CFMetaData@1988c474[cfId=017d9860-3f84-11e6-9023-2da642d6c5be,ksName=loan_servicing,cfName=ewfm,cfType=Standard,comparator=org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type),comment=,readRepairChance=0.0,dcLocalReadRepairChance=0.1,gcGraceSeconds=172800,defaultValidator=org.apache.cassandra.db.marshal.BytesType,keyValidator=org.apache.cassandra.db.marshal.TimestampType,minCompactionThreshold=4,maxCompactionThreshold=32,columnMetadata=[ColumnDefinition{name=coach_ind, type=org.apache.cassandra.db.marshal.BooleanType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=shaw_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=excl_ind, type=org.apache.cassandra.db.marshal.BooleanType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=strm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=data_dt, type=org.apache.cassandra.db.marshal.TimestampType, kind=PARTITION_KEY, componentIndex=null, indexName=null, indexType=null}, ColumnDefinition{name=site, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=shrt_nm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=etl_dt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=last_nm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=trmntn_dt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=euid, type=org.apache.cassandra.db.marshal.UTF8Type, kind=CLUSTERING_COLUMN, componentIndex=0, indexName=null, indexType=null}, ColumnDefinition{name=um_euid, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=avaya_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=empsk, type=org.apache.cassandra.db.marshal.DecimalType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=titan_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=dialr_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=remitco_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=sbstrm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=unit_mgr, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=unit_mgr_pfr, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=dm_euid, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=dept_mgr, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=frst_nm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=lob, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=lob_index, indexType=COMPOSITES}],compactionStrategyClass=class org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy,compactionStrategyOptions={},compressionParameters={sstable_compression=org.apache.cassandra.io.compress.LZ4Compressor},bloomFilterFpChance=0.01,memtableFlushPeriod=0,caching={"keys":"ALL", "rows_per_partition":"NONE"},defaultTimeToLive=0,minIndexInterval=128,maxIndexInterval=2048,speculativeRetry=99.0PERCENTILE,droppedColumns={},triggers=[],isDense=false] To org.apache.cassandra.config.CFMetaData@8b3cee1[cfId=017d9860-3f84-11e6-9023-2da642d6c5be,ksName=loan_servicing,cfName=ewfm,cfType=Standard,comparator=org.apache.cassandra.db.marshal.CompositeType(org.apache.cassandra.db.marshal.UTF8Type,org.apache.cassandra.db.marshal.UTF8Type),comment=,readRepairChance=0.0,dcLocalReadRepairChance=0.1,gcGraceSeconds=172800,defaultValidator=org.apache.cassandra.db.marshal.BytesType,keyValidator=org.apache.cassandra.db.marshal.TimestampType,minCompactionThreshold=4,maxCompactionThreshold=32,columnMetadata=[ColumnDefinition{name=shaw_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=excl_ind, type=org.apache.cassandra.db.marshal.BooleanType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=shrt_nm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=etl_dt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=avaya_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=titan_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=sbstrm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=unit_mgr, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=dept_mgr_pfr, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=lob, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=lob_index, indexType=COMPOSITES}, ColumnDefinition{name=coach_ind, type=org.apache.cassandra.db.marshal.BooleanType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=strm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=data_dt, type=org.apache.cassandra.db.marshal.TimestampType, kind=PARTITION_KEY, componentIndex=null, indexName=null, indexType=null}, ColumnDefinition{name=site, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=last_nm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=trmntn_dt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=euid, type=org.apache.cassandra.db.marshal.UTF8Type, kind=CLUSTERING_COLUMN, componentIndex=0, indexName=null, indexType=null}, ColumnDefinition{name=um_euid, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=empsk, type=org.apache.cassandra.db.marshal.DecimalType, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=dialr_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=remitco_id, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=unit_mgr_pfr, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=dm_euid, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=dept_mgr, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}, ColumnDefinition{name=frst_nm, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=1, indexName=null, indexType=null}],compactionStrategyClass=class org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy,compactionStrategyOptions={},compressionParameters={sstable_compression=org.apache.cassandra.io.compress.LZ4Compressor},bloomFilterFpChance=0.01,memtableFlushPeriod=0,caching={"keys":"ALL", "rows_per_partition":"NONE"},defaultTimeToLive=0,minIndexInterval=128,maxIndexInterval=2048,speculativeRetry=99.0PERCENTILE,droppedColumns={},triggers=[],isDense=false]
                            //INFO  [SharedPool-Worker-4] 2017-05-17 11:32:54,155  MigrationManager.java:326 - Drop Keyspace 'validation'

                            if (parsedValues[nCell] == "Update")
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Update " + parsedValues[nCell + 1];
                                handled = true;

                                if (parsedValues[nCell + 1] == "Keyspace")
                                {
                                    dataRow["Associated Item"] = RemoveQuotes(parsedValues[nCell + 2]);
                                }
                                else
                                {
                                    var splitItems = SplitTableName(RemoveQuotes(parsedValues[nCell + 2]));
                                    var ksTableName = splitItems.Item1 + '.' + splitItems.Item2;

                                    dataRow["Associated Item"] = ksTableName;
                                }
                            }
                            else if (parsedValues[nCell] == "Create")
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Created " + parsedValues[nCell + 1] + ' ' + parsedValues[nCell + 2];
                                handled = true;

                            }
                            else if (parsedValues[nCell] == "Drop")
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Dropped " + parsedValues[nCell + 1];
                                handled = true;

                                if (parsedValues[nCell + 1] == "Keyspace")
                                {
                                    dataRow["Associated Item"] = RemoveQuotes(parsedValues[nCell + 2]);
                                }
                                else
                                {
                                    var splitItems = SplitTableName(RemoveQuotes(parsedValues[nCell + 2]));
                                    var ksTableName = splitItems.Item1 + '.' + splitItems.Item2;

                                    dataRow["Associated Item"] = ksTableName;
                                }
                            }

                            #endregion
                        }
                        else if (parsedValues[ParserSettings.CLogLineFormats.ItemPos] == "ShardRouter.java")
                        {
                            #region ShardRouter.java Updated
                            //INFO  [GossipStage:1] 2017-05-12 18:03:01,432  ShardRouter.java:645 - Updating shards state due to endpoint /10.186.72.198 changing state SCHEMA=541fdec0-47c9-33fd-b79c-4a37acd21019
                            //INFO  [GossipStage:1] 2017-05-12 18:03:01,432  ShardRouter.java:645 - Updating shards state due to endpoint /10.186.72.198 being dead
                            if (parsedValues[nCell] == "Updating"
                                    && parsedValues[nCell + 1] == "shards")
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;

                                if (parsedValues.Last() == "dead")
                                {
                                    string endPoint;

                                    dataRow["Exception"] = "Updating shard state due to dead node";

                                    if (IPAddressStr(parsedValues[nCell + 6], out endPoint))
                                    {
                                        dataRow["Associated Item"] = endPoint;
                                    }
                                }
                                else
                                {
                                    dataRow["Exception"] = "Updating shard state due to change";
                                }
                                handled = true;

                                dataRow["Associated Value"] = parsedValues.Last();
                            }
                            #endregion
                        }
                        else if (dataRow["Associated Value"] == DBNull.Value
                                    && LookForIPAddress(parsedValues[nCell], ipAddress, out lineIPAddress))
                        {
                            dataRow["Associated Value"] = lineIPAddress;
                        }

                        if (!handled
                            && (parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "WARN"
                                    || parsedValues[ParserSettings.CLogLineFormats.IndicatorPos] == "ERROR")
                            && nCell > 4
                                && (parsedValues[nCell].ToLower().Contains("exception")
                                        || parsedValues[nCell].ToLower().Contains("error")
                                        || parsedValues[nCell].ToLower() == "failed")
                                && !(new char[] { '[', '(', '\'', '/', '"', '\\' }).Contains(parsedValues[nCell][0]))
                        {
                            #region exception
                            var subParsedValues = parsedValues.Skip(nCell + 1);
                            var indicatorWord = parsedValues[nCell].ToLower() == "exception"
                                                    || parsedValues[nCell].ToLower() == "error"
                                                    || parsedValues[nCell].ToLower() == "failed";

                            //look ahead for a future "real" exception or error
                            if (indicatorWord
                                    && subParsedValues.Any(item => RegExExpErrClassName.IsMatch(item)))
                            { }
                            else
                            {
                                ParseExceptions(ipAddress,
                                                    parsedValues[nCell],
                                                    dataRow,
                                                    string.Join(" ", indicatorWord
                                                                        ? parsedValues.Skip(startRange + 1)
                                                                        : subParsedValues),
                                                    null);
                                exceptionOccurred = true;
                            }

                            #endregion
                        }

                        logDesc.Append(' ');
                        logDesc.Append(parsedValues[nCell]);
                    }

                    dataRow["Description"] = logDesc.ToString().Trim();
                    dataRow["Source"] = fileName;

                    #endregion

                    if (dataRow["Flagged"] == DBNull.Value
                            || dataRow.Field<int>("Flagged") != (int)LogFlagStatus.Ignore)
                    {
                        AddRowToLogDataTable(dtCLog, dataRow, isDebugLogFile);
                        ++nbrRows;
                    }
                    lastRow = dataRow;
                }
            }

            if (!minmaxDate.IsEmpty())
            {
                //if (!isDebugLogFile)
                //{
                    lock (LogCassandraMaxMinTimestamp)
                    {
                        LogCassandraMaxMinTimestamp.SetMinMax(minmaxDate.Min);
                        LogCassandraMaxMinTimestamp.SetMinMax(minmaxDate.Max);
                    }
               // }

                LogCassandraNodeMaxMinTimestamps.AddOrUpdate(ipAddress,
                                                                strAddress => new List<LogCassandraNodeMaxMinTimestamp>()
                                                                                { new LogCassandraNodeMaxMinTimestamp(minmaxDate.Max, minmaxDate.Min, isDebugLogFile) },
                                                                (strAddress, dtRanges) => { lock (dtRanges) { dtRanges.Add(new LogCassandraNodeMaxMinTimestamp(minmaxDate, isDebugLogFile)); } return dtRanges; });
            }

            return nbrRows;
        }

        static void ParseExceptions(string ipAddress,
                                        string exceptionClass,
                                        DataRow dataRow,
                                        string remindingLine,
                                        Action<string, bool, DataRow> additionalUpdates,
                                        bool checkLastException = true)
        {
            if (exceptionClass.StartsWith("error...") && exceptionClass.Length > 8)
            {
                exceptionClass = exceptionClass.Substring(9);
            }
            else if (exceptionClass.StartsWith("null") && exceptionClass.Length > 4)
            {
                exceptionClass = exceptionClass.Substring(5);
            }

            var lastException = dataRow["Exception"] as string;
            var exception = exceptionClass.Trim(new char[] { ' ', '-', '.', ':', ';' });
            var exceptionDesc = remindingLine?.Trim(new char[] { ' ', '-', '.', ':', ';' });
            bool extendedExceptionDesc = false;
            var exceptionDescSplits = exceptionDesc == null ? new string[0] : RegExExceptionDesc.Split(exceptionDesc);

            if (checkLastException
                    && lastException == null
                    && dataRow["Exception Description"] == DBNull.Value)
            {
                if (dataRow["Task"] is string && ((string)dataRow["Task"]).StartsWith("SolrSecondaryIndex "))
                {
                    var taskItems = RegExSolrSecondaryIndex.Split((string)dataRow["Task"]);

                    if (taskItems.Length > 1)
                    {
                        var splitItems = SplitTableName(taskItems[1]);

                        UpdateRowColumn(dataRow,
                                        "Associated Item",
                                        dataRow["Associated Item"] as string,
                                        splitItems.Item1 + '.' + splitItems.Item2,
                                        "->");

                        if (taskItems.Length > 2)
                        {
                            lastException = "SolrSecondaryIndex." + taskItems[2];
                        }
                        else
                        {
                            lastException = "SolrSecondaryIndex";
                        }
                    }
                }

                if (lastException == null)
                {
                    ParseExceptions(ipAddress, (string)dataRow["Indicator"], dataRow, dataRow["Description"] as string, additionalUpdates, false);
                    lastException = dataRow["Exception"] as string;
                }
            }

            dataRow.BeginEdit();

            if (exception == "java.lang.OutOfMemoryError")
            {
                //Flag it so that it can be included in the stats
                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
            }

            UpdateRowColumn(dataRow, "Exception Description", dataRow["Exception Description"] as string, exception + "(" + exceptionDesc + ")");

            if (exceptionDescSplits.Length == 0)
            {
                if (string.IsNullOrEmpty(exceptionDesc))
                {
                    UpdateRowColumn(dataRow,
                                    "Exception",
                                    lastException,
                                    exception);
                }
                else
                {
                    var clMatch = RegExCLogCL.Match(exceptionDesc);
                    var toMatch = RegExCLogTO.Match(exceptionDesc);

                    if (toMatch.Success)
                    {
                        dataRow["Associated Value"] = int.Parse(toMatch.Groups[1].Value);
                        UpdateRowColumn(dataRow, "Exception", lastException, exception + "(No response|Operation timeout)");
                    }
                    else if (clMatch.Success)
                    {
                        dataRow["Associated Value"] = clMatch.Groups[1].Value;
                        UpdateRowColumn(dataRow, "Exception", lastException, exception + "(Cannot achieve consistency level)");
                    }
                    else
                    {
                        UpdateRowColumn(dataRow,
                                            "Exception",
                                            lastException,
                                            exception + "(" + exceptionDesc + ")");
                    }
                }
                extendedExceptionDesc = true;
            }
            else
            {
                string currentExceptionDesc = string.Empty;

                for (int nIndex = 0; nIndex < exceptionDescSplits.Length; ++nIndex)
                {
                    if (exceptionDescSplits[nIndex] == null)
                    {
                        continue;
                    }

                    exceptionDescSplits[nIndex] = exceptionDescSplits[nIndex].Trim();

                    if (exceptionDescSplits[nIndex] == string.Empty)
                    {
                        continue;
                    }

                    if (RegExExpErrClassName.IsMatch(exceptionDescSplits[nIndex]))
                    {
                        continue;
                    }

                    if (exceptionDescSplits[nIndex][0] == '/')
                    {
                        string locIpAddress;

                        if (IPAddressStr(exceptionDescSplits[nIndex][0] == '.'
                                                ? exceptionDescSplits[nIndex].Substring(1)
                                                : exceptionDescSplits[nIndex],
                                            out locIpAddress))
                        {
                            UpdateRowColumn(dataRow,
                                               "Associated Item",
                                               dataRow["Associated Item"] as string,
                                               locIpAddress,
                                               "->");
                        }
                        else
                        {
                            UpdateRowColumn(dataRow,
                                                "Associated Item",
                                                dataRow["Associated Item"] as string,
                                                exceptionDescSplits[nIndex],
                                                "->");
                        }

                        //Unexpected exception during request; channel = [id: 0xa6a28fb0, / 10.14.50.24:44796 => / 10.14.50.24:9042]
                        //Remove the first IPAdress's protocol
                        if (exceptionDescSplits.Skip(nIndex + 1).Select(i => i == null ? string.Empty : i.Trim()).Any(i => i == ">" || i == "=>"))
                        {
                            var protocolPos = exceptionDescSplits[nIndex].IndexOf(':');

                            if (protocolPos > 0)
                            {
                                exceptionDescSplits[nIndex] = exceptionDescSplits[nIndex].Substring(0, protocolPos) + ":####";
                            }
                        }
                    }
                    if (lastException == null || !lastException.Contains(exceptionDescSplits[nIndex]))
                    {
                        currentExceptionDesc += " " + exceptionDescSplits[nIndex];
                    }
                }

                if (!string.IsNullOrEmpty(currentExceptionDesc))
                {
                    UpdateRowColumn(dataRow,
                                        "Exception",
                                        lastException,
                                        exception + "(" + currentExceptionDesc.TrimStart() + ")");
                    extendedExceptionDesc = true;
                }
            }

            if (!extendedExceptionDesc)
            {
                UpdateRowColumn(dataRow,
                                "Exception",
                                lastException,
                                exception);
                extendedExceptionDesc = true;
            }

            additionalUpdates?.Invoke(exception, extendedExceptionDesc, dataRow);

            if (dataRow["Flagged"] == DBNull.Value || (int)dataRow["Flagged"] == 0)
            {
                dataRow["Flagged"] = (int)LogFlagStatus.Exception;
            }

            dataRow.EndEdit();
        }

        static string UpdateRowColumn(DataRow dataRow, string columnName, string currentValue, string appendValue, string delimitor = "=>")
        {
            if (!string.IsNullOrEmpty(currentValue) && currentValue.Contains(appendValue))
            {
                return currentValue;
            }

            var newValue = string.IsNullOrEmpty(currentValue) ? appendValue : currentValue + delimitor + appendValue;
            dataRow[columnName] = newValue;

            return newValue;
        }

        static Regex RegExSummaryLogExceptionNodes = new Regex(@"(.+?)\=\>",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExSummaryLogExceptionName = new Regex(@"(.+?)\(",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static Regex RegExSummaryLogIPAdress = new Regex(@"\/?\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\:?\d{1,5}(?:\->)?",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExSummaryLogKSTblName = new Regex(@"([a-z0-9-_$%+=@!?<>^*&]+)(?:\.)([a-z0-9-_$%+=@!?<>^*&]+)",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static void ParseCassandraLogIntoSummaryDataTable(DataTable dtroCLog,
                                                            DataTable dtCSummaryLog,
                                                            DataTable dtCExceptionSummaryLog,
                                                            DateTimeRange maxminLogDate,
                                                            string[] logAggregateAdditionalTaskExceptionItems,
                                                            string[] ignoreTaskExceptionItems,
                                                            IEnumerable<Tuple<DateTime, TimeSpan>> bucketFromAggregatePeriods)
        {
            if (dtCSummaryLog.Columns.Count == 0)
            {
                dtCSummaryLog.Columns.Add("Timestamp Period", typeof(DateTime)); //A
                dtCSummaryLog.Columns.Add("Aggregation Period", typeof(TimeSpan)); //B
                dtCSummaryLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true; //C
                dtCSummaryLog.Columns.Add("Node IPAddress", typeof(string)).AllowDBNull = true; //D
                dtCSummaryLog.Columns.Add("Type", typeof(string)).AllowDBNull = true; //E
                dtCSummaryLog.Columns.Add("Key", typeof(string)).AllowDBNull = true; //F
                dtCSummaryLog.Columns.Add("Path", typeof(string)).AllowDBNull = true; //G
                dtCSummaryLog.Columns.Add("Associated Item Type", typeof(string)).AllowDBNull = true; //H
                dtCSummaryLog.Columns.Add("Associated Item", typeof(string)).AllowDBNull = true; //I
                dtCSummaryLog.Columns.Add("Last Occurrence", typeof(DateTime)).AllowDBNull = true; //J
                dtCSummaryLog.Columns.Add("Occurrences", typeof(int)); //K
                dtCSummaryLog.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true; //L
                dtCSummaryLog.Columns.Add("KeySpace", typeof(string)).AllowDBNull = true; //M
                dtCSummaryLog.Columns.Add("Table", typeof(string)).AllowDBNull = true; //N

                dtCSummaryLog.DefaultView.Sort = "[Timestamp Period] DESC, [Data Center], [Key], [Path]";
            }

            CreateCassandraLogDataTable(dtCExceptionSummaryLog, true);

            if (dtroCLog.Rows.Count > 0)
            {
                var segments = new List<Tuple<DateTime, DateTime, TimeSpan, List<CLogSummaryInfo>>>();

                Program.ConsoleParsingLog.Increment("Summary Segment Generation...");

                for (int nIndex = 0; nIndex < bucketFromAggregatePeriods.Count(); ++nIndex)
                {
                    segments.Add(new Tuple<DateTime, DateTime, TimeSpan, List<CLogSummaryInfo>>(bucketFromAggregatePeriods.ElementAt(nIndex).Item1,
                                                                                                    bucketFromAggregatePeriods.ElementAtOrDefault(nIndex + 1) == null
                                                                                                        ? (maxminLogDate.Min == DateTime.MinValue ? DateTime.MinValue : (maxminLogDate.Min - new TimeSpan(1)))
                                                                                                        : bucketFromAggregatePeriods.ElementAt(nIndex + 1).Item1,
                                                                                                    bucketFromAggregatePeriods.ElementAt(nIndex).Item2,
                                                                                                    new List<CLogSummaryInfo>()));
                }

                Program.ConsoleParsingLog.TaskEnd("Summary Segment Generation...");

                Logger.Instance.InfoFormat("Summary Log Ranges: {{{0}}} Nbr Log Rows: {1:###,###,##0}", string.Join("; ", segments.Select(element => string.Format("#{0}# >= [Timestamp] and #{1}# < [Timestamp], Interval{{{2}}}", element.Item1, element.Item2, element.Item3))), dtroCLog.Rows.Count);

                var dtroCLogRows = (from dr in dtroCLog.AsEnumerable()
                                    let timeStamp = dr.Field<DateTime>("Timestamp")
                                    let flagged = dr.Field<int?>("Flagged")
                                    let indicator = dr.Field<string>("Indicator")
                                    where (flagged.HasValue && (flagged.Value == (int)LogFlagStatus.Exception || flagged.Value == (int)LogFlagStatus.Stats))
                                                     || !dr.IsNull("Exception")
                                                     || indicator == "ERROR"
                                                     || indicator == "WARN"
                                    orderby timeStamp descending
                                    select new
                                    {
                                        Timestamp = timeStamp,
                                        Indicator = indicator,
                                        TaskItem = dr.Field<string>("Task"),
                                        Item = dr.Field<string>("Item"),
                                        Exception = dr.Field<string>("Exception"),
                                        Flagged = flagged.HasValue ? flagged.Value != 0 : false,
                                        DataCenter = dr.Field<string>("Data Center"),
                                        IpAdress = dr.Field<string>("Node IPAddress"),
                                        AssocItem = dr.Field<string>("Associated Item"),
                                        DataRowArray = dr.ItemArray
                                    }).ToArray();

                Parallel.ForEach(segments, element =>
                //foreach (var element in segments)
                {
                    Program.ConsoleParsingLog.Increment(string.Format("Summary Parallel Segment Processing {0} to {1}", element.Item1, element.Item2));

                    var segmentView = from dr in dtroCLogRows
                                      where element.Item1 >= dr.Timestamp && element.Item2 < dr.Timestamp
                                      orderby dr.Timestamp descending
                                      select dr;

                    var startPeriod = element.Item1;
                    var endPeriod = startPeriod - element.Item3;

                    foreach (var dataView in segmentView)
                    {
                        if (dataView.Timestamp < endPeriod)
                        {
                            startPeriod = endPeriod;
                            endPeriod = startPeriod - element.Item3;

                            if (dataView.Timestamp < endPeriod)
                            {
                                var newBeginPeriod = (dataView.Timestamp).RoundUp(element.Item3);

                                if (element.Item4.Count > 0)
                                {
                                    while (newBeginPeriod < startPeriod)
                                    {
                                        element.Item4.Add(new CLogSummaryInfo(startPeriod, element.Item3, null, null, null, null, null));
                                        startPeriod = startPeriod - element.Item3;
                                    }
                                }

                                startPeriod = newBeginPeriod;
                                endPeriod = startPeriod - element.Item3;
                            }
                        }

                        if (ignoreTaskExceptionItems.Contains(dataView.TaskItem)
                            || ignoreTaskExceptionItems.Contains(dataView.Item)
                            || ignoreTaskExceptionItems.Contains(dataView.Exception))
                        {
                            continue;
                        }

                        if (!string.IsNullOrEmpty(dataView.Exception))
                        {
                            var exceptionName = DetermineExceptionNameFromPath(dataView.Exception);
                            var exceptionPath = dataView.Exception;

                            if (exceptionPath == exceptionName)
                            {
                                if (string.IsNullOrEmpty(dataView.AssocItem))
                                {
                                    exceptionPath = null;
                                }
                                else
                                {
                                    exceptionPath += string.Format("({0})", dataView.AssocItem);
                                }
                            }

                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod,
                                                                                "Exception",
                                                                                exceptionName,
                                                                                exceptionPath,
                                                                                dataView.DataCenter,
                                                                                dataView.IpAdress));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod,
                                                                    element.Item3,
                                                                    "Exception",
                                                                    exceptionName,
                                                                    exceptionPath,
                                                                    dataView.DataCenter,
                                                                    dataView.IpAdress);
                                element.Item4.Add(summaryInfo);
                            }

                            summaryInfo.Increment(dataView.Timestamp, dataView.AssocItem, dataView.DataRowArray);
                        }
                        else if (dataView.Flagged)
                        {
                            var item = dataView.Item;
                            var itemNameSplit = RegExSummaryLogExceptionName.Split(item);
                            var itemName = itemNameSplit.Length < 2 ? item : itemNameSplit[1];

                            if (string.IsNullOrEmpty(dataView.AssocItem))
                            {
                                item = null;
                            }
                            else
                            {
                                item += string.Format("({0})", dataView.AssocItem);
                            }

                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod,
                                                                                "Flagged",
                                                                                itemName,
                                                                                item,
                                                                                dataView.DataCenter,
                                                                                dataView.IpAdress));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod,
                                                                    element.Item3,
                                                                    "Flagged",
                                                                    itemName,
                                                                    item,
                                                                    dataView.DataCenter,
                                                                    dataView.IpAdress);
                                element.Item4.Add(summaryInfo);
                            }

                            summaryInfo.Increment(dataView.Timestamp, dataView.AssocItem, dataView.DataRowArray);
                        }
                        else
                        {
                            string itemType;
                            string itemPath = null;
                            string itemName;

                            if (logAggregateAdditionalTaskExceptionItems.Contains(dataView.TaskItem) && dataView.AssocItem != null)
                            {
                                itemType = "Task";
                                itemName = dataView.TaskItem;
                                itemPath = string.Format("{0}({1})", dataView.TaskItem, dataView.AssocItem);
                            }
                            else if (logAggregateAdditionalTaskExceptionItems.Contains(dataView.Item) && dataView.AssocItem != null)
                            {
                                itemType = "Item";
                                itemName = dataView.Item;
                                itemPath = string.Format("{0}({1})", dataView.Item, dataView.AssocItem);
                            }
                            else
                            {
                                itemType = "Indicator";
                                itemName = dataView.Indicator;
                            }

                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, itemType, itemName, itemPath, dataView.DataCenter, dataView.IpAdress));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod,
                                                                    element.Item3,
                                                                    itemType,
                                                                    itemName,
                                                                    itemPath,
                                                                    dataView.DataCenter,
                                                                    dataView.IpAdress);
                                element.Item4.Add(summaryInfo);
                            }

                            summaryInfo.Increment(dataView.Timestamp, dataView.AssocItem, dataView.DataRowArray);
                        }

                    }

                    Program.ConsoleParsingLog.TaskEnd(string.Format("Summary Parallel Segment Processing {0} to {1}", element.Item1, element.Item2));
                });//foreach

                var sortedSegments = from segment in segments
                                     orderby segment.Item1 descending
                                     select segment;

                Program.ConsoleParsingNonLog.Increment("Summary Generating DataTable...");

                var summaryDataTableTask = Task.Factory.StartNew(() =>
                    {
                        DataRow dataSummaryRow;

                        foreach (var element in segments)
                        {
                            if (element.Item4.Count == 0)
                            {
                                dataSummaryRow = dtCSummaryLog.NewRow();

                                dataSummaryRow["Data Center"] = null;
                                dataSummaryRow["Node IPAddress"] = null;

                                dataSummaryRow["Timestamp Period"] = element.Item1;
                                dataSummaryRow["Aggregation Period"] = element.Item3;
                                dataSummaryRow["Occurrences"] = 0;

                                dtCSummaryLog.Rows.Add(dataSummaryRow);
                            }
                            else
                            {
                                foreach (var item in element.Item4)
                                {
                                    dataSummaryRow = dtCSummaryLog.NewRow();

                                    dataSummaryRow["Data Center"] = item.DataCenter;
                                    dataSummaryRow["Node IPAddress"] = item.IPAddress;

                                    dataSummaryRow["Timestamp Period"] = item.Period;
                                    dataSummaryRow["Aggregation Period"] = item.PeriodSpan;
                                    dataSummaryRow["Type"] = item.ItemType;
                                    dataSummaryRow["Key"] = item.ItemKey;
                                    dataSummaryRow["Path"] = item.ItemPath;
                                    dataSummaryRow["Last Occurrence"] = item.MaxTimeStamp.HasValue ? (object)item.MaxTimeStamp.Value : DBNull.Value;
                                    dataSummaryRow["Occurrences"] = item.AggregationCount;
                                    dataSummaryRow["Reconciliation Reference"] = item.GroupIndicator;

                                    if (item.AssociatedItems.Count > 0)
                                    {
                                        dataSummaryRow["Associated Item"] = string.Join("; ", item.AssociatedItems);

                                        string assocType = "Other";

                                        foreach (var assocItem in item.AssociatedItems)
                                        {
                                            if (RegExSummaryLogIPAdress.IsMatch(assocItem))
                                            {
                                                assocType = "IPAddress";
                                            }
                                            else
                                            {
                                                var pos = assocItem.IndexOf("->");
                                                var splits = RegExSummaryLogKSTblName.Split(pos < 0 ? assocItem : assocItem.Substring(0, pos));

                                                if (splits.Length == 4 && splits[3] == string.Empty)
                                                {
                                                    dataSummaryRow["KeySpace"] = splits[1].TrimStart();
                                                    dataSummaryRow["Table"] = splits[2].TrimEnd();
                                                    assocType = "Table";
                                                    break;
                                                }
                                            }
                                        }
                                        dataSummaryRow["Associated Item Type"] = assocType;
                                    }

                                    dtCSummaryLog.Rows.Add(dataSummaryRow);
                                }
                            }
                        }
                    },
                    TaskCreationOptions.LongRunning);

                var summaryExceptionDataTableTask = Task.Factory.StartNew(() =>
                        {
                            DataRow dataSummaryRow;

                            foreach (var element in segments)
                            {
                                foreach (var item in element.Item4)
                                {
                                    foreach (var drArray in item.AssociatedDataArrays)
                                    {
                                        if (drArray.Length > 0)
                                        {
                                            dataSummaryRow = dtCExceptionSummaryLog.NewRow();
                                            var itemArray = dataSummaryRow.ItemArray;

                                            drArray.CopyTo(ref itemArray, 1);
                                            dataSummaryRow.ItemArray = itemArray;
                                            dataSummaryRow["Reconciliation Reference"] = item.GroupIndicator;

                                            dtCExceptionSummaryLog.Rows.Add(dataSummaryRow);
                                        }
                                    }
                                }
                            }
                        },
                    TaskCreationOptions.LongRunning);

                summaryDataTableTask.Wait();
                summaryExceptionDataTableTask.Wait();

                Program.ConsoleParsingNonLog.TaskEnd("Summary Generating DataTable...");
            }

            Logger.Instance.InfoFormat("Summary Log Nbr Rows: {0:###,###,##0}", dtCSummaryLog.Rows.Count);
        }

        static string DetermineExceptionNameFromPath(string exceptionPath)
        {
            return DetermineExceptionNameFromPath(exceptionPath, RegExSummaryLogExceptionNodes.Split(exceptionPath));
        }

        static string DetermineExceptionNameFromPath(string exceptionPath, string[] exceptionPathNodes, int posFromEnd = 1)
        {
            bool defaultNodes = false;

            var keyNode = (defaultNodes = exceptionPathNodes.Length == 0 || posFromEnd > exceptionPathNodes.Length)
                                ? exceptionPath
                                : exceptionPathNodes[exceptionPathNodes.Length - posFromEnd];

            if (keyNode.Length > 5
                    && keyNode[0] == '/'
                    && (exceptionPathNodes.Length - (posFromEnd + 1)) >= 0)
            {
                keyNode = exceptionPathNodes[exceptionPathNodes.Length - (posFromEnd + 1)];
            }

            var exceptionNameSplit = RegExSummaryLogExceptionName.Split(keyNode);
            var exceptionName = exceptionNameSplit.Length < 2 ? keyNode : exceptionNameSplit[1];

            if (defaultNodes)
            {
                return exceptionName;
            }

            if (ParserSettings.SummaryIgnoreExceptions.Contains(exceptionName.ToLower()))
            {
                return DetermineExceptionNameFromPath(exceptionPath, exceptionPathNodes, posFromEnd + 1);
            }

            return exceptionName;
        }

        //GCInspector.java:258 - G1 Young Generation GC in 691ms.  G1 Eden Space: 4,682,940,416 -> 0; G1 Old Gen: 2,211,450,256 -> 2,797,603,280; G1 Survivor Space: 220,200,960 -> 614,465,536;
        //GCInspector.java:258 - G1 Young Generation GC in 277ms. G1 Eden Space: 4047503360 -> 0; G1 Old Gen: 2855274656 -> 2855274648;
        static Regex RegExG1Line = new Regex(@"\s*G1.+in\s+([0-9,]+)(?:.*Eden Space:\s*([0-9,.]+)\s*->\s*([0-9,.]+))?(?:.*Old Gen:\s*([0-9,.]+)\s*->\s*([0-9,.]+))?(?:.*Survivor Space:\s*([0-9,.]+)\s*->\s*([0-9,.]+).*)?.*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //GCInspector.java (line 116) GC for ParNew: 394 ms for 1 collections, 13571498424 used; max is 25340346368
        static Regex RegExGCLine = new Regex(@"\s*GC.+ParNew:\s+([0-9,]+)",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //ConcurrentMarkSweep GC in 363ms. CMS Old Gen: 5688178056 -> 454696416; Par Eden Space: 3754560 -> 208755688;
        //ConcurrentMarkSweep GC in 2083ms. CMS Old Gen: 8524829104 -> 8531031448; CMS Perm Gen: 68555136 -> 68555392; Par Eden Space: 1139508352 -> 47047616; Par Survivor Space: 35139688 -> 45900968
        static Regex RegExGCMSLine = new Regex(@"\s*ConcurrentMarkSweep.+in\s+([0-9,]+)(?:.*Old Gen:\s*([0-9,.]+)\s*->\s*([0-9,.]+))?(?:.*Eden Space:\s*([0-9,.]+)\s*->\s*([0-9,.]+))?.*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExPoolLine = new Regex(@"\s*(\w+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCacheLine = new Regex(@"\s*(\w+)\s+(\d+)\s+(\d+)\s+(\w+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExTblLine = new Regex(@"\s*(.+)\s+(\d+)\s*,\s*(\d+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExPool2Line = new Regex(@"\s*(\w+)\s+(\w+/\w+|\d+)\s+(\w+/\w+|\d+).*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //INFO  [CompactionExecutor:4657] 2016-06-12 06:26:25,534  CompactionTask.java:274 - Compacted 4 sstables to [/data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/system-size_estimates-ka-11348,]. 2,270,620 bytes to 566,478 (~24% of original) in 342ms = 1.579636MB/s. 40 total partitions merged to 10. Partition merge counts were {4:10, }
        //DEBUG	CompactionExecutor	CompactionTask.java	 Compacted (aa83aec0-6a0b-11e6-923c-7d02e3681807) 4 sstables to [/var/lib/cassandra/data/system/compaction_history-b4dbb7b4dc493fb5b3bfce6e434832ca/mb-217-big,] to level=0. 64,352 bytes to 62,408 (~96% of original) in 1,028ms = 0.057896MB/s. 0 total partitions merged to 1,428. Partition merge counts were {1:1456, }
        static Regex RegExCompactionTaskCompletedLine = new Regex(@"Compacted\s+(?:\(.+\)\s+)?(\d+)\s+sstables.+\[\s*(.+)\,\s*\]\s*(?:to level.*\s*)?\.\s+(.+)\s+bytes to (.+)\s+\(\s*(.+)\s*\%.+in\s+(.+)\s*ms\s+=\s+(.+)\s*MB/s.\s+(\d+).+merged to\s+(\d+).+were\s+\{\s*(.+)\,\s*\}",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //[repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] new session: will sync c1249170.ews.int/10.12.49.11, /10.12.51.29 on range (7698152963051967815,7704157762555128476] for OpsCenter.[rollups7200, settings, events, rollups60, rollups86400, rollups300, events_timeline, backup_reports, bestpractice_results, pdps]
        static Regex RegExRepairNewSessionLine = new Regex(@"\s*\[repair\s+#(.+)\]\s+(.+)\s+session:.+on range\s+\[?\(([0-9-]+).*\,([0-9-]+)\]\]?\s+for\s+(.+)\.\[.+\]",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //starting user-requested repair of range [(-1537228672809129313,-1531223873305968652]] for keyspace prod_fcra and column families [ifps_inquiry, ifps_contribution, rtics_contribution, rtics_inquiry, bics_contribution, avlo_inquiry, bics_inquiry]
        static Regex RegExRepairUserRequest = new Regex(@".*starting\s+user-requested\s+repair.+range\s+\[?\(\s*([0-9-]+)\s*\,\s*([0-9-]+)\s*\]\]?\s+.+keyspace\s+(.+)\s+and.+",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //Starting repair command #9, repairing 1 ranges for keyspace prod_fcra (parallelism=PARALLEL, full=true)
        static Regex RegExRepairUserRequest1 = new Regex(@".*Starting\s+repair\s+command\s.+keyspace\s+(.+)\s+\((.+)\)",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //[repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] session completed successfully
        static Regex RegExRepairEndSessionLine = new Regex(@"\s*\[repair\s+#(.+)\]\s+session\s+(.+)\s+(.+)",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //[streaming task #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] Performing streaming repair of 1 ranges with /10.12.51.29
        static Regex RegExRepairNbrRangesLine = new Regex(@"\[.+\s+#(.+)\]\s+Performing streaming repair.+\s+(\d+)\s+ranges.+\/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //[repair #87a95910-bf45-11e6-a2a7-f19e9c4c25c4] Received merkle tree for node_slow_log from /10.14.149.8
        static Regex RegExRepairReceivedLine = new Regex(@"\s*\[repair\s+#(.+)\]\s+(.+)\s+merkle tree for\s+.+from.+\/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //Reindexing on keyspace account_owner and column family ew_account_owners
        //Finished reindexing on keyspace prod_fcra and column family rtics_contribution
        static Regex RegExSolrReIndex = new Regex(@".*Reindexing.+keyspace\s+(.+)\s+.+column\s+family\s+(.+)",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //Reindexing 1117 commit log updates for core prod_fcra.bics_contribution
        static Regex RegExSolrReIndex1 = new Regex(@".*Reindexing\s+(\d+)\s+commit.+core\s+(.+)\.(.+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //Executing hard commit on index prod_fcra.bics_contribution
        static Regex RegExSolrReIndex2 = new Regex(@".*Executing hard commit on index\s+(.+)\.(.+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        //Truncated commit log for core prod_fcra.bics_contribution
        static Regex RegExSolrReIndex3 = new Regex(@".*Truncated commit log for core\s+(.+)\.(.+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static readonly Regex RegExSolrHardCommitInternalTime = new Regex(@"^commitInternal\w+\s+(?:duration\s*=\s*(\d+)\s*ms\s+)*startTime\s*=\s*(\d+)$",
                                                                        RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public interface ILogInfo
        {
            string DataCenter { get; }
            string IPAddress { get; }
            string Keyspace { get; }
            string Table { get; }
            DateTime StartTime { get; }
            DateTime CompletionTime { get; }
            DateTime LogTimestamp { get; }
            int Duration { get; }
            object GroupIndicator { get; }
            string Type { get; }
        }

        public interface ILogRateInfo : ILogInfo
        {
            decimal IORate { get; }
        }

        public interface ILogSessionInfo
        {
            string SessionPath { get; }
            string Session { get; set; }
        }

        public abstract class LogSessionInfo : ILogSessionInfo
        {
            public string SessionPath { get; private set; }
            public string Session { get; set; }

            public string SetSession()
            {
                return this.Session = (Guid.NewGuid()).ToString();
            }

            public string SetSession(Guid session)
            {
                return this.Session = session.ToString();
            }

            public string SetSession(string session)
            {
                return this.Session = session;
            }

            public string GenerateSessionPath<T>(IEnumerable<T> sessionCollection)
                where T : ILogSessionInfo
            {
                var sessionValues = sessionCollection?.Select(i => string.IsNullOrEmpty(i.Session) ? string.Empty : i.Session);

                return this.SessionPath = sessionValues == null ? null : string.Join("=>", sessionValues);
            }
        }

        public class GCLogInfo : ILogInfo
        {
            #region ILogInfo
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; }
            public string Table { get { return null; } }
            public DateTime StartTime
            {
                get { return this.LogTimestamp.Subtract(TimeSpan.FromMilliseconds(this.Duration)); }
            }
            public DateTime CompletionTime { get { return this.LogTimestamp; } }
            public DateTime LogTimestamp { get; set; }
            public int Duration { get; set; }
            public object GroupIndicator { get; set; }
            public string Type { get { return "GC"; } }

            #endregion

            public decimal GCEdenFrom;
            public decimal GCEdenTo;
            public decimal GCSurvivorFrom;
            public decimal GCSurvivorTo;
            public decimal GCOldFrom;
            public decimal GCOldTo;
        }

        public interface ICompactionLogInfo : ILogRateInfo
        {
            int SSTables { get; }
        }

        public class CompactionLogInfo : ICompactionLogInfo
        {
            #region ICompactionLogInto Members
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public int SSTables { get; set; }
            public object GroupIndicator { get; set; }
            public DateTime LogTimestamp { get; set; }
            public DateTime StartTime { get { return this.LogTimestamp.Subtract(TimeSpan.FromMilliseconds(this.Duration)); } }
            public DateTime CompletionTime { get { return this.LogTimestamp; } }
            public int Duration { get; set; }
            public decimal IORate { get; set; }
            public string Type { get { return "Compaction"; } }
            #endregion

            public decimal OldSize;
            public decimal NewSize;
            public string PartitionsMerged;
            public string MergeCounts;

        }

        public class AntiCompactionLogInfo : ICompactionLogInfo
        {
            #region ICompactionLogInto Members
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public int SSTables { get; set; }
            public object GroupIndicator { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime LogTimestamp { get { return this.StartTime; } }
            public DateTime CompletionTime { get; set; }
            public decimal IORate
            {
                get
                {
                    if (this.OldSize.HasValue && this.NewSize.HasValue)
                    {
                        return (this.OldSize.Value + this.NewSize.Value) / ((decimal)this.Duration / 1000M);
                    }

                    return 0;
                }
            }
            public int Duration
            {
                get
                {
                    return this.CompletionTime == DateTime.MinValue ? 0 : (int)(this.CompletionTime - this.StartTime).TotalMilliseconds;
                }
            }
            public string Type { get { return "AntiCompaction"; } }
            #endregion

            public List<Tuple<string, string>> TokenRanges = null;

            public void AddRange(string startRange, string endRange)
            {
                startRange = startRange?.Trim();
                endRange = endRange?.Trim();

                if (string.IsNullOrEmpty(startRange) || string.IsNullOrEmpty(endRange))
                {
                    return;
                }

                if (this.TokenRanges == null)
                {
                    this.TokenRanges = new List<Tuple<string, string>>() { new Tuple<string, string>(startRange, endRange) };
                    return;
                }

                if (!this.TokenRanges.Exists(i => i.Item1 == startRange && i.Item2 == endRange))
                {
                    this.TokenRanges.Add(new Tuple<string, string>(startRange, endRange));
                }
            }
            public bool Aborted { get; set; }

            public decimal? OldSize;
            public decimal? NewSize;

            public decimal? CompactedStorage
            {
                get
                {
                    if (this.OldSize.HasValue && this.NewSize.HasValue)
                    {
                        return Math.Abs(this.OldSize.Value - this.NewSize.Value);
                    }
                    return null;
                }
            }
        }

        public class SolrHardCommitLogInfo : ILogInfo
        {
            #region ILogInfo Members

            public DateTime StartTime { get; set; }
            public DateTime CompletionTime { get; set; }
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public object GroupIndicator { get; set; }
            public int Duration { get { return (int)(this.CompletionTime - this.StartTime).TotalMilliseconds; } }
            public DateTime LogTimestamp { get { return this.StartTime; } }
            public string Type { get { return "SolrHardCommit"; } }

            #endregion

            public int TaskId;
            public long InternalStartTime;

            public IEnumerable<Tuple<string, string>> SolrIndexes = null;
        }

        public class SolrReIndexingLogInfo : ILogInfo
        {
            #region ILogInfo Members

            public DateTime StartTime { get; set; }
            public DateTime CompletionTime { get; set; }
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public object GroupIndicator { get; set; }
            public int Duration { get { return (int)(this.CompletionTime - this.StartTime).TotalMilliseconds; } }
            public DateTime LogTimestamp { get { return this.StartTime; } }
            public string Type { get { return "SolrReIndexing"; } }

            #endregion

            public int NbrUpdates;
        }


        public class MemTableFlushOccurrenceLogInfo : LogSessionInfo, ILogRateInfo
        {
            #region ILogRateInfo
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime CompletionTime { get; set; }
            public DateTime LogTimestamp { get { return this.StartTime; } }
            public object GroupIndicator { get; set; }
            public string Type { get { return "MemTableFlushOccurrence"; } }
            public int Duration { get { return (int)(this.CompletionTime - this.StartTime).TotalMilliseconds; } }
            public decimal IORate { get { return this.Duration == 0 ? 0 : this.FlushedStorage / ((decimal)this.Duration / 1000M); } }

            #endregion

            public MemTableFlushOccurrenceLogInfo(Guid session)
            {
                this.SetSession(session);
            }

            public MemTableFlushOccurrenceLogInfo(string session)
            {
                this.SetSession(session);
            }

            public MemTableFlushOccurrenceLogInfo()
            {
                this.SetSession();
            }

            public int NbrWriteOPS;
            //public decimal PreFlushOnHeapMemory;
            //public decimal PreFlushOffHeapMemory;
            public decimal FlushedStorage;
            public string SSTableFilePath;
            public int TaskId;
        }

        public class MemTableFlushLogInfo : LogSessionInfo, ILogRateInfo
        {
            #region ILogRateInfo
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public DateTime StartTime { get { return this.EnqueuingStart; } }
            public DateTime CompletionTime { get { return this.Duration == 0 ? this.StartTime : this.Occurrences.Max(i => i.CompletionTime); } }
            public DateTime LogTimestamp { get { return this.StartTime; } }
            public object GroupIndicator { get; set; }
            private string _type;
            public string Type
            {
                get { return string.IsNullOrEmpty(this._type) ? this._type : "MemTable Flush (" + this._type + ")"; }
                set { this._type = value; }
            }

            public int Duration
            {
                get
                {
                    if (this.OccurrenceCount == 0)
                    {
                        return 0;
                    }

                    return this.Occurrences.Sum(i => i.CompletionTime == DateTime.MinValue ? 0 : i.Duration);
                }
            }
            public decimal IORate
            {
                get
                {
                    var duration = this.Duration;

                    return duration == 0 ? 0 : this.FlushedStorage / ((decimal)duration / 1000M);
                }
            }

            #endregion

            public MemTableFlushLogInfo()
            {
                this.SetSession();
            }

            public DateTime EnqueuingStart;

            public int TaskId;
            public bool Completed;
            public DataRow LogDataRow;

            public decimal MaxIORate
            {
                get { return this.Occurrences.Max(i => i.IORate); }
            }
            public decimal MinIORate
            {
                get { return this.Occurrences.Where(i => i.IORate > 0).Min(i => i.IORate); }
            }
            public decimal AvgIORate
            {
                get { return this.Occurrences.Where(i => i.IORate > 0).Average(i => i.IORate); }
            }

            public int NbrWriteOPS
            {
                get { return this.Occurrences.Sum(i => i.NbrWriteOPS); }
            }

            public decimal FlushedStorage
            {
                get { return this.Occurrences.Sum(i => i.FlushedStorage); }
            }

            public IEnumerable<MemTableFlushOccurrenceLogInfo> Occurrences = Enumerable.Empty<MemTableFlushOccurrenceLogInfo>();

            public int OccurrenceCount
            {
                get
                {
                    return this.Occurrences == Enumerable.Empty<MemTableFlushOccurrenceLogInfo>()
                                ? 0
                                : this.Occurrences.Count(i => i.CompletionTime != DateTime.MinValue);
                }
            }

            public MemTableFlushOccurrenceLogInfo AddOccurrence(MemTableFlushOccurrenceLogInfo occurence)
            {
                if (this.Occurrences == Enumerable.Empty<MemTableFlushOccurrenceLogInfo>())
                {
                    this.Occurrences = new List<MemTableFlushOccurrenceLogInfo>();
                }

                ((List<MemTableFlushOccurrenceLogInfo>)this.Occurrences).Add(occurence);

                occurence.DataCenter = this.DataCenter;
                occurence.IPAddress = this.IPAddress;

                return occurence;
            }

            public MemTableFlushOccurrenceLogInfo AddUpdateOccurrence(string table,
                                                                        DateTime timeStamp,
                                                                        int nbrOPS,
                                                                        int taskId,
                                                                        decimal groupId,
                                                                        bool alwaysCreateNewInstance = true)
            {
                if (this.TaskId == 0)
                {
                    this.TaskId = taskId;
                }

                if (this.Occurrences == Enumerable.Empty<MemTableFlushOccurrenceLogInfo>())
                {
                    return this.AddOccurrence(new MemTableFlushOccurrenceLogInfo(this.Session)
                    {
                        Table = table,
                        StartTime = timeStamp,
                        TaskId = taskId,
                        GroupIndicator = groupId,
                        NbrWriteOPS = nbrOPS
                    });
                }

                var occurrence = alwaysCreateNewInstance
                                        ? null
                                        : ((List<MemTableFlushOccurrenceLogInfo>)this.Occurrences).Find(i => string.IsNullOrEmpty(i.Keyspace)
                                                                                                                && i.Table == table
                                                                                                                && i.TaskId == taskId);

                if (occurrence == null)
                {
                    return this.AddOccurrence(new MemTableFlushOccurrenceLogInfo(this.Session)
                    {
                        Table = table,
                        StartTime = timeStamp,
                        TaskId = taskId,
                        GroupIndicator = groupId,
                        NbrWriteOPS = nbrOPS
                    });
                }

                return occurrence;
            }

            public MemTableFlushOccurrenceLogInfo AddUpdateOccurrence(string keySpace,
                                                                        string table,
                                                                        string ssTableFilePath,
                                                                        DateTime timeStamp,
                                                                        decimal flushedStorage,
                                                                        int taskId,
                                                                        decimal groupId)
            {
                if (string.IsNullOrEmpty(this.Keyspace))
                {
                    this.Keyspace = keySpace;
                }
                if (this.TaskId == 0)
                {
                    this.TaskId = taskId;
                }

                if (this.Occurrences == Enumerable.Empty<MemTableFlushOccurrenceLogInfo>())
                {
                    return this.AddOccurrence(new MemTableFlushOccurrenceLogInfo(this.Session)
                    {
                        Keyspace = keySpace,
                        Table = table,
                        StartTime = timeStamp,
                        CompletionTime = timeStamp,
                        TaskId = taskId,
                        GroupIndicator = groupId,
                        FlushedStorage = flushedStorage,
                        SSTableFilePath = ssTableFilePath
                    });
                }

                var occurrence = ((List<MemTableFlushOccurrenceLogInfo>)this.Occurrences).Find(i => (string.IsNullOrEmpty(i.Keyspace) || i.Keyspace == keySpace)
                                                                                                        && i.Table == table
                                                                                                        && i.TaskId == taskId);

                if (occurrence == null)
                {
                    return this.AddOccurrence(new MemTableFlushOccurrenceLogInfo(this.Session)
                    {
                        Keyspace = keySpace,
                        Table = table,
                        StartTime = timeStamp,
                        CompletionTime = timeStamp,
                        TaskId = taskId,
                        GroupIndicator = groupId,
                        FlushedStorage = flushedStorage,
                        SSTableFilePath = ssTableFilePath
                    });
                }

                occurrence.Keyspace = keySpace;
                occurrence.CompletionTime = timeStamp;
                occurrence.FlushedStorage = flushedStorage;
                occurrence.SSTableFilePath = ssTableFilePath;

                return occurrence;
            }
        }

        public class RepairLogInfo : LogSessionInfo, ILogInfo
        {
            #region ILogInfo
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get { return null; } }
            public DateTime StartTime { get; set; }
            public DateTime CompletionTime { get; set; }
            public DateTime LogTimestamp { get { return this.StartTime; } }
            public object GroupIndicator { get; set; }
            public string Type { get { return "Repair"; } }
            public int Duration { get { return (int)(this.CompletionTime - this.StartTime).TotalMilliseconds; } }

            #endregion

            public string Options;
            public bool UserRequest;
            public string TokenRangeStart;
            public string TokenRangeEnd;
            public int GCs { get { return this.GCList == null ? 0 : this.GCList.Count(); } }
            public int Compactions { get { return this.CompactionList == null ? 0 : this.CompactionList.Count(); } }
            public int MemTableFlushes { get { return this.MemTableFlushList == null ? 0 : this.MemTableFlushList.Count(); } }
            public int Exceptions;

            public bool Aborted;
            public string Exception;
            public int NbrRepairs;
            public List<string> RepairNodes = new List<string>();
            public List<string> ReceivedNodes = new List<string>();
            public IEnumerable<SolrReIndexingLogInfo> SolrReIndexing = null;
            public IEnumerable<SolrHardCommitLogInfo> SolrHardCommits = null;
            public IEnumerable<GCLogInfo> GCList = null;
            public IEnumerable<ICompactionLogInfo> CompactionList = null;
            public IEnumerable<MemTableFlushLogInfo> MemTableFlushList = null;
            public IEnumerable<PerformanceInfo> PerformanceWarningList = null;

            public static string DCIPAddress(string dcName, string ipAdress)
            {
                return (dcName == null ? string.Empty : dcName) + "|" + ipAdress;
            }

            public RepairLogInfo Add()
            {
                return this;
            }

            public RepairLogInfo Completed(DateTime timestamp)
            {
                this.CompletionTime = timestamp;

                return this;
            }

            public RepairLogInfo Abort(DateTime timestamp, string exception = null)
            {
                this.Completed(timestamp);
                this.Aborted = true;

                if (!string.IsNullOrEmpty(exception))
                {
                    this.Exception = exception;
                }
                return this;
            }

        }

        public class PerformanceInfo : ILogInfo
        {
            #region ILogInfo
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime CompletionTime { get; set; }
            public DateTime LogTimestamp
            {
                get { return this.StartTime; }
                set { this.StartTime = value; }
            }
            public object GroupIndicator { get; set; }
            public string Type { get; set; }
            public int Duration
            {
                get
                {
                    if (this.StartTime != DateTime.MinValue && this.CompletionTime != DateTime.MinValue)
                    {
                        return (int)(this.CompletionTime - this.StartTime).TotalMilliseconds;
                    }

                    return this.Latency;
                }
            }
            #endregion

            public int Latency { get; set; }
        }

        public class MemTableStatInfo : ILogInfo
        {
            #region ILogInfo
            public string DataCenter { get; set; }
            public string IPAddress { get; set; }
            public string Keyspace { get; set; }
            public string Table { get; set; }
            public DateTime StartTime { get; set; }
            public DateTime CompletionTime { get { return this.LogTimestamp; } }
            public DateTime LogTimestamp { get; set; }
            public int Duration { get { return (int)(this.CompletionTime - this.StartTime).TotalMilliseconds; } }
            public object GroupIndicator { get; set; }
            public string Type { get { return "MemTable Stats"; } }

            #endregion

            public long InitialOPS { get; set; }
            public long InitialSize { get; set; }

            public long EndingOPS { get; set; }
            public long EndingSize { get; set; }
        }

        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<GCLogInfo>> GCOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<GCLogInfo>>();
        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<ICompactionLogInfo>> CompactionOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<ICompactionLogInfo>>();
        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo>> SolrReindexingOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo>>();
        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<MemTableFlushLogInfo>> MemTableFlushOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<MemTableFlushLogInfo>>();
        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<PerformanceInfo>> PerformanceOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<PerformanceInfo>>();
        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<SolrHardCommitLogInfo>> SolrHardCommits = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<SolrHardCommitLogInfo>>();
        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<MemTableStatInfo>> MemTableStats = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<MemTableStatInfo>>();

        const decimal ReferenceIncrementValue = 0.000001m;

        static void InitializeStatusDataTable(DataTable dtCStatusLog)
        {
            if (dtCStatusLog.Columns.Count == 0)
            {
                dtCStatusLog.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;

                dtCStatusLog.Columns.Add("Timestamp", typeof(DateTime));
                dtCStatusLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Node IPAddress", typeof(string));
                dtCStatusLog.Columns.Add("Pool/Cache Type", typeof(string)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("KeySpace", typeof(string)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Table", typeof(string)).AllowDBNull = true;

                dtCStatusLog.Columns.Add("GC Time (ms)", typeof(long)).AllowDBNull = true; //h
                dtCStatusLog.Columns.Add("Eden-From (mb)", typeof(decimal)).AllowDBNull = true; //i
                dtCStatusLog.Columns.Add("Eden-To (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Old-From (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Old-To (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Survivor-From (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Survivor-To (mb)", typeof(decimal)).AllowDBNull = true; //n

                dtCStatusLog.Columns.Add("Active", typeof(object)).AllowDBNull = true; //o
                dtCStatusLog.Columns.Add("Pending", typeof(object)).AllowDBNull = true; //p
                dtCStatusLog.Columns.Add("Completed", typeof(long)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Blocked", typeof(long)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("All Time Blocked", typeof(long)).AllowDBNull = true; //s

                dtCStatusLog.Columns.Add("Size (mb)", typeof(decimal)).AllowDBNull = true;//t
                dtCStatusLog.Columns.Add("Capacity (mb)", typeof(decimal)).AllowDBNull = true; //u
                dtCStatusLog.Columns.Add("KeysToSave", typeof(string)).AllowDBNull = true; //v
                dtCStatusLog.Columns.Add("MemTable OPS", typeof(long)).AllowDBNull = true; //w
                dtCStatusLog.Columns.Add("Data (mb)", typeof(decimal)).AllowDBNull = true; //x

                dtCStatusLog.Columns.Add("SSTables", typeof(int)).AllowDBNull = true; //y
                dtCStatusLog.Columns.Add("From (mb)", typeof(decimal)).AllowDBNull = true; //z
                dtCStatusLog.Columns.Add("To (mb)", typeof(decimal)).AllowDBNull = true;//aa
                dtCStatusLog.Columns.Add("Latency (ms)", typeof(int)).AllowDBNull = true; //ab
                dtCStatusLog.Columns.Add("Rate (MB/s)", typeof(decimal)).AllowDBNull = true; //ac
                dtCStatusLog.Columns.Add("Partitions Merged", typeof(string)).AllowDBNull = true; //ad
                dtCStatusLog.Columns.Add("Merge Counts", typeof(string)).AllowDBNull = true; //ae

                dtCStatusLog.Columns.Add("Session", typeof(string)).AllowDBNull = true; //af
                dtCStatusLog.Columns.Add("Duration (ms)", typeof(int)).AllowDBNull = true; //ag
                dtCStatusLog.Columns.Add("Start Token Range (exclusive)", typeof(string)).AllowDBNull = true; //ah
                dtCStatusLog.Columns.Add("End Token Range (inclusive)", typeof(string)).AllowDBNull = true; //ai
                dtCStatusLog.Columns.Add("Nbr GCs", typeof(int)).AllowDBNull = true; //aj
                dtCStatusLog.Columns.Add("Nbr Compactions", typeof(int)).AllowDBNull = true; //ak
                dtCStatusLog.Columns.Add("Nbr MemTable Flush Events", typeof(int)).AllowDBNull = true; //al
                dtCStatusLog.Columns.Add("Nbr Solr ReIdxs", typeof(int)).AllowDBNull = true; //am
                dtCStatusLog.Columns.Add("Nbr Exceptions", typeof(int)).AllowDBNull = true; //an
                dtCStatusLog.Columns.Add("Requested", typeof(bool)).AllowDBNull = true; //ao
                dtCStatusLog.Columns.Add("Aborted", typeof(int)).AllowDBNull = true; //ap
                dtCStatusLog.Columns.Add("Session Path", typeof(string)).AllowDBNull = true; //aq

                //dtCStatusLog.DefaultView.Sort = "[Timestamp] DESC, [Data Center], [Pool/Cache Type], [KeySpace], [Table], [Node IPAddress]";
            }
        }

        static void ParseCassandraLogIntoStatusLogDataTable(DataTable dtroCLog,
                                                                DataTable dtCStatusLog,
                                                                DataTable dtCFStats,
                                                                DataTable dtTPStats,
                                                                Common.Patterns.Collections.ThreadSafe.Dictionary<string, string> dictGCIno,
                                                                string ipAddress,
                                                                string dcName,
                                                                IEnumerable<string> ignoreKeySpaces,
                                                                List<CKeySpaceTableNames> kstblExists)
        {
            #region data table definations
            //StatusLogger.java:51 - Pool Name                    Active   Pending      Completed   Blocked  All Time Blocked
            //StatusLogger.java:66 - MutationStage                     0         0     2424035521         0                 0
            //StatusLogger.java:66 - CompactionManager                 1         1
            //StatusLogger.java:66 - MessagingService                n/a       0/0
            //
            //StatusLogger.java:97 - Cache Type                     Size                 Capacity               KeysToSave
            //StatusLogger.java:99 - KeyCache                  100245406                104857600                      all
            //
            //StatusLogger.java:112 - ColumnFamily                Memtable ops,data
            //StatusLogger.java:115 - dse_perf.node_slow_log            2120,829964
            //
            //CompactionTask.java - Compacted 4 sstables to [/data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/system-size_estimates-ka-11348,]. 2,270,620 bytes to 566,478 (~24% of original) in 342ms = 1.579636MB/s. 40 total partitions merged to 10. Partition merge counts were {4:10, }

            //Out of order example:
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,820  StatusLogger.java:115 - system.hints                          23,1572
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,820  StatusLogger.java:66 - MiscStage                         0         0              0         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,829  StatusLogger.java:115 - system.IndexInfo                          0,0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,829  StatusLogger.java:115 - system.schema_columnfamilies                 0,0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,829  StatusLogger.java:115 - system.schema_triggers                    0,0
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,829  StatusLogger.java:66 - AntiEntropySessions               0         0           3467         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,829  StatusLogger.java:115 - system.size_estimates          377300,9604612
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,829  StatusLogger.java:66 - HintedHandoff                     0         1           1400         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,829  StatusLogger.java:115 - system.paxos                              0,0
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,830  StatusLogger.java:66 - GossipStage                       0         0        1490684         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,830  StatusLogger.java:115 - system.peer_events                        0,0
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:66 - CacheCleanupExecutor              0         0              0         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:115 - system.range_xfers                        0,0
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:66 - InternalResponseStage             0         0              0         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:115 - system.compactions_in_progress                 0,0
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:66 - CommitLogArchiver                 0         0              0         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:115 - system.peers                              0,0
            //INFO[Service Thread] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:66 - CompactionExecutor                0         0        2120758         0                 0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:115 - system.schema_keyspaces                   0,0
            //INFO[ScheduledTasks: 1] 2016 - 09 - 18 19:29:55,831  StatusLogger.java:115 - system.schema_usertypes                   0,0
            //INFO  [AntiEntropySessions:19] 2016-10-17 16:16:07,725  RepairSession.java:260 - [repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] new session: will sync c1249170.ews.int/10.12.49.11, /10.12.51.29 on range (7698152963051967815,7704157762555128476] for OpsCenter.[rollups7200, settings, events, rollups60, rollups86400, rollups300, events_timeline, backup_reports, bestpractice_results, pdps]
            //INFO  [AntiEntropySessions:19] 2016-10-17 16:16:08,478  RepairSession.java:299 - [repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] session completed successfully

            InitializeStatusDataTable(dtCStatusLog);

            #endregion

            if (dtroCLog.Rows.Count > 0)
            {
                //		dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                //		dtCLog.Columns.Add("Node IPAddress", typeof(string));
                //		dtCLog.Columns.Add("Timestamp", typeof(DateTime));
                //		dtCLog.Columns.Add("Indicator", typeof(string));
                //		dtCLog.Columns.Add("Task", typeof(string));
                //		dtCLog.Columns.Add("Item", typeof(string));
                //		dtCLog.Columns.Add("Associated Item", typeof(string)).AllowDBNull = true;
                //		dtCLog.Columns.Add("Associated Value", typeof(object)).AllowDBNull = true;
                //		dtCLog.Columns.Add("Description", typeof(string));
                //		dtCLog.Columns.Add("Flagged", typeof(int)).AllowDBNull = true;
                var statusLogItem = from dr in dtroCLog.AsEnumerable()
                                    let item = dr.Field<string>("Item")
                                    let timestamp = dr.Field<DateTime>("Timestamp")
                                    let flagged = dr.Field<int?>("Flagged")
                                    let descr = dr.Field<string>("Description")?.Trim()
                                    where ((flagged.HasValue && (flagged.Value == (int)LogFlagStatus.Stats || flagged.Value == (int)LogFlagStatus.StatsOnly))
                                            || item == "GCInspector.java"
                                            || item == "StatusLogger.java"
                                            || item == "CompactionTask.java")
                                    orderby timestamp ascending, item ascending
                                    select new
                                    {
                                        Task = dr.Field<string>("Task"),
                                        TaskId = dr.Field<int?>("TaskId"),
                                        Item = item,
                                        Timestamp = timestamp,
                                        Flagged = flagged,
                                        Exception = dr.Field<string>("Exception"),
                                        AssocItem = dr.Field<string>("Associated Item"),
                                        AssocValue = dr.Field<object>("Associated Value"),
                                        Description = descr
                                    };


                var gcLatencies = new List<int>();
                var pauses = new List<long>();
                var compactionLatencies = new List<Tuple<string, string, int>>();
                var compactionRates = new List<Tuple<string, string, decimal>>();
                var partitionLargeSizes = new List<Tuple<string, string, decimal>>();
                var tombstoneCounts = new List<Tuple<string, string, string, int>>();
                var tpStatusCounts = new List<Tuple<string, long, long, long, long, long>>();
                var statusMemTables = new List<Tuple<string, string, long, decimal>>();
                var tpSlowQueries = new List<int>();
                var batchSizes = new List<Tuple<string, string, string, long>>();
                var jvmFatalErrors = new List<string>();
                var workPoolErrors = new List<string>();
                var nodeStatus = new List<Tuple<string, string, string>>();
                var droppedHints = new List<int>();
                var timedoutHints = new List<int>();
                var droppedMutations = new List<int>();
                var maxMemoryAllocFailed = new List<int>();
                var solrReindexing = new List<SolrReIndexingLogInfo>();
                var solrHardCommit = new List<SolrHardCommitLogInfo>();
                var detectedSchemaChanges = new List<Tuple<char, string, string>>();
                var shardStateChanges = new List<string>();
                var memTblStats = new List<MemTableStatInfo>();
                long grpInd = CLogSummaryInfo.IncrementGroupInicator();
                var refCnt = (decimal)grpInd;

                foreach (var item in statusLogItem)
                {
                    if (string.IsNullOrEmpty(item.Item))
                    {
                        continue;
                    }

                    refCnt += ReferenceIncrementValue;

                    if (item.Item == "GCInspector.java")
                    {
                        #region GCInspector.java

                        if (string.IsNullOrEmpty(item.Description))
                        {
                            continue;
                        }

                        object time = null;
                        var dataRow = dtCStatusLog.NewRow();

                        if (item.Description.TrimStart().StartsWith("GC for ParNew"))
                        {
                            var splits = RegExGCLine.Split(item.Description);
                            time = DetermineTime(splits[1]);

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC-ParNew";
                            dataRow["GC Time (ms)"] = (long)((dynamic)time);
                            dataRow["Reconciliation Reference"] = refCnt;

                            dtCStatusLog.Rows.Add(dataRow);
                            gcLatencies.Add((int)((dynamic)time));

                            dictGCIno.TryAdd((dcName == null ? string.Empty : dcName) + "|" + ipAddress, "GC-ParNew");
                        }
                        if (item.Description.TrimStart().StartsWith("ConcurrentMarkSweep"))
                        {
                            var splits = RegExGCMSLine.Split(item.Description);
                            time = DetermineTime(splits[1]);

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC-CMS";
                            dataRow["GC Time (ms)"] = (long)((dynamic)time);
                            dataRow["Reconciliation Reference"] = refCnt;

                            if (splits.Length >= 4 && !string.IsNullOrEmpty(splits[2]))
                            {
                                dataRow["Old-From (mb)"] = ConvertInToMB(splits[2], "bytes");
                                dataRow["Old-To (mb)"] = ConvertInToMB(splits[3], "bytes");
                            }
                            if (splits.Length >= 6 && !string.IsNullOrEmpty(splits[4]))
                            {
                                dataRow["Eden-From (mb)"] = ConvertInToMB(splits[4], "bytes");
                                dataRow["Eden-To (mb)"] = ConvertInToMB(splits[5], "bytes");
                            }

                            dtCStatusLog.Rows.Add(dataRow);
                            gcLatencies.Add((int)((dynamic)time));

                            dictGCIno.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress, "GC-CMS", (item1, item2) => "GC-CMS");
                        }
                        else if (item.Description.TrimStart().StartsWith("G1 Young Generation GC in"))
                        {
                            var splits = RegExG1Line.Split(item.Description);
                            time = DetermineTime(splits[1]);

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC-G1";
                            dataRow["GC Time (ms)"] = (long)((dynamic)time);
                            dataRow["Reconciliation Reference"] = refCnt;

                            if (splits.Length >= 4 && !string.IsNullOrEmpty(splits[2]))
                            {
                                dataRow["Eden-From (mb)"] = ConvertInToMB(splits[2], "bytes");
                                dataRow["Eden-To (mb)"] = ConvertInToMB(splits[3], "bytes");
                            }
                            if (splits.Length >= 6 && !string.IsNullOrEmpty(splits[4]))
                            {
                                dataRow["Old-From (mb)"] = ConvertInToMB(splits[4], "bytes");
                                dataRow["Old-To (mb)"] = ConvertInToMB(splits[5], "bytes");
                            }
                            if (splits.Length >= 8 && !string.IsNullOrEmpty(splits[6]))
                            {
                                dataRow["Survivor-From (mb)"] = ConvertInToMB(splits[6], "bytes");
                                dataRow["Survivor-To (mb)"] = ConvertInToMB(splits[7], "bytes");
                            }

                            dtCStatusLog.Rows.Add(dataRow);
                            gcLatencies.Add((int)((dynamic)time));

                            dictGCIno.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress, "GC-C1", (item1, item2) => "GC-G1");
                        }

                        if (time != null)
                        {
                            var gcLogInfo = new GCLogInfo()
                            {
                                LogTimestamp = item.Timestamp,
                                DataCenter = dcName,
                                IPAddress = ipAddress,
                                Duration = (int)((dynamic)time),
                                GroupIndicator = refCnt,
                                GCEdenFrom = dataRow.IsNull("Eden-From (mb)") ? 0 : dataRow.Field<decimal>("Eden-From (mb)"),
                                GCEdenTo = dataRow.IsNull("Eden-To (mb)") ? 0 : dataRow.Field<decimal>("Eden-To (mb)"),
                                GCSurvivorFrom = dataRow.IsNull("Survivor-From (mb)") ? 0 : dataRow.Field<decimal>("Survivor-From (mb)"),
                                GCSurvivorTo = dataRow.IsNull("Survivor-To (mb)") ? 0 : dataRow.Field<decimal>("Survivor-To (mb)"),
                                GCOldFrom = dataRow.IsNull("Old-From (mb)") ? 0 : dataRow.Field<decimal>("Old-From (mb)"),
                                GCOldTo = dataRow.IsNull("Old-To (mb)") ? 0 : dataRow.Field<decimal>("Old-To (mb)"),
                            };

                            GCOccurrences.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
                                                        ignore => { return new Common.Patterns.Collections.ThreadSafe.List<GCLogInfo>() { gcLogInfo }; },
                                                        (ignore, gcList) =>
                                                        {
                                                            gcList.Add(gcLogInfo);
                                                            return gcList;
                                                        });
                        }

                        #endregion
                    }
                    else if (item.Item == "FailureDetector.java")
                    {
                        #region FailureDetector.java

                        if (item.Exception.StartsWith("Pause"))
                        {
                            var dataRow = dtCStatusLog.NewRow();
                            var time = item.AssocValue as long?;

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC Pause";
                            dataRow["Reconciliation Reference"] = refCnt;

                            if (time.HasValue)
                            {
                                dataRow["GC Time (ms)"] = time.Value / 1000000L;
                                pauses.Add(time.Value);
                            }
                            else
                            {
                                Program.ConsoleWarnings.Increment("Invalid Pause Value...");
                                Logger.Dump(new DataRow[] { dataRow }, Logger.DumpType.Warning, "Invalid Pause Value");
                            }

                            dtCStatusLog.Rows.Add(dataRow);
                        }
                        #endregion
                    }
                    else if (item.Item == "StatusLogger.java")
                    {
                        #region StatusLogger.java

                        if (string.IsNullOrEmpty(item.Description))
                        {
                            continue;
                        }

                        var descr = item.Description.Trim();

                        if (descr.StartsWith("Pool Name")
                             || descr.StartsWith("ColumnFamily ")
                             || descr.StartsWith("Table ")
                             || descr.StartsWith("Cache Type"))
                        {
                            continue;
                        }
                        else
                        {
                            try
                            {
                                if (RegExCacheLine.IsMatch(descr))
                                {
                                    var splits = RegExCacheLine.Split(descr);
                                    var dataRow = dtCStatusLog.NewRow();

                                    dataRow["Timestamp"] = item.Timestamp;
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = splits[1];
                                    dataRow["Reconciliation Reference"] = refCnt;
                                    dataRow["Size (mb)"] = ConvertInToMB(splits[2], "bytes");
                                    dataRow["Capacity (mb)"] = ConvertInToMB(splits[3], "bytes");
                                    dataRow["KeysToSave"] = splits[4];

                                    dtCStatusLog.Rows.Add(dataRow);
                                    continue;
                                }
                                else if (RegExTblLine.IsMatch(descr))
                                {
                                    var splits = RegExTblLine.Split(descr);
                                    var ksTable = SplitTableName(splits[1], null);

                                    if (ignoreKeySpaces.Contains(ksTable.Item1))
                                    {
                                        continue;
                                    }

                                    var dataRow = dtCStatusLog.NewRow();
                                    var dataSize = long.Parse(splits[3]);

                                    dataRow["Timestamp"] = item.Timestamp;
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = "ColumnFamily";
                                    dataRow["KeySpace"] = ksTable.Item1;
                                    dataRow["Table"] = ksTable.Item2;
                                    dataRow["Reconciliation Reference"] = refCnt;
                                    dataRow["MemTable OPS"] = long.Parse(splits[2]);
                                    dataRow["Data (mb)"] = ConvertInToMB(splits[3], "bytes");

                                    dtCStatusLog.Rows.Add(dataRow);

                                    var mOPS = (long)dataRow["MemTable OPS"];
                                    var mSize = (decimal)dataRow["Data (mb)"];

                                    statusMemTables.Add(new Tuple<string, string, long, decimal>(ksTable.Item1,
                                                                                                            ksTable.Item2,
                                                                                                            mOPS,
                                                                                                            mSize));

                                    var memTblStat = memTblStats.FirstOrDefault(m => m.Keyspace == ksTable.Item1 && m.Table == ksTable.Item2);

                                    if (memTblStat == null
                                            || memTblStat.EndingOPS > mOPS)
                                    {
                                        memTblStat = new MemTableStatInfo()
                                        {
                                            DataCenter = dcName,
                                            IPAddress = ipAddress,
                                            Keyspace = ksTable.Item1,
                                            Table = ksTable.Item2,
                                            StartTime = item.Timestamp,
                                            LogTimestamp = item.Timestamp,
                                            GroupIndicator = grpInd,
                                            InitialOPS = memTblStat == null ? mOPS : 0,
                                            InitialSize = memTblStat == null ? (long)(mSize * BytesToMB) : 0,
                                            EndingOPS = mOPS,
                                            EndingSize = (long)(mSize * BytesToMB)
                                        };
                                        memTblStats.Add(memTblStat);
                                    }
                                    else
                                    {
                                        memTblStat.EndingOPS = mOPS;
                                        memTblStat.EndingSize = (long)(mSize * BytesToMB);
                                        memTblStat.LogTimestamp = item.Timestamp;
                                    }

                                    continue;
                                }
                                else if (RegExPoolLine.IsMatch(descr)
                                            || RegExPool2Line.IsMatch(descr))
                                {
                                    var splits = RegExPoolLine.Split(descr);
                                    var dataRow = dtCStatusLog.NewRow();

                                    if (splits.Length == 1)
                                    {
                                        splits = RegExPool2Line.Split(descr);
                                    }

                                    dataRow["Timestamp"] = item.Timestamp;
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = splits[1];
                                    dataRow["Reconciliation Reference"] = refCnt;

                                    if (splits.Length == 8)
                                    {
                                        dataRow["Active"] = long.Parse(splits[2]);
                                        dataRow["Pending"] = long.Parse(splits[3]);
                                        dataRow["Completed"] = long.Parse(splits[4]);
                                        dataRow["Blocked"] = long.Parse(splits[5]);
                                        dataRow["All Time Blocked"] = long.Parse(splits[6]);

                                        tpStatusCounts.Add(new Tuple<string, long, long, long, long, long>(splits[1],
                                                                                                            (long)dataRow["Active"],
                                                                                                            (long)dataRow["Pending"],
                                                                                                            (long)dataRow["Completed"],
                                                                                                            (long)dataRow["Blocked"],
                                                                                                            (long)dataRow["All Time Blocked"]));
                                    }
                                    else
                                    {
                                        long numValue;
                                        if (long.TryParse(splits[2], out numValue))
                                        {
                                            dataRow["Active"] = numValue;
                                        }
                                        else
                                        {
                                            dataRow["Active"] = splits[2];
                                        }

                                        if (long.TryParse(splits[3], out numValue))
                                        {
                                            dataRow["Pending"] = numValue;
                                        }
                                        else
                                        {
                                            dataRow["Pending"] = splits[3];
                                        }
                                    }

                                    dtCStatusLog.Rows.Add(dataRow);
                                    continue;
                                }
                                else
                                {
                                    var msg = string.Format("StatusLogger Invalid Line \"{0}\" for {1}", descr, ipAddress);
                                    Logger.Instance.Warn(msg);
                                    Program.ConsoleWarnings.Increment(string.Empty, msg, 45);
                                }
                            }
                            catch (Exception e)
                            {
                                var msg = string.Format("StatusLogger Invalid Line\\Exception \"{0}\" for {1}", descr, ipAddress);
                                Logger.Instance.Error(msg, e);
                                Program.ConsoleErrors.Increment(string.Empty, msg, 45);
                            }
                        }
                        #endregion
                    }
                    else if (item.Item == "CompactionTask.java")
                    {
                        #region CompactionTask
                        var splits = RegExCompactionTaskCompletedLine.Split(item.Description);

                        if (splits.Length == 12)
                        {
                            var ksItems = DSEDiagnosticLibrary.StringHelpers.ParseSSTableFileIntoKSTableNames(splits[2]);

                            if (ksItems != null)
                            {
                                if (ignoreKeySpaces.Contains(ksItems.Item1))
                                {
                                    continue;
                                }

                                var ksItem = kstblExists.FirstOrDefault(k => k.KeySpaceName == ksItems.Item1 && k.Name == ksItems.Item2);

                                if (ksItem == null)
                                {
                                    ksItem = new CKeySpaceTableNames(ksItems.Item1,
                                                                        ksItems.Item4 == null
                                                                            ? ksItems.Item2
                                                                            : ksItems.Item4 + '.' + ksItems.Item2,
                                                                        ksItems.Item4 != null);
                                }

                                var dataRow = dtCStatusLog.NewRow();
                                var time = DetermineTime(splits[6]);
                                var rate = decimal.Parse(splits[7].Replace(",", string.Empty));

                                dataRow["Timestamp"] = item.Timestamp;
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["Pool/Cache Type"] = "Compaction";
                                dataRow["Reconciliation Reference"] = refCnt;

                                dataRow["KeySpace"] = ksItem.KeySpaceName;
                                dataRow["Table"] = ksItem.DisplayName;
                                dataRow["SSTables"] = int.Parse(splits[1].Replace(",", string.Empty));
                                dataRow["From (mb)"] = ConvertInToMB(splits[3], "bytes");
                                dataRow["To (mb)"] = ConvertInToMB(splits[4], "bytes");
                                dataRow["Latency (ms)"] = time;
                                dataRow["Rate (MB/s)"] = rate;
                                dataRow["Partitions Merged"] = splits[8] + ":" + splits[9];
                                dataRow["Merge Counts"] = splits[10];

                                dtCStatusLog.Rows.Add(dataRow);
                                compactionLatencies.Add(new Tuple<string, string, int>(ksItem.KeySpaceName, ksItem.Name, (int)time));
                                compactionRates.Add(new Tuple<string, string, decimal>(ksItem.KeySpaceName, ksItem.Name, rate));

                                var compactionLogInfo = new CompactionLogInfo()
                                {
                                    LogTimestamp = item.Timestamp,
                                    DataCenter = dcName,
                                    IPAddress = ipAddress,
                                    GroupIndicator = refCnt,
                                    Keyspace = ksItem.KeySpaceName,
                                    Table = ksItem.Name,
                                    SSTables = dataRow.Field<int>("SSTables"),
                                    OldSize = dataRow.Field<decimal>("From (mb)"),
                                    NewSize = dataRow.Field<decimal>("To (mb)"),
                                    Duration = (int)time,
                                    IORate = rate,
                                    PartitionsMerged = dataRow.Field<string>("Partitions Merged"),
                                    MergeCounts = dataRow.Field<string>("Merge Counts")
                                };

                                CompactionOccurrences.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
                                    ignore =>
                                    {
                                        return new Common.Patterns.Collections.ThreadSafe.List<ICompactionLogInfo>() { compactionLogInfo };
                                    },
                                    (ignore, gcList) =>
                                    {
                                        gcList.Add(compactionLogInfo);
                                        return gcList;
                                    });
                            }
                        }
                        #endregion
                    }
                    else if (item.Item == "CompactionController.java"
                                || item.Item == "SSTableWriter.java"
                                || item.Item == "BigTableWriter.java")
                    {
                        #region CompactionController or SSTableWriter or BigTableWriter

                        var kstblName = item.AssocItem;
                        var partSize = item.AssocValue as decimal?;

                        if (kstblName == null || !partSize.HasValue)
                        {
                            continue;
                        }

                        var kstblSplit = SplitTableName(kstblName, null);

                        if (ignoreKeySpaces.Contains(kstblSplit.Item1))
                        {
                            continue;
                        }

                        var dataRow = dtCStatusLog.NewRow();

                        dataRow["Timestamp"] = item.Timestamp;
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Pool/Cache Type"] = "Partition Size";
                        dataRow["Reconciliation Reference"] = refCnt;

                        dataRow["KeySpace"] = kstblSplit.Item1;
                        dataRow["Table"] = kstblSplit.Item2;
                        dataRow["Size (mb)"] = partSize.Value / BytesToMB;

                        dtCStatusLog.Rows.Add(dataRow);

                        partitionLargeSizes.Add(new Tuple<string, string, decimal>(kstblSplit.Item1, kstblSplit.Item2, partSize.Value));

                        #endregion
                    }
                    else if (item.Item == "SliceQueryFilter.java" || item.Item == "ReadCommand.java")
                    {
                        #region SliceQueryFilter

                        var kstblName = item.AssocItem;
                        var partSize = item.AssocValue as int?;
                        var warningType = item.Exception;

                        if (kstblName == null || !partSize.HasValue || warningType == null)
                        {
                            continue;
                        }

                        //Query Tombstones Warning
                        //Query Tombstones Aborted
                        //Query Reads Warning

                        var kstblSplit = SplitTableName(kstblName, null);

                        if (ignoreKeySpaces.Contains(kstblSplit.Item1))
                        {
                            continue;
                        }

                        var dataRow = dtCStatusLog.NewRow();
                        var indType = warningType == "Query Tombstones Warning"
                                                            ? "Tombstones warning"
                                                            : (warningType == "Query Tombstones Aborted"
                                                                    ? "Tombstones query aborted"
                                                                    : (warningType == "Query Reads Warning" ? "Query read warning" : warningType));

                        dataRow["Timestamp"] = item.Timestamp;
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Pool/Cache Type"] = indType;
                        dataRow["Reconciliation Reference"] = refCnt;

                        dataRow["KeySpace"] = kstblSplit.Item1;
                        dataRow["Table"] = kstblSplit.Item2;
                        dataRow["Completed"] = (long)partSize.Value;

                        dtCStatusLog.Rows.Add(dataRow);

                        tombstoneCounts.Add(new Tuple<string, string, string, int>(indType,
                                                                                    kstblSplit.Item1,
                                                                                    kstblSplit.Item2,
                                                                                    partSize.Value));

                        #endregion
                    }
                    else if (item.Item == "CqlSlowLogWriter.java")
                    {
                        #region CqlSlowLogWriter
                        var time = item.AssocValue as int?;

                        if (time.HasValue)
                        {
                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "Slow Query latency";
                            dataRow["Reconciliation Reference"] = refCnt;
                            dataRow["Latency (ms)"] = time.Value;

                            dtCStatusLog.Rows.Add(dataRow);

                            tpSlowQueries.Add(time.Value);

                            var performanceInfo = new PerformanceInfo()
                            {
                                Type = "Slow Query",
                                DataCenter = dcName,
                                IPAddress = ipAddress,
                                GroupIndicator = refCnt,
                                LogTimestamp = item.Timestamp,
                                Latency = time.Value
                            };

                            PerformanceOccurrences.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
                                    ignore =>
                                    {
                                        return new Common.Patterns.Collections.ThreadSafe.List<PerformanceInfo>() { performanceInfo };
                                    },
                                    (ignore, performanceList) =>
                                    {
                                        performanceList.Add(performanceInfo);
                                        return performanceList;
                                    });
                        }

                        #endregion
                    }
                    else if (item.Item == "BatchStatement.java")
                    {
                        #region BatchSize

                        var kstblNames = item.AssocItem;
                        var batchSize = item.AssocValue as int?;

                        if (kstblNames == null || !batchSize.HasValue)
                        {
                            continue;
                        }

                        var multiKSTblNames = kstblNames.Contains(',');

                        foreach (var kstblName in multiKSTblNames ? kstblNames.Split(',') : new string[] { kstblNames })
                        {
                            var kstblSplit = SplitTableName(kstblName, null);

                            if (ignoreKeySpaces.Contains(kstblSplit.Item1))
                            {
                                continue;
                            }

                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "Batch size";
                            dataRow["Reconciliation Reference"] = refCnt;
                            dataRow["KeySpace"] = kstblSplit.Item1;
                            dataRow["Table"] = kstblSplit.Item2;
                            dataRow["Completed"] = (long)batchSize.Value;

                            dtCStatusLog.Rows.Add(dataRow);

                            batchSizes.Add(new Tuple<string, string, string, long>("Batch size", kstblSplit.Item1, kstblSplit.Item2, batchSize.Value));
                        }

                        #endregion
                    }
                    else if (item.Item == "JVMStabilityInspector.java"
                                || (item.Item == "Message.java"
                                        && item.Exception.Contains("java.lang.OutOfMemoryError")))
                    {
                        #region JVMStabilityInspector or java.lang.OutOfMemoryError

                        if (!string.IsNullOrEmpty(item.Exception))
                        {
                            var pathNodes = RegExSummaryLogExceptionNodes.Split(item.Exception);
                            var keyNode = pathNodes.Length == 0 ? item.Exception : pathNodes.Last();
                            var exceptionNameSplit = RegExSummaryLogExceptionName.Split(keyNode);
                            var exceptionName = exceptionNameSplit.Length < 2 ? keyNode : exceptionNameSplit[1];

                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = exceptionName;
                            dataRow["Reconciliation Reference"] = refCnt;

                            dtCStatusLog.Rows.Add(dataRow);

                            jvmFatalErrors.Add(exceptionName);
                        }

                        #endregion
                    }
                    else if (item.Item == "WorkPool.java")
                    {
                        #region WorkPool

                        if (!string.IsNullOrEmpty(item.Exception))
                        {
                            var pathNodes = RegExSummaryLogExceptionNodes.Split(item.Exception);
                            var keyNode = pathNodes.Length == 0 ? item.Exception : pathNodes.Last();
                            var exceptionNameSplit = RegExSummaryLogExceptionName.Split(keyNode);
                            var exceptionName = exceptionNameSplit.Length < 2 ? keyNode : exceptionNameSplit[1];

                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = exceptionName;
                            dataRow["Reconciliation Reference"] = refCnt;

                            dtCStatusLog.Rows.Add(dataRow);

                            workPoolErrors.Add(exceptionName);
                        }

                        #endregion
                    }
                    else if (item.Item == "StorageService.java")
                    {
                        #region StorageService

                        if (!string.IsNullOrEmpty(item.Exception))
                        {
                            var kstblName = item.AssocItem;
                            string ksName = null;
                            string tblName = null;

                            if (!string.IsNullOrEmpty(kstblName))
                            {
                                var kstblSplit = SplitTableName(kstblName, null);

                                ksName = kstblSplit.Item1;
                                tblName = kstblSplit.Item2;

                                if (ignoreKeySpaces.Contains(ksName))
                                {
                                    continue;
                                }
                            }

                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = item.Exception;
                            dataRow["Reconciliation Reference"] = refCnt;
                            dataRow["KeySpace"] = ksName;
                            dataRow["Table"] = tblName;

                            dtCStatusLog.Rows.Add(dataRow);

                            nodeStatus.Add(new Tuple<string, string, string>(item.Exception, ksName, tblName));
                        }

                        #endregion
                    }
                    else if (item.Item == "HintedHandoffMetrics.java")
                    {
                        #region HintedHandoffMetrics.java
                        //		WARN  [HintedHandoffManager:1] 2016-07-25 04:26:10,445  HintedHandoffMetrics.java:79 - /10.170.110.191 has 1711 dropped hints, because node is down past configured hint window.
                        if (item.Exception.StartsWith("Dropped Hints"))
                        {
                            var nbrDropped = item.AssocValue as int?;

                            if (nbrDropped.HasValue)
                            {
                                var dataRow = dtCStatusLog.NewRow();

                                dataRow["Timestamp"] = item.Timestamp;
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["Pool/Cache Type"] = item.Exception;
                                dataRow["Reconciliation Reference"] = refCnt;
                                dataRow["Completed"] = (long)nbrDropped.Value;

                                dtCStatusLog.Rows.Add(dataRow);

                                droppedHints.Add(nbrDropped.Value);
                            }
                            else
                            {
                                Program.ConsoleWarnings.Increment("Invalid Dropped Hints Value...");
                                Logger.Dump(string.Format("Invalid Dropped Hints Value of \"{0}\" for {1} => {2}", item.AssocValue, ipAddress, item.AssocItem),
                                                            Logger.DumpType.Warning);
                            }
                        }
                        #endregion
                    }
                    else if (item.Item == "HintedHandOffManager.java")
                    {
                        #region HintedHandOffManager.java
                        //INFO  [HintedHandoff:2] 2016-10-29 09:04:51,254  HintedHandOffManager.java:486 - Timed out replaying hints to /10.12.51.20; aborting (0 delivered)
                        if (item.Exception.StartsWith("Hints"))
                        {
                            var nbrCompleted = item.AssocValue as int?;

                            if (nbrCompleted.HasValue)
                            {
                                var dataRow = dtCStatusLog.NewRow();

                                dataRow["Timestamp"] = item.Timestamp;
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["Pool/Cache Type"] = item.Exception;
                                dataRow["Reconciliation Reference"] = refCnt;
                                dataRow["Completed"] = (long)nbrCompleted.Value;

                                dtCStatusLog.Rows.Add(dataRow);

                                timedoutHints.Add(nbrCompleted.Value);
                            }
                            else
                            {
                                Program.ConsoleWarnings.Increment("Invalid Hints Value...");
                                Logger.Dump(string.Format("Invalid Hints Value of \"{0}\" for {1} => {2}", item.AssocValue, ipAddress, item.AssocItem),
                                                            Logger.DumpType.Warning);
                            }
                        }
                        #endregion
                    }
                    else if (item.Item == "NoSpamLogger.java")
                    {
                        #region NoSpamLogger.java
                        //NoSpamLogger.java:94 - Unlogged batch covering 80 partitions detected against table[hlservicing.lvl1_bkfs_invoicechronology]. You should use a logged batch for atomicity, or asynchronous writes for performance.
                        //NoSpamLogger.java:94 - Unlogged batch covering 94 partitions detected against tables [hl_data_commons.l3_heloc_fraud_score_hist, hl_data_commons.l3_heloc_fraud_score]. You should use a logged batch for atomicity, or asynchronous writes for performance.
                        //Maximum memory usage reached (536,870,912 bytes), cannot allocate chunk of 1,048,576 bytes
                        var assocValue = item.AssocValue as int?;

                        if (item.Exception.EndsWith("Batch Partitions"))
                        {
                            //"Associated Item" -- Tables
                            //"Associated Value" -- nbr partitions
                            var strTables = item.AssocItem;

                            if (string.IsNullOrEmpty(strTables) || !assocValue.HasValue)
                            {
                                Program.ConsoleWarnings.Increment("Missing Table(s) or invalid partition value...");
                                Logger.Dump(string.Format("Missing Table(s) \"{0}\" or invalid partition value of \"{1}\" for IP {1}",
                                                            strTables,
                                                            item.AssocValue,
                                                            ipAddress), Logger.DumpType.Warning);
                            }
                            else
                            {
                                var keyTbls = strTables.Split(',')
                                                .Select(kytblName => kytblName.Trim())
                                                .Select(kytblName =>
                                                    {
                                                        var kytbl = SplitTableName(kytblName);
                                                        return new { Keyspace = kytbl.Item1, Table = kytbl.Item2 };
                                                    });
                                foreach (var keyTbl in keyTbls)
                                {
                                    if (ignoreKeySpaces.Contains(keyTbl.Keyspace))
                                    {
                                        continue;
                                    }

                                    var dataRow = dtCStatusLog.NewRow();

                                    dataRow["Timestamp"] = item.Timestamp;
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = item.Exception + " Count";
                                    dataRow["Reconciliation Reference"] = refCnt;
                                    dataRow["KeySpace"] = keyTbl.Keyspace;
                                    dataRow["Table"] = keyTbl.Table;
                                    dataRow["Completed"] = (long)assocValue.Value;

                                    dtCStatusLog.Rows.Add(dataRow);

                                    batchSizes.Add(new Tuple<string, string, string, long>(item.Exception + " Count", keyTbl.Keyspace, keyTbl.Table, assocValue.Value));

                                    refCnt += ReferenceIncrementValue;
                                }
                            }
                        }
                        else if (item.Exception == "Maximum Memory Reached cannot Allocate")
                        {
                            //"Associated Value" -- bytes
                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "Allocation Failed Maximum Memory Reached";
                            dataRow["Reconciliation Reference"] = refCnt;

                            if (assocValue.HasValue)
                            {
                                dataRow["Capacity (mb)"] = ((decimal)assocValue.Value) / BytesToMB;
                                maxMemoryAllocFailed.Add(assocValue.Value);
                            }
                            else
                            {
                                Program.ConsoleWarnings.Increment("Invalid Allocation Failed Maximum Memory Reached Value...");
                                Logger.Dump(new DataRow[] { dataRow }, Logger.DumpType.Warning, "Invalid Allocation Failed Maximum Memory Reached Value");
                            }

                            dtCStatusLog.Rows.Add(dataRow);
                        }

                        #endregion
                    }
                    else if (item.Item == "MessagingService.java")
                    {
                        #region MessagingService.java
                        //MessagingService.java --  MUTATION messages were dropped in last 5000 ms: 43 for internal timeout and 0 for cross node timeout

                        if (item.Exception == "Dropped Mutations")
                        {
                            var assocValue = item.AssocValue as int?;
                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "Dropped Mutation";
                            dataRow["Reconciliation Reference"] = refCnt;

                            dataRow["Completed"] = (long)assocValue.Value;
                            droppedMutations.Add(assocValue.Value);

                            dtCStatusLog.Rows.Add(dataRow);
                        }

                        #endregion
                    }
                    else if (item.Item == "AbstractSolrSecondaryIndex.java")
                    {
                        #region AbstractSolrSecondaryIndex
                        //Finished reindexing on keyspace prod_fcra and column family rtics_contribution
                        //Reindexing on keyspace prod_fcra and column family rtics_contribution
                        #region ReIndex/Finish
                        var regExSolrReIndex = RegExSolrReIndex.Split(item.Description);

                        if (regExSolrReIndex.Length > 2)
                        {
                            if (ignoreKeySpaces.Contains(regExSolrReIndex[1].Trim()))
                            {
                                continue;
                            }

                            if (item.Description.TrimStart().StartsWith("Finish"))
                            {
                                var solrReIndx = solrReindexing.LastOrDefault();

                                if (solrReIndx != null)
                                {
                                    regExSolrReIndex[1] = regExSolrReIndex[1].Trim();
                                    regExSolrReIndex[2] = regExSolrReIndex[2].Trim();

                                    if (!(solrReIndx.Keyspace == regExSolrReIndex[1]
                                            && solrReIndx.Table == regExSolrReIndex[2]
                                            && solrReIndx.CompletionTime == DateTime.MinValue))
                                    {
                                        solrReIndx = solrReindexing
                                                        .Find(s => s.Keyspace == regExSolrReIndex[1]
                                                                        && s.Table == regExSolrReIndex[2]
                                                                        && s.CompletionTime == DateTime.MinValue);
                                    }

                                    if (solrReIndx != null)
                                    {
                                        solrReIndx.CompletionTime = item.Timestamp;
                                        solrReIndx.GroupIndicator = refCnt;

                                        var dataRow = dtCStatusLog.NewRow();

                                        dataRow["Timestamp"] = item.Timestamp;
                                        dataRow["Data Center"] = dcName;
                                        dataRow["Node IPAddress"] = ipAddress;
                                        dataRow["Pool/Cache Type"] = "Solr ReIndex Finish";
                                        dataRow["KeySpace"] = solrReIndx.Keyspace;
                                        dataRow["Table"] = solrReIndx.Table;
                                        dataRow["Reconciliation Reference"] = refCnt;
                                        dataRow["Latency (ms)"] = solrReIndx.Duration;

                                        dtCStatusLog.Rows.Add(dataRow);
                                    }
                                }
                            }
                            else
                            {
                                var solrReIndx = new SolrReIndexingLogInfo()
                                {
                                    StartTime = item.Timestamp,
                                    DataCenter = dcName,
                                    IPAddress = ipAddress,
                                    Keyspace = regExSolrReIndex[1].Trim(),
                                    Table = regExSolrReIndex[2].Trim(),
                                    GroupIndicator = refCnt
                                };

                                solrReindexing.Add(solrReIndx);
                                SolrReindexingOccurrences.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
                                    ignore =>
                                    {
                                        return new Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo>() { solrReIndx };
                                    },
                                    (ignore, gcList) =>
                                    {
                                        gcList.Add(solrReIndx);
                                        return gcList;
                                    });
                            }
                        }
                        #endregion

                        //Reindexing 1117 commit log updates for core prod_fcra.bics_contribution
                        #region ReIndex Core
                        regExSolrReIndex = RegExSolrReIndex1.Split(item.Description);

                        if (regExSolrReIndex.Length > 2)
                        {
                            if (ignoreKeySpaces.Contains(regExSolrReIndex[2].Trim()))
                            {
                                continue;
                            }

                            var solrReIndx = new SolrReIndexingLogInfo()
                            {
                                StartTime = item.Timestamp,
                                DataCenter = dcName,
                                IPAddress = ipAddress,
                                Keyspace = regExSolrReIndex[2].Trim(),
                                Table = regExSolrReIndex[3].Trim(),
                                NbrUpdates = int.Parse(regExSolrReIndex[1]),
                                GroupIndicator = refCnt
                            };

                            solrReindexing.Add(solrReIndx);
                            SolrReindexingOccurrences.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
                                ignore =>
                                {
                                    return new Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo>() { solrReIndx };
                                },
                                (ignore, gcList) =>
                                {
                                    gcList.Add(solrReIndx);
                                    return gcList;
                                });
                        }

                        #endregion

                        //Executing hard commit on index prod_fcra.bics_contribution
                        //Truncated commit log for core prod_fcra.bics_contribution
                        #region Hard Commit
                        regExSolrReIndex = RegExSolrReIndex2.Split(item.Description);

                        if (regExSolrReIndex.Length == 0)
                        {
                            regExSolrReIndex = RegExSolrReIndex3.Split(item.Description);
                        }

                        if (regExSolrReIndex.Length > 1)
                        {
                            if (ignoreKeySpaces.Contains(regExSolrReIndex[1].Trim()))
                            {
                                continue;
                            }

                            var solrReIndx = solrReindexing.LastOrDefault();

                            if (solrReIndx != null)
                            {
                                regExSolrReIndex[1] = regExSolrReIndex[1].Trim();
                                regExSolrReIndex[2] = regExSolrReIndex[2].Trim();

                                if (!(solrReIndx.Keyspace == regExSolrReIndex[1]
                                        && solrReIndx.Table == regExSolrReIndex[2]
                                        && solrReIndx.CompletionTime == DateTime.MinValue))
                                {
                                    solrReIndx = solrReindexing
                                                    .Find(s => s.Keyspace == regExSolrReIndex[1]
                                                                    && s.Table == regExSolrReIndex[2]
                                                                    && s.CompletionTime == DateTime.MinValue);
                                }

                                if (solrReIndx != null)
                                {
                                    solrReIndx.CompletionTime = item.Timestamp;
                                    solrReIndx.GroupIndicator = refCnt;

                                    var dataRow = dtCStatusLog.NewRow();

                                    dataRow["Timestamp"] = item.Timestamp;
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = "Solr ReIndex Finish";
                                    dataRow["KeySpace"] = solrReIndx.Keyspace;
                                    dataRow["Table"] = solrReIndx.Table;
                                    dataRow["Reconciliation Reference"] = refCnt;
                                    dataRow["Latency (ms)"] = solrReIndx.Duration;
                                    dtCStatusLog.Rows.Add(dataRow);
                                }
                            }
                        }

                        #endregion

                        #endregion
                    }
                    else if (item.Task == "MemtablePostFlush" || item.Task == "StreamReceiveTask")
                    {
                        #region MemtablePostFlush
                        //INFO [MemtablePostFlush:22429] 2017-05-19 08:51:41,572  ColumnFamilyStore.java:1006 - Flushing SecondaryIndex Cql3SolrSecondaryIndex{columnDefs=[ColumnDefinition{name=appownrtxt, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_appownrtxt_index, indexType=CUSTOM},
                        //INFO [MemtablePostFlush:22429] 2017-05-19 08:51:41,573  AbstractSolrSecondaryIndex.java:1133 - Executing hard commit on index coafstatim.application
                        //INFO [MemtablePostFlush:22429] 2017-05-19 08:51:41,573  IndexWriter.java:3429 - commitInternalStart startTime = 1495198301573
                        //INFO [MemtablePostFlush:22429] 2017-05-19 08:51:41,770  IndexWriter.java:3454 - commitInternalComplete duration = 197 ms startTime = 1495198301573

                        /* DSE 5.X
                         INFO  [StreamReceiveTask:832] 2017-06-29 10:18:31,883  SecondaryIndexManager.java:359 - Submitting index build of ckspsocp1_pymtsocp_solr_query_index for data in BigTableReader(path='/apps/cassandra/data/data2/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21290-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data1/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21291-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data1/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21292-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data2/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21293-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data1/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21294-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data1/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21295-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data2/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21296-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data1/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21297-big-Data.db'),BigTableReader(path='/apps/cassandra/data/data1/ckspsocp1/pymtsocp-17b3c431c42511e6b6a39d750c224e11/mc-21298-big-Data.db')
                         INFO  [StreamReceiveTask:832] 2017-06-29 10:18:32,097  AbstractSolrSecondaryIndex.java:1183 - Executing hard commit on index ckspsocp1.pymtsocp
                         INFO  [StreamReceiveTask:832] 2017-06-29 10:18:32,101  IndexWriter.java:3463 - commitInternalStart startTime=1498756712101
                         INFO  [StreamReceiveTask:832] 2017-06-29 10:18:32,878  IndexWriter.java:3488 - commitInternalComplete duration=777 ms startTime=1498756712101
                         */

                        if ((item.Item == "ColumnFamilyStore.java"
                                && item.Description.StartsWith("Flushing SecondaryIndex Cql3SolrSecondaryIndex"))
                            || (item.Item == "SecondaryIndexManager.java"
                                    && item.Description.StartsWith("Submitting index build")))
                        {
                            #region Flush Solr Index
                            var ksTbls = item.AssocItem.Split(',')
                                            .Select(n => kstblExists.FirstOrDefault(i => i.SolrIndexName == n.Trim()))
                                            .Where(i => i != null && !ignoreKeySpaces.Any(k => k == i.KeySpaceName))
                                            .Select(i => new Tuple<string, string>(i.KeySpaceName, i.Name));
                            var keySpaces = ksTbls.Select(i => i.Item1).DuplicatesRemoved(i => i);

                            if (ksTbls.IsEmpty())
                            {
                                continue;
                            }

                            var newSolrCommit = new SolrHardCommitLogInfo()
                            {
                                DataCenter = dcName,
                                IPAddress = ipAddress,
                                StartTime = item.Timestamp,
                                TaskId = item.TaskId.Value,
                                SolrIndexes = ksTbls,
                                Keyspace = keySpaces.IsMultiple() ? null : keySpaces.First()
                            };

                            solrHardCommit.Add(newSolrCommit);
                            SolrHardCommits.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
                                ignore =>
                                {
                                    return new Common.Patterns.Collections.ThreadSafe.List<SolrHardCommitLogInfo>() { newSolrCommit };
                                },
                                (ignore, hcList) =>
                                {
                                    hcList.Add(newSolrCommit);
                                    return hcList;
                                });
                            #endregion
                        }
                        else if (item.Item == "IndexWriter.java")
                        {
                            var regExTime = RegExSolrHardCommitInternalTime.Match(item.Description);

                            if (regExTime.Success)
                            {
                                if (item.Description.StartsWith("commitInternalStart "))
                                {
                                    #region commitInternalStart
                                    var startEvent = solrHardCommit.LastOrDefault(e => e.TaskId == item.TaskId);

                                    if (startEvent != null)
                                    {
                                        startEvent.InternalStartTime = long.Parse(regExTime.Groups[2].Value);
                                    }
                                    #endregion
                                }
                                else
                                {
                                    #region commitInternalEnd
                                    var startTime = long.Parse(regExTime.Groups[2].Value);
                                    var endEvent = solrHardCommit.LastOrDefault(e => e.TaskId == item.TaskId
                                                                                        && e.InternalStartTime == startTime);

                                    if (endEvent != null)
                                    {
                                        endEvent.CompletionTime = item.Timestamp;
                                        endEvent.GroupIndicator = refCnt;

                                        foreach (var indexName in endEvent.SolrIndexes)
                                        {
                                            var dataRow = dtCStatusLog.NewRow();

                                            dataRow["Timestamp"] = item.Timestamp;
                                            dataRow["Data Center"] = dcName;
                                            dataRow["Node IPAddress"] = ipAddress;
                                            dataRow["Pool/Cache Type"] = "Solr Hard Commit Finish";
                                            dataRow["KeySpace"] = indexName.Item1;
                                            dataRow["Table"] = indexName.Item2;
                                            dataRow["Reconciliation Reference"] = refCnt;
                                            dataRow["Latency (ms)"] = endEvent.Duration;

                                            dtCStatusLog.Rows.Add(dataRow);
                                        }
                                    }
                                    #endregion
                                }
                            }
                        }

                        #endregion
                    }
                    else if (item.Item == "MigrationManager.java")
                    {
                        #region schema changes
                        var changeMode = item.Exception[0];
                        var ksItem = item.AssocItem == null ? true : !item.AssocItem.Contains('.');
                        string keySpace = ksItem ? item.AssocItem : null;
                        string table = null;

                        if (!ksItem)
                        {
                            var kstblItem = SplitTableName(item.AssocItem);

                            keySpace = kstblItem.Item1;
                            table = kstblItem.Item2;
                        }

                        detectedSchemaChanges.Add(new Tuple<char, string, string>(changeMode, keySpace, table));
                        #endregion
                    }
                    else if (item.Item == "ShardRouter.java")
                    {
                        #region ShardRouter.java (shard change)
                        shardStateChanges.Add((string)item.AssocValue);
                        #endregion
                    }
                }

                #region Add TP/CF Stats Info

                if (dtTPStats != null)
                {
                    initializeTPStatsDataTable(dtTPStats);

                    #region gcLatencies

                    gcLatencies.RemoveAll(x => x <= 0);

                    if (gcLatencies.Count > 0)
                    {
                        Logger.Instance.InfoFormat("Adding GC Latencies ({2}) to TPStats for \"{0}\" \"{1}\"", dcName, ipAddress, gcLatencies.Count);

                        var gcMax = gcLatencies.Max();
                        var gcMin = gcLatencies.Min();
                        var gcAvg = (int)gcLatencies.Average();

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC minimum latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = gcMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC maximum latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = gcMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC mean latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = gcAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC occurrences";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Occurrences"] = gcLatencies.Count;

                        dtTPStats.Rows.Add(dataRow);
                    }

                    #endregion

                    #region tpSlowQueries

                    tpSlowQueries.RemoveAll(x => x <= 0);

                    if (tpSlowQueries.Count > 0)
                    {
                        Logger.Instance.InfoFormat("Adding Slow Quries ({2}) to TPStats for \"{0}\" \"{1}\"", dcName, ipAddress, tpSlowQueries.Count);

                        var slowMax = tpSlowQueries.Max();
                        var slowMin = tpSlowQueries.Min();
                        var slowAvg = (int)tpSlowQueries.Average();

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query minimum latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = slowMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query maximum latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = slowMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query mean latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = slowAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query occurrences";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Occurrences"] = tpSlowQueries.Count;

                        dtTPStats.Rows.Add(dataRow);
                    }

                    #endregion

                    #region tpStatusCounts

                    tpStatusCounts.RemoveAll(x => x.Item2 == 0 && x.Item3 == 0 && x.Item4 == 0 && x.Item5 == 0 && x.Item6 == 0);

                    if (tpStatusCounts.Count > 0)
                    {
                        Logger.Instance.InfoFormat("Adding Pool Items ({2}) to TPStats for \"{0}\" \"{1}\"", dcName, ipAddress, tpStatusCounts.Count);

                        var tpItems = from tpItem in tpStatusCounts
                                      group tpItem by new { tpItem.Item1 }
                                                  into g
                                      select new
                                      {
                                          Item1 = g.Key.Item1,
                                          maxItem2 = g.Max(s => s.Item2),
                                          maxItem3 = g.Max(s => s.Item3),
                                          maxItem4 = g.Max(s => s.Item4),
                                          maxItem5 = g.Max(s => s.Item5),
                                          maxItem6 = g.Max(s => s.Item6),

                                          minItem2 = g.Min(s => s.Item2),
                                          minItem3 = g.Min(s => s.Item3),
                                          minItem4 = g.Min(s => s.Item4),
                                          minItem5 = g.Min(s => s.Item5),
                                          minItem6 = g.Min(s => s.Item6),

                                          avgItem2 = (long)g.Average(s => s.Item2),
                                          avgItem3 = (long)g.Average(s => s.Item3),
                                          avgItem4 = (long)g.Average(s => s.Item4),
                                          avgItem5 = (long)g.Average(s => s.Item5),
                                          avgItem6 = (long)g.Average(s => s.Item6),

                                          totItem2 = g.Sum(s => s.Item2),
                                          totItem3 = g.Sum(s => s.Item3),
                                          totItem4 = g.Sum(s => s.Item4),
                                          totItem5 = g.Sum(s => s.Item5),
                                          totItem6 = g.Sum(s => s.Item6),

                                          Count = g.Count()
                                      };

                        foreach (var tpItem in tpItems)
                        {
                            var dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = tpItem.Item1 + " maximum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Active"] = tpItem.maxItem2;
                            dataRow["Pending"] = tpItem.maxItem3;
                            dataRow["Completed"] = tpItem.maxItem4;
                            dataRow["Blocked"] = tpItem.maxItem5;
                            dataRow["All time blocked"] = tpItem.maxItem6;

                            dtTPStats.Rows.Add(dataRow);

                            dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = tpItem.Item1 + " minimum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Active"] = tpItem.minItem2;
                            dataRow["Pending"] = tpItem.minItem3;
                            dataRow["Completed"] = tpItem.minItem4;
                            dataRow["Blocked"] = tpItem.minItem5;
                            dataRow["All time blocked"] = tpItem.minItem6;

                            dtTPStats.Rows.Add(dataRow);

                            dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = tpItem.Item1 + " mean";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Active"] = tpItem.avgItem2;
                            dataRow["Pending"] = tpItem.avgItem3;
                            dataRow["Completed"] = tpItem.avgItem4;
                            dataRow["Blocked"] = tpItem.avgItem5;
                            dataRow["All time blocked"] = tpItem.avgItem6;

                            dtTPStats.Rows.Add(dataRow);

                            dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = tpItem.Item1 + " Total";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Active"] = tpItem.totItem2;
                            dataRow["Pending"] = tpItem.totItem3;
                            dataRow["Completed"] = tpItem.totItem4;
                            dataRow["Blocked"] = tpItem.totItem5;
                            dataRow["All time blocked"] = tpItem.totItem6;

                            dtTPStats.Rows.Add(dataRow);

                            dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = tpItem.Item1 + " occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Occurrences"] = tpItem.Count;

                            dtTPStats.Rows.Add(dataRow);
                        }
                    }

                    #endregion

                    #region Pause

                    if (pauses.Count > 0)
                    {
                        Logger.Instance.InfoFormat("Adding Pause ({2}) to TPStats for \"{0}\" \"{1}\"", dcName, ipAddress, pauses.Count);

                        var gcMax = (int)pauses.Max();
                        var gcMin = (int)pauses.Min();
                        var gcAvg = (int)pauses.Average();

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause minimum latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = gcMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause maximum latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = gcMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause mean latency";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Latency (ms)"] = gcAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause occurrences";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Occurrences"] = pauses.Count;

                        dtTPStats.Rows.Add(dataRow);
                    }

                    #endregion

                    #region JVM

                    if (jvmFatalErrors.Count > 0)
                    {
                        var jvmItems = from jvmItem in jvmFatalErrors
                                       group jvmItem by jvmItem into g
                                       select new
                                       {
                                           item = g.Key,
                                           Count = g.Count()
                                       };

                        foreach (var jvmGrp in jvmItems)
                        {
                            var dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = RemoveNamespace(jvmGrp.item) + " occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Occurrences"] = jvmGrp.Count;

                            dtTPStats.Rows.Add(dataRow);
                        }
                    }

                    #endregion

                    #region WorkPool

                    if (workPoolErrors.Count > 0)
                    {
                        var wptems = from wpItem in workPoolErrors
                                     group wpItem by wpItem into g
                                     select new
                                     {
                                         item = g.Key,
                                         Count = g.Count()
                                     };

                        foreach (var wpGrp in wptems)
                        {
                            var dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = RemoveNamespace(wpGrp.item) + " occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Occurrences"] = wpGrp.Count;

                            dtTPStats.Rows.Add(dataRow);
                        }
                    }

                    #endregion

                    #region Node Status

                    if (nodeStatus.Count > 0)
                    {
                        var nodeStatusItems = from nodeItem in nodeStatus
                                              where string.IsNullOrEmpty(nodeItem.Item2)
                                              group nodeItem.Item1 by nodeItem.Item1 into g
                                              select new
                                              {
                                                  item = g.Key,
                                                  Count = g.Count()
                                              };

                        foreach (var statusGrp in nodeStatusItems)
                        {
                            var dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Attribute"] = statusGrp.item + " occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Occurrences"] = statusGrp.Count;

                            dtTPStats.Rows.Add(dataRow);
                        }
                    }

                    #endregion

                    #region Dropped Hints

                    if (droppedHints.Count > 0)
                    {
                        var droppedTotalNbr = droppedHints.Sum();
                        var droppedMaxNbr = droppedHints.Max();
                        var droppedMinNbr = droppedHints.Min();
                        var droppedAvgNbr = (int)droppedHints.Average();
                        var droppedOccurences = droppedHints.Count;

                        //Dropped Hints Total
                        //Dropped Hints maximum
                        //Dropped Hints mean
                        //Dropped Hints minimum
                        //Dropped Hints occurrences

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints Total";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedTotalNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints maximum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedMaxNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints mean";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedAvgNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints minimum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedMinNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints occurrences";
                        dataRow["Reconciliation Reference"] = grpInd;
                        //dataRow["Value"] = droppedMinNbr;
                        dataRow["Occurrences"] = droppedOccurences;

                        dtTPStats.Rows.Add(dataRow);
                    }

                    #endregion

                    #region Timed Out Hints

                    if (timedoutHints.Count > 0)
                    {
                        var timedoutTotalNbr = timedoutHints.Sum();
                        var timedoutMaxNbr = timedoutHints.Max();
                        var timedoutMinNbr = timedoutHints.Min();
                        var timedoutAvgNbr = (int)timedoutHints.Average();
                        var timedoutOccurences = timedoutHints.Count;

                        //Timedout Hints Total
                        //Timedout Hints maximum
                        //Timedout Hints mean
                        //Timedout Hints minimum
                        //Timedout Hints occurrences

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Timedout Hints Total";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Completed"] = timedoutTotalNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Timedout Hints maximum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Completed"] = timedoutMaxNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Timedout Hints mean";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Completed"] = timedoutAvgNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Timedout Hints minimum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Completed"] = timedoutMinNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Timedout Hints occurrences";
                        dataRow["Reconciliation Reference"] = grpInd;
                        //dataRow["Value"] = droppedMinNbr;
                        dataRow["Occurrences"] = timedoutOccurences;

                        dtTPStats.Rows.Add(dataRow);
                    }

                    #endregion

                    #region Allocation Failed Maximum Memory Reached

                    if (maxMemoryAllocFailed.Count > 0)
                    {
                        var allocTotalMem = maxMemoryAllocFailed.Sum();
                        var allocMaxMem = maxMemoryAllocFailed.Max();
                        var allocMinMem = maxMemoryAllocFailed.Min();
                        var allocAvgMem = (decimal)maxMemoryAllocFailed.Average();
                        var allocMemOccurences = maxMemoryAllocFailed.Count;

                        //Allocation Failed Maximum Memory Reached Total
                        //Allocation Failed Maximum Memory Reached maximum
                        //Allocation Failed Maximum Memory Reached minimum
                        //Allocation Failed Maximum Memory Reached mean
                        //Allocation Failed Maximum Memory Reached occurrences

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached Total";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Size (mb)"] = ((decimal)allocTotalMem) / BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached maximum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Size (mb)"] = ((decimal)allocMaxMem) / BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached mean";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Size (mb)"] = allocAvgMem / BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached minimum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Size (mb)"] = ((decimal)allocMinMem) / BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached occurrences";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Occurrences"] = allocMemOccurences;

                        dtTPStats.Rows.Add(dataRow);
                    }

                    #endregion

                    #region Dropped Mutations

                    if (droppedMutations.Count > 0)
                    {
                        var droppedTotalNbr = droppedMutations.Sum();
                        var droppedMaxNbr = droppedMutations.Max();
                        var droppedMinNbr = droppedMutations.Min();
                        var droppedAvgNbr = (int)droppedMutations.Average();
                        var droppedOccurences = droppedMutations.Count;

                        //Dropped Mutation Total
                        //Dropped Mutation maximum
                        //Dropped Mutation mean
                        //Dropped Mutation minimum
                        //Dropped Mutation occurrences

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation Total";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedTotalNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation maximum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedMaxNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation mean";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedAvgNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation minimum";
                        dataRow["Reconciliation Reference"] = grpInd;
                        dataRow["Dropped"] = droppedMinNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation occurrences";
                        dataRow["Reconciliation Reference"] = grpInd;
                        //dataRow["Value"] = droppedMinNbr;
                        dataRow["Occurrences"] = droppedOccurences;

                        dtTPStats.Rows.Add(dataRow);
                    }

                    #endregion

                    #region Shard Changes
                    {
                        var shardStats = from shardItem in shardStateChanges
                                         group shardItem by shardItem
                                              into g
                                         select new
                                         {
                                             ShardType = g.Key,
                                             Count = g.Count()
                                         };

                        foreach (var statItem in shardStats)
                        {
                            var dataRow = dtTPStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            if (statItem.ShardType == "dead")
                            {
                                dataRow["Attribute"] = "Shard State Change Dead Node";
                            }
                            else
                            {
                                dataRow["Attribute"] = "Shard State Change Schema";
                            }
                            dataRow["Reconciliation Reference"] = grpInd;
                            //dataRow["Completed"] = shardStateChanges;
                            dataRow["Occurrences"] = statItem.Count;

                            dtTPStats.Rows.Add(dataRow);
                        }
                    }
                    #endregion
                }

                if (dtCFStats != null)
                {
                    if (compactionLatencies.Count > 0)
                    {
                        #region compactionLatencies

                        initializeCFStatsDataTable(dtCFStats);

                        compactionLatencies.RemoveAll(x => x.Item3 <= 0);

                        if (compactionLatencies.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding Compaction Latencies ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, compactionLatencies.Count);

                            var compStats = from cmpItem in compactionLatencies
                                            group cmpItem by new { cmpItem.Item1, cmpItem.Item2 }
                                              into g
                                            select new
                                            {
                                                KeySpace = g.Key.Item1,
                                                Table = g.Key.Item2,
                                                Max = g.Max(s => s.Item3),
                                                Min = g.Min(s => s.Item3),
                                                Avg = (int)g.Average(s => s.Item3),
                                                Count = g.Count()
                                            };

                            foreach (var statItem in compStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Compaction maximum latency";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Max;
                                dataRow["(Value)"] = statItem.Max;
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Compaction minimum latency";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Min;
                                dataRow["(Value)"] = statItem.Min;
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Compaction mean latency";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Avg;
                                dataRow["(Value)"] = statItem.Avg;
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Compaction occurrences";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;
                                //dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }
                        #endregion
                    }

                    if (compactionLatencies.Count > 0)
                    {
                        #region compactionRates

                        initializeCFStatsDataTable(dtCFStats);

                        compactionRates.RemoveAll(x => x.Item3 <= 0);

                        if (compactionRates.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding Compaction Rates ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, compactionRates.Count);

                            var compStats = from cmpItem in compactionRates
                                            group cmpItem by new { cmpItem.Item1, cmpItem.Item2 }
                                              into g
                                            select new
                                            {
                                                KeySpace = g.Key.Item1,
                                                Table = g.Key.Item2,
                                                Max = g.Max(s => s.Item3),
                                                Min = g.Min(s => s.Item3),
                                                Avg = g.Average(s => s.Item3),
                                                Count = g.Count()
                                            };

                            foreach (var statItem in compStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Compaction maximum rate";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Max;
                                dataRow["(Value)"] = statItem.Max;
                                dataRow["Unit of Measure"] = "mb/sec";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Compaction minimum rate";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Min;
                                dataRow["(Value)"] = statItem.Min;
                                dataRow["Unit of Measure"] = "mb/sec";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Compaction mean rate";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Avg;
                                dataRow["(Value)"] = statItem.Avg;
                                dataRow["Unit of Measure"] = "mb/sec";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }
                        #endregion
                    }

                    if (partitionLargeSizes.Count > 0)
                    {
                        #region partitionLargeSizes

                        initializeCFStatsDataTable(dtCFStats);

                        partitionLargeSizes.RemoveAll(x => x.Item3 <= 0);

                        if (partitionLargeSizes.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding Partition Sizes ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, partitionLargeSizes.Count);

                            var compStats = from cmpItem in partitionLargeSizes
                                            group cmpItem by new { cmpItem.Item1, cmpItem.Item2 }
                                              into g
                                            select new
                                            {
                                                KeySpace = g.Key.Item1,
                                                Table = g.Key.Item2,
                                                Max = g.Max(s => s.Item3),
                                                Min = g.Min(s => s.Item3),
                                                Avg = (decimal)g.Average(s => s.Item3),
                                                Count = g.Count()
                                            };

                            foreach (var statItem in compStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Partition large maximum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = (long)(statItem.Max * BytesToMB);
                                dataRow["Size in MB"] = statItem.Max;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Partition large minimum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = (long)(statItem.Min * BytesToMB);
                                dataRow["Size in MB"] = statItem.Min;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Partition large mean";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = (long)(statItem.Avg * BytesToMB);
                                dataRow["Size in MB"] = statItem.Avg;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "Partition large occurrences";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }

                        #endregion
                    }

                    if (tombstoneCounts.Count > 0)
                    {
                        #region tombstoneCounts

                        initializeCFStatsDataTable(dtCFStats);

                        tombstoneCounts.RemoveAll(x => x.Item4 <= 0);

                        if (tombstoneCounts.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding Tombstone Counts ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, tombstoneCounts.Count);

                            var compStats = from cmpItem in tombstoneCounts
                                            group cmpItem by new { cmpItem.Item1, cmpItem.Item2, cmpItem.Item3 }
                                              into g
                                            select new
                                            {
                                                Attr = g.Key.Item1,
                                                KeySpace = g.Key.Item2,
                                                Table = g.Key.Item3,
                                                Total = g.Sum(s => s.Item4),
                                                Max = g.Max(s => s.Item4),
                                                Min = g.Min(s => s.Item4),
                                                Avg = (int)g.Average(s => s.Item4),
                                                Count = g.Count()
                                            };

                            foreach (var statItem in compStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " Total";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Total;
                                dataRow["(value)"] = statItem.Total;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " maximum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Max;
                                dataRow["(value)"] = statItem.Max;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " minimum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Min;
                                dataRow["(Value)"] = statItem.Min;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " mean";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Avg;
                                dataRow["(Value)"] = statItem.Avg;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " occurrences";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }

                        #endregion
                    }

                    if (statusMemTables.Count > 0)
                    {
                        #region statusMemTables

                        initializeCFStatsDataTable(dtCFStats);

                        statusMemTables.RemoveAll(x => x.Item3 <= 0 && x.Item4 <= 0);

                        if (statusMemTables.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding MemTables Stats ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, statusMemTables.Count);

                            var memtblStats = from cmpItem in statusMemTables
                                              group cmpItem by new { cmpItem.Item1, cmpItem.Item2 }
                                                  into g
                                              select new
                                              {
                                                  KeySpace = g.Key.Item1,
                                                  Table = g.Key.Item2,
                                                  maxItem3 = g.Max(s => s.Item3),
                                                  minItem3 = g.Min(s => s.Item3),
                                                  avgItem3 = (long)g.Average(s => s.Item3),
                                                  maxItem4 = g.Max(s => s.Item4),
                                                  minItem4 = g.Min(s => s.Item4),
                                                  avgItem4 = g.Average(s => s.Item4),
                                                  Count = g.Count()
                                              };

                            foreach (var statItem in memtblStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Write OPS maximum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.maxItem3;
                                dataRow["(value)"] = statItem.maxItem3;
                                dataRow["Unit of Measure"] = "Operations per Second";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Write OPS minimum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.minItem3;
                                dataRow["(value)"] = statItem.minItem3;
                                dataRow["Unit of Measure"] = "Operations per Second";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Write OPS mean";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.avgItem3;
                                dataRow["(value)"] = statItem.avgItem3;
                                dataRow["Unit of Measure"] = "Operations per Second";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Write occurrences";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;

                                dtCFStats.Rows.Add(dataRow);

                                //Size
                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Write Size maximum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = (long)(statItem.maxItem4 * BytesToMB);
                                dataRow["Size in MB"] = statItem.maxItem4;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Write Size minimum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = (long)(statItem.minItem4 * BytesToMB);
                                dataRow["Size in MB"] = statItem.minItem4;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Write Size mean";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = (long)(statItem.avgItem4 * BytesToMB);
                                dataRow["Size in MB"] = statItem.avgItem4;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }

                        #endregion
                    }

                    if (batchSizes.Count > 0)
                    {
                        #region batcheSizes

                        initializeCFStatsDataTable(dtCFStats);

                        batchSizes.RemoveAll(x => x.Item4 <= 0);

                        if (batchSizes.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding Batch Sizes ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, batchSizes.Count);

                            var compStats = from cmpItem in batchSizes
                                            group cmpItem by new { cmpItem.Item1, cmpItem.Item2, cmpItem.Item3 }
                                              into g
                                            select new
                                            {
                                                Attr = g.Key.Item1,
                                                KeySpace = g.Key.Item2,
                                                Table = g.Key.Item3,
                                                Total = g.Sum(s => s.Item4),
                                                Max = g.Max(s => s.Item4),
                                                Min = g.Min(s => s.Item4),
                                                Avg = (int)g.Average(s => s.Item4),
                                                Count = g.Count()
                                            };

                            foreach (var statItem in compStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " Total";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Total;
                                dataRow["(value)"] = statItem.Total;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " maximum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Min;
                                dataRow["(Value)"] = statItem.Min;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " minimum";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Min;
                                dataRow["(Value)"] = statItem.Min;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " mean";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Avg;
                                dataRow["(Value)"] = statItem.Avg;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = statItem.Attr + " occurrences";
                                dataRow["Reconciliation Reference"] = grpInd;
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }

                        #endregion
                    }

                    if (nodeStatus.Count > 0)
                    {
                        #region Node Status
                        initializeCFStatsDataTable(dtCFStats);

                        var nodeStatusItems = from nodeItem in nodeStatus
                                              where !string.IsNullOrEmpty(nodeItem.Item2)
                                              group nodeItem by new { nodeItem.Item1, nodeItem.Item2, nodeItem.Item3 } into g
                                              select new
                                              {
                                                  item = g.Key.Item1,
                                                  KeySpace = g.Key.Item2,
                                                  Table = g.Key.Item3,
                                                  Count = g.Count()
                                              };

                        foreach (var statusGrp in nodeStatusItems)
                        {
                            var dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = statusGrp.item + " occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Count;
                            dataRow["(value)"] = statusGrp.Count;

                            dtCFStats.Rows.Add(dataRow);
                        }

                        #endregion
                    }

                    if (solrReindexing.Count > 0)
                    {
                        #region Solr ReIndexing
                        initializeCFStatsDataTable(dtCFStats);

                        var solrReIdxItems = from solrItem in solrReindexing
                                             where solrItem.Duration > 0
                                             group solrItem by new { solrItem.Keyspace, solrItem.Table } into g
                                             select new
                                             {
                                                 KeySpace = g.Key.Keyspace,
                                                 Table = g.Key.Table,
                                                 Count = g.Count(),
                                                 Max = g.Max(i => i.Duration),
                                                 Min = g.Min(i => i.Duration),
                                                 Avg = g.Average(i => i.Duration),
                                                 Std = g.Select(i => i.Duration).StandardDeviationP()
                                             };

                        foreach (var statusGrp in solrReIdxItems)
                        {
                            var dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr reindex duration maximum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Max;
                            dataRow["(value)"] = statusGrp.Max;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr reindex duration minimum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Min;
                            dataRow["(value)"] = statusGrp.Min;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr reindex duration mean";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Avg;
                            dataRow["(value)"] = statusGrp.Avg;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr reindex duration stdev";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Std;
                            dataRow["(value)"] = statusGrp.Std;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr reindex occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Count;
                            dataRow["(value)"] = statusGrp.Count;

                            dtCFStats.Rows.Add(dataRow);
                        }

                        #endregion
                    }

                    if (solrHardCommit.Count > 0)
                    {
                        #region Solr Hard Commit
                        initializeCFStatsDataTable(dtCFStats);

                        var solrHrdCmttems = from solrItem in solrHardCommit
                                                                .SelectMany(s => s.SolrIndexes
                                                                                    .Select(i => new
                                                                                    {
                                                                                        Keyspace = i.Item1,
                                                                                        Index = i.Item2,
                                                                                        Duration = s.Duration
                                                                                    }))
                                             where solrItem.Duration > 0
                                             group solrItem by new { solrItem.Keyspace, solrItem.Index } into g
                                             select new
                                             {
                                                 KeySpace = g.Key.Keyspace,
                                                 Table = g.Key.Index,
                                                 Count = g.Count(),
                                                 Max = g.Max(i => i.Duration),
                                                 Min = g.Min(i => i.Duration),
                                                 Avg = g.Average(i => i.Duration),
                                                 Std = g.Select(i => i.Duration).StandardDeviationP()
                                             };

                        foreach (var statusGrp in solrHrdCmttems)
                        {
                            var dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr Hard Commit duration maximum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Max;
                            dataRow["(value)"] = statusGrp.Max;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr Hard Commit duration minimum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Min;
                            dataRow["(value)"] = statusGrp.Min;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr Hard Commit duration mean";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Avg;
                            dataRow["(value)"] = statusGrp.Avg;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr Hard Commit duration stdev";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Std;
                            dataRow["(value)"] = statusGrp.Std;
                            dataRow["Unit of Measure"] = "ms";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Solr Hard Commit occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Count;
                            dataRow["(value)"] = statusGrp.Count;

                            dtCFStats.Rows.Add(dataRow);
                        }

                        #endregion
                    }

                    if (detectedSchemaChanges.Count > 0)
                    {
                        #region schema changes
                        initializeCFStatsDataTable(dtCFStats);

                        var nodeSchemaChangeItem = from schemaItem in detectedSchemaChanges
                                                   group schemaItem by new { schemaItem.Item1, schemaItem.Item2, schemaItem.Item3 } into g
                                                   select new
                                                   {
                                                       chgType = g.Key.Item1,
                                                       KeySpace = g.Key.Item2,
                                                       Table = g.Key.Item3,
                                                       Count = g.Count()
                                                   };

                        foreach (var statusGrp in nodeSchemaChangeItem)
                        {
                            var dataRow = dtCFStats.NewRow();
                            string changeType = string.Empty;

                            switch (statusGrp.chgType)
                            {
                                case 'C':
                                    changeType = "Created";
                                    break;
                                case 'D':
                                    changeType = "Dropped";
                                    break;
                                case 'U':
                                    changeType = "Updated";
                                    break;
                            }

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statusGrp.KeySpace;
                            dataRow["Table"] = statusGrp.Table;
                            dataRow["Attribute"] = "Schema " + changeType + " occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statusGrp.Count;
                            dataRow["(value)"] = statusGrp.Count;

                            dtCFStats.Rows.Add(dataRow);
                        }

                        #endregion
                    }

                    if (memTblStats.Count > 0)
                    {
                        #region MemTableStats Delta

                        initializeCFStatsDataTable(dtCFStats);

                        Logger.Instance.InfoFormat("Adding MemTable Write Stats ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, memTblStats.Count);

                        var memtblStats = from cmpItem in memTblStats
                                          group cmpItem by new { cmpItem.Keyspace, cmpItem.Table }
                                                into g
                                          let difs = (g.First().InitialOPS == 0 ? g : g.Skip(1))
                                                      .Select(s => new { opsDif = s.EndingOPS - s.InitialOPS, sizeDif = s.EndingSize - s.InitialSize })
                                                      .Where(s => s.opsDif > 0)
                                          where difs.HasAtLeastOneElement()
                                          select new
                                          {
                                              KeySpace = g.Key.Keyspace,
                                              Table = g.Key.Table,
                                              maxOPS = difs.Max(s => s.opsDif),
                                              minOPS = difs.Min(s => s.opsDif),
                                              avgOPS = (long)difs.Average(s => s.opsDif),
                                              maxSize = difs.Max(s => s.sizeDif),
                                              minSize = difs.Min(s => s.sizeDif),
                                              avgSize = difs.Average(s => s.sizeDif),
                                              sumSize = difs.Sum(s => s.sizeDif),
                                              Count = g.Count()
                                          };

                        foreach (var statItem in memtblStats)
                        {
                            var dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write OPS Delta maximum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.maxOPS;
                            dataRow["(value)"] = statItem.maxOPS;
                            dataRow["Unit of Measure"] = "Operations per Second";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write OPS Delta minimum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.minOPS;
                            dataRow["(value)"] = statItem.minOPS;
                            dataRow["Unit of Measure"] = "Operations per Second";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write OPS Delta mean";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.avgOPS;
                            dataRow["(value)"] = statItem.avgOPS;
                            dataRow["Unit of Measure"] = "Operations per Second";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write Delta occurrences";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.Count;
                            dataRow["(Value)"] = statItem.Count;

                            dtCFStats.Rows.Add(dataRow);

                            //Size
                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write Size Delta maximum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.maxSize;
                            dataRow["Size in MB"] = (decimal)statItem.avgSize / BytesToMB;
                            dataRow["Unit of Measure"] = "bytes";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write Size Delta minimum";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.minSize;
                            dataRow["Size in MB"] = (decimal)statItem.minSize / BytesToMB;
                            dataRow["Unit of Measure"] = "bytes";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write Size Delta mean";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.avgSize;
                            dataRow["Size in MB"] = (decimal)statItem.avgSize / BytesToMB;
                            dataRow["Unit of Measure"] = "bytes";

                            dtCFStats.Rows.Add(dataRow);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = statItem.KeySpace;
                            dataRow["Table"] = statItem.Table;
                            dataRow["Attribute"] = "MemTable Write Size Delta Total";
                            dataRow["Reconciliation Reference"] = grpInd;
                            dataRow["Value"] = statItem.sumSize;
                            dataRow["Size in MB"] = (decimal)statItem.sumSize / BytesToMB;
                            dataRow["Unit of Measure"] = "bytes";

                            dtCFStats.Rows.Add(dataRow);
                        }
                        #endregion
                    }
                }

                #endregion


            }
        }

        class GCContinuousInfo
        {
            /// <summary>
            /// The change in the Java Pool space. Positive number means the pool increased, a negative number decrease.
            /// </summary>
            public struct SpaceChanged
            {
                public decimal Eden;
                public decimal Survivor;
                public decimal Old;
            }

            public string Node;
            public List<int> Latencies;
            public List<object> GroupRefIds;
            public List<DateTime> Timestamps;
            public List<SpaceChanged> GCSpacePoolChanges;
            public string Type;
            public decimal Percentage;
            public bool Deleted = false;
        };

        /// <summary>
        /// Log Timestamp, DC|Node, Keyspace.Table, Component (e.g., GC, flushing, etc.), nbr of detected items
        /// </summary>
        static Common.Patterns.Collections.ThreadSafe.List<Tuple<DateTime, string, string, string, int>> ComponentDisabled = new Common.Patterns.Collections.ThreadSafe.List<Tuple<DateTime, string, string, string, int>>();
        public static void DetectContinuousGCIntoNodeStats(DataTable dtNodeStats,
                                                                DataTable dtLog,
                                                                int overlapToleranceInMS,
                                                                int overlapContinuousGCNbrInSeries,
                                                                TimeSpan gcTimeframeDetection,
                                                                decimal gcDetectionPercent)
        {
            if (dtNodeStats == null)
            {
                return;
            }

            var gcList = new Common.Patterns.Collections.ThreadSafe.List<GCContinuousInfo>();

            Logger.Instance.InfoFormat("Begin Detecting Continuous (tolerance {0}, series {1})/Timeframe (timeframe {2}, percentage {3}) GCs for {4}",
                                            overlapToleranceInMS,
                                            overlapContinuousGCNbrInSeries,
                                            gcTimeframeDetection,
                                            gcDetectionPercent,
                                            string.Join(", ", GCOccurrences.Select(g => string.Format("{0} nbr GC events {1}", g.Key, g.Value.Count))));

            System.Threading.Tasks.Parallel.ForEach(GCOccurrences.UnSafe, gcInfo =>
            //foreach (var gcInfo in GCOccurrences.UnSafe)
            {
                IEnumerable<GCLogInfo> gcInfoTimeLine = gcInfo.Value.OrderBy(item => item.LogTimestamp);
                var gcInfoTimeLineCnt = gcInfoTimeLine.Count();
                bool redueGCTimeFrameAnalysis = ParserSettings.GCComplexReduceAnalysisOverEvents > 0 && gcInfoTimeLineCnt > ParserSettings.GCComplexReduceAnalysisOverEvents;

                if (redueGCTimeFrameAnalysis)
                {

                    if (ParserSettings.GCComplexAnalysisTakeEventsWhenOver <= 0 || ParserSettings.GCComplexAnalysisTakeEventsWhenOver > ParserSettings.GCComplexReduceAnalysisOverEvents)
                    {
                        Logger.Instance.WarnFormat("GC Timeframe analysis disabled due to large number of GCs ({1:###,###,###,000}) for node {0}. GC Log Timestamp {2}, Ending Timestamp {3}",
                                                gcInfo.Key,
                                                gcInfoTimeLineCnt,
                                                gcInfoTimeLine.First().LogTimestamp,
                                                gcInfoTimeLine.Last().LogTimestamp);

                        ComponentDisabled.Add(new Tuple<DateTime, string, string, string, int>(
                                                    gcInfo.Value.First().LogTimestamp,
                                                    gcInfo.Key,
                                                    null,
                                                    "GC TimeFrame Analysis Reduced",
                                                    gcInfoTimeLineCnt));
                        return;
                    }

                    int maxDur = 0;
                    int maxPos = 0;
                    int nbrPos = 0;
                    int midWayPt = ParserSettings.GCComplexAnalysisTakeEventsWhenOver / 2;
                    var orgCount = gcInfoTimeLineCnt;
                    var orgLogTimestamp = gcInfoTimeLine.First().LogTimestamp;
                    var orgEndLogTimestamp = gcInfoTimeLine.Last().LogTimestamp;

                    gcInfoTimeLine.ForEach(g =>
                                    {
                                        if (g.Duration > maxDur)
                                        {
                                            maxDur = g.Duration;
                                            maxPos = nbrPos;
                                        }
                                        ++nbrPos;
                                    });

                    int skipNbr = 0;

                    if (maxPos > midWayPt)
                    {
                        skipNbr = maxPos - midWayPt;

                        if (skipNbr >= midWayPt)
                        {
                            skipNbr = orgCount - ParserSettings.GCComplexAnalysisTakeEventsWhenOver;
                        }
                    }

                    gcInfoTimeLine = gcInfoTimeLine.Skip(skipNbr).Take(ParserSettings.GCComplexAnalysisTakeEventsWhenOver);
                    gcInfoTimeLineCnt = gcInfoTimeLine.Count();

                    Logger.Instance.WarnFormat("Reduced GC Timeframe analysis due to large number of GCs ({1:###,###,###,000}) for node {0}. Items Skipped {2:###,###,###,000}, Taken next {3:###,###,###,000} resulting in {4:###,###,###,000} total GCs. New Starting GC Log Timestamp {5} (old {6}), New Ending Timestamp {7} (old {8})",
                                                gcInfo.Key,
                                                orgCount,
                                                skipNbr,
                                                ParserSettings.GCComplexAnalysisTakeEventsWhenOver,
                                                gcInfoTimeLineCnt,
                                                gcInfoTimeLine.First().LogTimestamp,
                                                orgLogTimestamp,
                                                gcInfoTimeLine.Last().LogTimestamp,
                                                orgEndLogTimestamp);

                    ComponentDisabled.Add(new Tuple<DateTime, string, string, string, int>(
                                                gcInfo.Value.First().LogTimestamp,
                                                gcInfo.Key,
                                                null,
                                                "GC TimeFrame Analysis Reduced",
                                                orgCount));
                }

                DateTime timeFrame = gcDetectionPercent < 0
                                        || gcTimeframeDetection == TimeSpan.Zero
                                                ? DateTime.MinValue
                                                : gcInfoTimeLine.First().LogTimestamp + gcTimeframeDetection;
                GCContinuousInfo detectionTimeFrame = timeFrame == DateTime.MinValue
                                                        ? null
                                                        : new GCContinuousInfo()
                                                        {
                                                            Node = gcInfo.Key,
                                                            Latencies = new List<int>() { gcInfoTimeLine.First().Duration },
                                                            GroupRefIds = new List<object>() { gcInfoTimeLine.First().GroupIndicator },
                                                            Timestamps = new List<DateTime>() { gcInfoTimeLine.First().LogTimestamp },
                                                            GCSpacePoolChanges = new List<GCContinuousInfo.SpaceChanged>()
                                                            {
                                                                    new GCContinuousInfo.SpaceChanged()
                                                                    {
                                                                        Eden = gcInfoTimeLine.First().GCEdenTo - gcInfoTimeLine.First().GCEdenFrom,
                                                                        Survivor = gcInfoTimeLine.First().GCSurvivorTo - gcInfoTimeLine.First().GCSurvivorFrom,
                                                                        Old = gcInfoTimeLine.First().GCOldTo - gcInfoTimeLine.First().GCOldFrom
                                                                    }
                                                            },
                                                            Type = "TimeFrame"
                                                        };
                GCContinuousInfo currentGCOverlappingInfo = null;
                bool overLapped = false;

                for (int nIndex = 1; nIndex < gcInfoTimeLineCnt; ++nIndex)
                {
                    #region GC Continous (overlapping)

                    if (overlapToleranceInMS >= 0
                            && gcInfoTimeLine.ElementAt(nIndex - 1).LogTimestamp.AddMilliseconds(gcInfoTimeLine.ElementAt(nIndex).Duration + overlapToleranceInMS)
                                        >= gcInfoTimeLine.ElementAt(nIndex).LogTimestamp)
                    {
                        if (overLapped)
                        {
                            currentGCOverlappingInfo.Latencies.Add(gcInfoTimeLine.ElementAt(nIndex).Duration);
                            currentGCOverlappingInfo.GroupRefIds.Add(gcInfoTimeLine.ElementAt(nIndex).GroupIndicator);
                            currentGCOverlappingInfo.GCSpacePoolChanges.Add(new GCContinuousInfo.SpaceChanged()
                            {
                                Eden = gcInfoTimeLine.ElementAt(nIndex).GCEdenTo - gcInfoTimeLine.ElementAt(nIndex).GCEdenFrom,
                                Survivor = gcInfoTimeLine.ElementAt(nIndex).GCSurvivorTo - gcInfoTimeLine.ElementAt(nIndex).GCSurvivorFrom,
                                Old = gcInfoTimeLine.ElementAt(nIndex).GCOldTo - gcInfoTimeLine.ElementAt(nIndex).GCOldFrom
                            });
                        }
                        else
                        {
                            overLapped = true;
                            gcList.Add(currentGCOverlappingInfo = new GCContinuousInfo()
                            {
                                Node = gcInfo.Key,
                                Latencies = new List<int>() { gcInfoTimeLine.ElementAt(nIndex - 1).Duration, gcInfoTimeLine.ElementAt(nIndex).Duration },
                                GroupRefIds = new List<object>() { gcInfoTimeLine.ElementAt(nIndex - 1).GroupIndicator },
                                Timestamps = new List<DateTime>() { gcInfoTimeLine.ElementAt(nIndex - 1).LogTimestamp },
                                GCSpacePoolChanges = new List<GCContinuousInfo.SpaceChanged>()
                                                        {
                                                            new GCContinuousInfo.SpaceChanged()
                                                            {
                                                                Eden = gcInfoTimeLine.ElementAt(nIndex-1).GCEdenTo - gcInfoTimeLine.ElementAt(nIndex-1).GCEdenFrom,
                                                                Survivor = gcInfoTimeLine.ElementAt(nIndex-1).GCSurvivorTo - gcInfoTimeLine.ElementAt(nIndex-1).GCSurvivorFrom,
                                                                Old = gcInfoTimeLine.ElementAt(nIndex-1).GCOldTo - gcInfoTimeLine.ElementAt(nIndex-1).GCOldFrom
                                                            },
                                                            new GCContinuousInfo.SpaceChanged()
                                                            {
                                                                Eden = gcInfoTimeLine.ElementAt(nIndex).GCEdenTo - gcInfoTimeLine.ElementAt(nIndex).GCEdenFrom,
                                                                Survivor = gcInfoTimeLine.ElementAt(nIndex).GCSurvivorTo - gcInfoTimeLine.ElementAt(nIndex).GCSurvivorFrom,
                                                                Old = gcInfoTimeLine.ElementAt(nIndex).GCOldTo - gcInfoTimeLine.ElementAt(nIndex).GCOldFrom
                                                            }
                                                        },
                                Type = "Overlap"
                            });
                        }
                    }
                    else
                    {
                        overLapped = false;
                    }
                    #endregion

                    #region GC TimeFrame

                    if (gcInfoTimeLine.ElementAt(nIndex).LogTimestamp <= timeFrame)
                    {
                        detectionTimeFrame.GroupRefIds.Add(gcInfoTimeLine.ElementAt(nIndex).GroupIndicator);
                        detectionTimeFrame.Latencies.Add(gcInfoTimeLine.ElementAt(nIndex).Duration);
                        detectionTimeFrame.Timestamps.Add(gcInfoTimeLine.ElementAt(nIndex).LogTimestamp);
                        detectionTimeFrame.GCSpacePoolChanges.Add(new GCContinuousInfo.SpaceChanged()
                        {
                            Eden = gcInfoTimeLine.ElementAt(nIndex).GCEdenTo - gcInfoTimeLine.ElementAt(nIndex).GCEdenFrom,
                            Survivor = gcInfoTimeLine.ElementAt(nIndex).GCSurvivorTo - gcInfoTimeLine.ElementAt(nIndex).GCSurvivorFrom,
                            Old = gcInfoTimeLine.ElementAt(nIndex).GCOldTo - gcInfoTimeLine.ElementAt(nIndex).GCOldFrom
                        });
                    }
                    else if (detectionTimeFrame != null)
                    {
                        var totalGCLat = detectionTimeFrame.Latencies.Sum();
                        detectionTimeFrame.Percentage = 1m - ((decimal)(gcTimeframeDetection.TotalMilliseconds - totalGCLat) / (decimal)gcTimeframeDetection.TotalMilliseconds);

                        if (detectionTimeFrame.Percentage >= gcDetectionPercent)
                        {
                            gcList.Add(detectionTimeFrame);
                        }

                        timeFrame = gcInfoTimeLine.ElementAt(nIndex).LogTimestamp + gcTimeframeDetection;
                        detectionTimeFrame = new GCContinuousInfo()
                        {
                            Node = gcInfo.Key,
                            Latencies = new List<int>() { gcInfoTimeLine.ElementAt(nIndex).Duration },
                            GroupRefIds = new List<object>() { gcInfoTimeLine.ElementAt(nIndex).GroupIndicator },
                            Timestamps = new List<DateTime>() { gcInfoTimeLine.ElementAt(nIndex).LogTimestamp },
                            GCSpacePoolChanges = new List<GCContinuousInfo.SpaceChanged>()
                                                    {
                                                        new GCContinuousInfo.SpaceChanged()
                                                        {
                                                            Eden = gcInfoTimeLine.ElementAt(nIndex).GCEdenTo - gcInfoTimeLine.ElementAt(nIndex).GCEdenFrom,
                                                            Survivor = gcInfoTimeLine.ElementAt(nIndex).GCSurvivorTo - gcInfoTimeLine.ElementAt(nIndex).GCSurvivorFrom,
                                                            Old = gcInfoTimeLine.ElementAt(nIndex).GCOldTo - gcInfoTimeLine.ElementAt(nIndex).GCOldFrom
                                                        }
                                                    },
                            Type = "TimeFrame"
                        };
                    }
                    #endregion
                }

                #region GC TimeFrame

                if (detectionTimeFrame != null)
                {
                    var totalGCLat = detectionTimeFrame.Latencies.Sum();
                    detectionTimeFrame.Percentage = 1m - ((decimal)(gcTimeframeDetection.TotalMilliseconds - totalGCLat) / (decimal)gcTimeframeDetection.TotalMilliseconds);

                    if (detectionTimeFrame.Percentage >= gcDetectionPercent)
                    {
                        gcList.Add(detectionTimeFrame);
                    }
                }
                #endregion
            }
            );

            if (gcList.Count > 0)
            {
                #region GC Continous Check nbr occurrences

                if (overlapContinuousGCNbrInSeries > 0)
                {
                    gcList.UnSafe
                            .Where(i => !i.Deleted && i.Type == "Overlap" && i.Latencies.Count < overlapContinuousGCNbrInSeries)
                            .ForEach(i => i.Deleted = true);
                }

                #endregion

                initializeTPStatsDataTable(dtNodeStats);
                int nbrAdded = 0;
                int nbrRemoved = 0;

                foreach (var item in gcList.UnSafe)
                {
                    if (item.Deleted)
                    {
                        ++nbrRemoved;
                    }
                    else
                    {
                        var splitName = item.Node.Split('|');

                        if (item.Type == "Overlap")
                        {
                            #region GC Continous (overlapping)

                            var dataRow = dtNodeStats.NewRow();
                            var refIds = string.Join(",", item.GroupRefIds.DuplicatesRemoved(id => id));

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC Continuous maximum latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Max();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Max(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Max(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Max(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC Continuous minimum latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Min();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Min(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Min(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Min(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC Continuous mean latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Average();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Average(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Average(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Average(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC Continuous occurrences";
                            dataRow["Reconciliation Reference"] = "[" + string.Join(", ", item.GroupRefIds
                                                                                            .SelectWithIndex((refId, idx)
                                                                                                => string.Format("[{0}, {1:yyyy-MM-dd HH:mm:ss.ff}]",
                                                                                                                    refId,
                                                                                                                    item.Timestamps.ElementAtOrDefault(idx)))) + "]";
                            dataRow["Occurrences"] = item.Latencies.Count;

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC Continuous latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Sum();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Sum(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Sum(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Sum(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC Continuous standard deviation latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = (int)item.Latencies.StandardDeviationP();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Select(i => i.Eden).StandardDeviationP();
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Select(i => i.Survivor).StandardDeviationP();
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Select(i => i.Old).StandardDeviationP();

                            dtNodeStats.Rows.Add(dataRow);

                            //Update Log with warning
                            lock (dtLog)
                            {
                                dataRow = dtLog.NewRow();

                                dataRow["Data Center"] = splitName[0];
                                dataRow["Node IPAddress"] = splitName[1];
                                dataRow["Timestamp"] = item.Timestamps.FirstOrDefault();
                                dataRow["Exception"] = "GC Continuous Warning";
                                dataRow["Associated Value"] = item.Latencies.Count;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Indicator"] = "WARN";
                                dataRow["Task"] = "DetectContinuousGCIntoNodeStats";
                                dataRow["Item"] = "Generated";
                                dataRow["Description"] = string.Format("GC Continuous Warning where a total of {0} detected for a total latency of {1:###,###,##0} ms that had an ending timestamp of {2}",
                                                                        item.Latencies.Count,
                                                                        item.Latencies.Sum(),
                                                                        item.Timestamps.LastOrDefault());

                                dtLog.Rows.Add(dataRow);
                            }
                            #endregion
                        }
                        else if (item.Type == "TimeFrame")
                        {
                            #region GC TimeFrame

                            var dataRow = dtNodeStats.NewRow();
                            var refIds = string.Join(",", item.GroupRefIds.DuplicatesRemoved(id => id));

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC TimeFrame maximum latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Max();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Max(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Max(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Max(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC TimeFrame minimum latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Min();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Min(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Min(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Min(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC TimeFrame mean latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Average();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Average(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Average(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Average(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC TimeFrame occurrences";
                            dataRow["Reconciliation Reference"] = "[" + string.Join(", ", item.GroupRefIds
                                                                                           .SelectWithIndex((refId, idx)
                                                                                               => string.Format("[{0}, {1:yyyy-MM-dd HH:mm:ss.ff}]",
                                                                                                                   refId,
                                                                                                                   item.Timestamps.ElementAtOrDefault(idx)))) + "]";
                            dataRow["Occurrences"] = item.Latencies.Count;

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC TimeFrame latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = item.Latencies.Sum();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Sum(i => i.Eden);
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Sum(i => i.Survivor);
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Sum(i => i.Old);

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC TimeFrame standard deviation latency";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Latency (ms)"] = (int)item.Latencies.StandardDeviationP();
                            dataRow["GC Eden Space Change (mb)"] = item.GCSpacePoolChanges.Select(i => i.Eden).StandardDeviationP();
                            dataRow["GC Survivor Space Change (mb)"] = item.GCSpacePoolChanges.Select(i => i.Survivor).StandardDeviationP();
                            dataRow["GC Old Space Change (mb)"] = item.GCSpacePoolChanges.Select(i => i.Old).StandardDeviationP();

                            dtNodeStats.Rows.Add(dataRow);

                            dataRow = dtNodeStats.NewRow();

                            dataRow["Source"] = "Cassandra Log";
                            dataRow["Data Center"] = splitName[0];
                            dataRow["Node IPAddress"] = splitName[1];
                            dataRow["Attribute"] = "GC TimeFrame percent";
                            dataRow["Reconciliation Reference"] = refIds;
                            dataRow["Size (mb)"] = item.Percentage;

                            dtNodeStats.Rows.Add(dataRow);

                            //Update Log with warning
                            lock (dtLog)
                            {
                                dataRow = dtLog.NewRow();

                                dataRow["Data Center"] = splitName[0];
                                dataRow["Node IPAddress"] = splitName[1];
                                dataRow["Timestamp"] = item.Timestamps.FirstOrDefault();
                                dataRow["Exception"] = "GC TimeFrame percent Warning";
                                dataRow["Associated Value"] = item.Latencies.Count;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Indicator"] = "WARN";
                                dataRow["Task"] = "DetectContinuousGCIntoNodeStats";
                                dataRow["Item"] = "Generated";
                                dataRow["Description"] = string.Format("GC TimeFrame percent Warning where a total of {0} detected for a total latency of {1:###,###,##0) ms that had an ending timestamp of {2}",
                                                                        item.Latencies.Count,
                                                                        item.Latencies.Sum(),
                                                                        item.Timestamps.LastOrDefault());

                                dtLog.Rows.Add(dataRow);
                            }
                            #endregion
                        }

                        ++nbrAdded;
                    }
                }

                Logger.Instance.InfoFormat("Adding GC Continuous Occurrences ({0}) to TPStats", nbrAdded);

            }

            Logger.Instance.Info("Completed Detecting Continuous/Timeframe GCs");
        }

        public static IEnumerable<RepairLogInfo> ParseReadRepairFromLog(DataTable dtroCLog,
                                                                            Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                                                            Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
                                                                            IEnumerable<string> ignoreKeySpaces,
                                                                            int readrepairThreshold)
        {

            if (dtroCLog.Rows.Count == 0)
            {
                return Enumerable.Empty<RepairLogInfo>();
            }

            Logger.Instance.Info("Begin Parsing of Repairs from logs");

            var globalReadRepairs = new Common.Patterns.Collections.ThreadSafe.List<RepairLogInfo>();
            {
                var logItems = from dr in dtroCLog.AsEnumerable()
                               let dcName = dr.Field<string>("Data Center")
                               let ipAddress = dr.Field<string>("Node IPAddress")
                               let item = dr.Field<string>("Item")
                               let timestamp = dr.Field<DateTime>("Timestamp")
                               let flagged = dr.Field<int?>("Flagged")
                               let descr = dr.Field<string>("Description")?.Trim().ToLower()
                               let exception = dr.Field<string>("Exception")
                               let assocValue = dr.Field<object>("Associated Value")
                               where ((flagged.HasValue && (flagged.Value == 3 || flagged.Value == 1))
                                       || (item == "RepairSession.java"
                                               && (descr.Contains("new session")
                                                        || descr.Contains("session completed")
                                                        || descr.Contains("received merkle tree")))
                                       || (item == "StreamingRepairTask.java"
                                                && descr.Contains("streaming repair"))
                                        || (exception != null
                                                && (exception.StartsWith("node shutdown", StringComparison.CurrentCultureIgnoreCase)
                                                        || exception.StartsWith("node startup", StringComparison.CurrentCultureIgnoreCase))))
                               group new
                               {
                                   Task = dr.Field<string>("Task"),
                                   Item = item,
                                   Timestamp = timestamp,
                                   Flagged = flagged,
                                   Exception = exception,
                                   Description = descr,
                                   AssocValue = assocValue is string ? (string)assocValue : assocValue?.ToString()
                               }
                               by new { dcName, ipAddress } into g
                               select new
                               {
                                   DCName = g.Key.dcName,
                                   IPAddress = g.Key.ipAddress,
                                   LogItems = (from l in g orderby l.Timestamp ascending select l)
                               };

                Parallel.ForEach(logItems, logGroupItem =>
                //foreach (var logGroupItem in logItems)
                {
                    Logger.Instance.InfoFormat("Begin Parsing of Repairs for {0}|{1} with {2} items",
                                                    logGroupItem.DCName,
                                                    logGroupItem.IPAddress,
                                                    logGroupItem.LogItems.Count());

                    DateTime oldestLogTimeStamp = DateTime.MaxValue;
                    var dtStatusLog = new DataTable(string.Format("NodeStatus-ReadRepair-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));
                    var currentReadRepairs = new List<RepairLogInfo>();
                    var optionsReadRepair = new List<Tuple<string, string>>(); //Keyspace, Options
                    var userRequests = new List<Tuple<string, string, bool>>(); //Start Range, Keyspace, User Request
                    var readRepairs = new List<RepairLogInfo>();
                    var groupIndicator = CLogSummaryInfo.IncrementGroupInicator();

                    if (dtLogStatusStack != null) dtLogStatusStack.Push(dtStatusLog);

                    InitializeStatusDataTable(dtStatusLog);

                    foreach (var item in logGroupItem.LogItems)
                    {
                        oldestLogTimeStamp = item.Timestamp;

                        if (item.Item == "RepairSession.java")
                        {
                            #region RepairSession.java
                            //[repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] new session: will sync c1249170.ews.int/10.12.49.11, /10.12.51.29 on range (7698152963051967815,7704157762555128476] for OpsCenter.[rollups7200, settings, events, rollups60, rollups86400, rollups300, events_timeline, backup_reports, bestpractice_results, pdps]
                            var regExNewSession = RegExRepairNewSessionLine.Split(item.Description);

                            if (regExNewSession.Length > 5 && regExNewSession[2] == "new")
                            {
                                #region New Session
                                if (ignoreKeySpaces.Contains(regExNewSession[5].Trim()))
                                {
                                    continue;
                                }

                                var logInfo = new RepairLogInfo()
                                {
                                    Session = regExNewSession[1].Trim(),
                                    DataCenter = logGroupItem.DCName,
                                    IPAddress = logGroupItem.IPAddress,
                                    Keyspace = regExNewSession[5].Trim(),
                                    StartTime = item.Timestamp,
                                    //Finish;
                                    TokenRangeStart = regExNewSession[3].Trim(),
                                    TokenRangeEnd = regExNewSession[4].Trim(),
                                    GroupIndicator = groupIndicator
                                };

                                var dataRow = dtStatusLog.NewRow();

                                dataRow["Timestamp"] = item.Timestamp;
                                dataRow["Data Center"] = logGroupItem.DCName;
                                dataRow["Node IPAddress"] = logGroupItem.IPAddress;
                                dataRow["Pool/Cache Type"] = "Read Repair (" + item.Task + ") Start";
                                dataRow["Reconciliation Reference"] = groupIndicator;

                                dataRow["Session"] = logInfo.Session;
                                dataRow["Start Token Range (exclusive)"] = logInfo.TokenRangeStart;
                                dataRow["End Token Range (inclusive)"] = logInfo.TokenRangeEnd;
                                dataRow["KeySpace"] = logInfo.Keyspace;
                                dataRow["Requested"] = !string.IsNullOrEmpty(logInfo.Options);

                                logInfo.Add();
                                currentReadRepairs.Add(logInfo);
                                readRepairs.Add(logInfo);
                                globalReadRepairs.Add(logInfo);

                                dataRow["Session Path"] = logInfo.GenerateSessionPath(currentReadRepairs);
                                dtStatusLog.Rows.Add(dataRow);
                                #endregion
                            }
                            else
                            {
                                //[repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] session completed successfully
                                var regExEndSession = RegExRepairEndSessionLine.Split(item.Description);

                                if (regExEndSession.Length > 3 && regExEndSession[2] == "completed")
                                {
                                    #region Session Completed
                                    var logInfo = currentReadRepairs.Find(r => r.Session == regExEndSession[1]);

                                    if (logInfo != null)
                                    {
                                        var rrOption = optionsReadRepair.FindAll(u => u.Item1 == logInfo.Keyspace).LastOrDefault();
                                        var userRequest = userRequests.FindAll(u => u.Item1 == logInfo.TokenRangeStart && u.Item2 == logInfo.Keyspace).LastOrDefault();

                                        currentReadRepairs.Remove(logInfo);

                                        if (rrOption != null)
                                        {
                                            logInfo.Options = rrOption.Item2;
                                            optionsReadRepair.Remove(rrOption);
                                        }
                                        if (userRequest != null)
                                        {
                                            logInfo.UserRequest = userRequest.Item3;
                                            userRequests.Remove(userRequest);
                                        }

                                        logInfo.Completed(item.Timestamp);

                                        var dataRow = dtStatusLog.NewRow();

                                        dataRow["Timestamp"] = item.Timestamp;
                                        dataRow["Data Center"] = logGroupItem.DCName;
                                        dataRow["Node IPAddress"] = logGroupItem.IPAddress;
                                        dataRow["Pool/Cache Type"] = "Read Repair (" + item.Task + ") Finished";
                                        dataRow["Reconciliation Reference"] = groupIndicator;

                                        dataRow["Session"] = logInfo.Session;
                                        dataRow["Start Token Range (exclusive)"] = logInfo.TokenRangeStart;
                                        dataRow["End Token Range (inclusive)"] = logInfo.TokenRangeEnd;
                                        dataRow["KeySpace"] = logInfo.Keyspace;
                                        dataRow["Nbr GCs"] = logInfo.GCs;
                                        dataRow["Nbr Compactions"] = logInfo.Compactions;
                                        dataRow["Nbr MemTable Flush Events"] = logInfo.MemTableFlushes;
                                        dataRow["Nbr Exceptions"] = logInfo.Exceptions;
                                        dataRow["Nbr Solr ReIdxs"] = logInfo.SolrReIndexing == null ? 0 : logInfo.SolrReIndexing.Count();
                                        dataRow["Duration (ms)"] = logInfo.Duration;
                                        if (logInfo.UserRequest) dataRow["Requested"] = logInfo.UserRequest;
                                        dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(r => r.Session)) + "X" + logInfo.Session;

                                        dtStatusLog.Rows.Add(dataRow);
                                    }
                                    #endregion
                                }
                                else
                                {
                                    #region Received Tree
                                    // RepairSession.java:181 - [repair #87a95910-bf45-11e6-a2a7-f19e9c4c25c4] Received merkle tree for node_slow_log from /10.14.149.8
                                    var regExReceived = RegExRepairReceivedLine.Split(item.Description);

                                    if (regExReceived.Length > 4 && regExReceived[2] == "received")
                                    {
                                        var logInfo = currentReadRepairs.Find(r => r.Session == regExReceived[1]);

                                        if (logInfo != null)
                                        {
                                            if (!logInfo.ReceivedNodes.Contains(regExReceived[3]))
                                            {
                                                logInfo.ReceivedNodes.Add(regExReceived[3]);
                                            }
                                        }
                                    }
                                    #endregion
                                }
                            }
                            #endregion
                        }
                        else if (item.Item == "StreamingRepairTask.java")
                        {
                            #region StreamingRepairTask.java

                            //[streaming task #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] Performing streaming repair of 1 ranges with /10.12.51.29
                            var regExNbrSession = RegExRepairNbrRangesLine.Split(item.Description);

                            if (regExNbrSession.Length > 4)
                            {
                                var gcInfo = currentReadRepairs.Find(r => r.Session == regExNbrSession[1].Trim());

                                if (gcInfo != null)
                                {
                                    gcInfo.NbrRepairs += int.Parse(regExNbrSession[2]);

                                    if (!gcInfo.RepairNodes.Contains(regExNbrSession[3]))
                                    {
                                        gcInfo.RepairNodes.Add(regExNbrSession[3]);
                                    }
                                }
                            }

                            #endregion
                        }
                        else if (item.Item == "StorageService.java")
                        {
                            #region StorageService.java
                            var regExRepairOpts = RegExRepairUserRequest1.Split(item.Description);

                            if (regExRepairOpts.Length == 4)
                            {
                                if (ignoreKeySpaces.Contains(regExRepairOpts[1].Trim()))
                                {
                                    continue;
                                }
                                optionsReadRepair.Add(new Tuple<string, string>(regExRepairOpts[1].Trim(), regExRepairOpts[2].Trim()));
                            }
                            else
                            {
                                var regExRepairUseRequest = RegExRepairUserRequest.Split(item.Description);

                                if (regExRepairUseRequest.Length == 5)
                                {
                                    if (ignoreKeySpaces.Contains(regExRepairUseRequest[3].Trim()))
                                    {
                                        continue;
                                    }
                                    userRequests.Add(new Tuple<string, string, bool>(regExRepairUseRequest[1].Trim(), regExRepairUseRequest[3].Trim(), true));
                                }
                            }
                            #endregion
                        }

                        #region Read Repair Aborted Porcessing
                        if (currentReadRepairs.Count > 0
                                && !string.IsNullOrEmpty(item.Exception))
                        {
                            if (item.Exception.StartsWith("node shutdown", StringComparison.CurrentCultureIgnoreCase)
                                    || item.Exception.StartsWith("node startup", StringComparison.CurrentCultureIgnoreCase))
                            {
                                currentReadRepairs.AsEnumerable().Reverse().ToArray().ForEach(r =>
                                {
                                    if (!string.IsNullOrEmpty(r.Session))
                                    {
                                        r.Abort(item.Timestamp, item.Exception);
                                        currentReadRepairs.Remove(r);

                                        var dataRow = dtStatusLog.NewRow();

                                        dataRow["Timestamp"] = item.Timestamp;
                                        dataRow["Data Center"] = logGroupItem.DCName;
                                        dataRow["Node IPAddress"] = logGroupItem.IPAddress;
                                        dataRow["Pool/Cache Type"] = "Read Repair (" + item.Exception + ") Aborted";
                                        dataRow["Reconciliation Reference"] = groupIndicator;

                                        dataRow["Session"] = r.Session;
                                        dataRow["Start Token Range (exclusive)"] = r.TokenRangeStart;
                                        dataRow["End Token Range (inclusive)"] = r.TokenRangeEnd;
                                        dataRow["KeySpace"] = r.Keyspace;
                                        dataRow["Nbr GCs"] = r.GCs;
                                        dataRow["Nbr Compactions"] = r.Compactions;
                                        dataRow["Nbr MemTable Flush Events"] = r.MemTableFlushes;
                                        dataRow["Nbr Exceptions"] = r.Exceptions;
                                        dataRow["Nbr Solr ReIdxs"] = r.SolrReIndexing == null ? 0 : r.SolrReIndexing.Count();
                                        dataRow["Duration (ms)"] = r.Duration;
                                        if (r.UserRequest) dataRow["Requested"] = r.UserRequest;
                                        dataRow["Aborted"] = 1;
                                        dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(i => "X" + i.Session));

                                        dtStatusLog.Rows.Add(dataRow);
                                    }
                                });

                                currentReadRepairs.Clear();
                            }
                            else if (!string.IsNullOrEmpty(item.AssocValue)
                                            && currentReadRepairs.Any(r => !string.IsNullOrEmpty(r.Session)
                                                                                && item.AssocValue.Contains(r.Session)))
                            {
                                currentReadRepairs.ToArray().ForEach(r =>
                                {
                                    if (!string.IsNullOrEmpty(r.Session)
                                            && item.AssocValue.Contains(r.Session))
                                    {
                                        r.Abort(item.Timestamp, item.Exception);
                                        ++r.Exceptions;

                                        currentReadRepairs.Remove(r);

                                        var dataRow = dtStatusLog.NewRow();

                                        dataRow["Timestamp"] = item.Timestamp;
                                        dataRow["Data Center"] = logGroupItem.DCName;
                                        dataRow["Node IPAddress"] = logGroupItem.IPAddress;
                                        dataRow["Pool/Cache Type"] = "Read Repair Session Failure";
                                        dataRow["Reconciliation Reference"] = groupIndicator;

                                        dataRow["Session"] = r.Session;
                                        dataRow["Start Token Range (exclusive)"] = r.TokenRangeStart;
                                        dataRow["End Token Range (inclusive)"] = r.TokenRangeEnd;
                                        dataRow["KeySpace"] = r.Keyspace;
                                        dataRow["Nbr GCs"] = r.GCs;
                                        dataRow["Nbr Compactions"] = r.Compactions;
                                        dataRow["Nbr MemTable Flush Events"] = r.MemTableFlushes;
                                        dataRow["Nbr Exceptions"] = r.Exceptions;
                                        dataRow["Nbr Solr ReIdxs"] = r.SolrReIndexing == null ? 0 : r.SolrReIndexing.Count();
                                        dataRow["Latency (ms)"] = r.Duration;
                                        if (r.UserRequest) dataRow["Requested"] = r.UserRequest;
                                        dataRow["Aborted"] = 1;
                                        dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(i => "X" + i.Session));

                                        dtStatusLog.Rows.Add(dataRow);
                                    }
                                });
                            }
                            else
                            {
                                currentReadRepairs.ForEach(r =>
                                {
                                    ++r.Exceptions;
                                });
                            }
                        }
                        #endregion
                    }

                    #region Orphaned RRs
                    currentReadRepairs.Where(r => !string.IsNullOrEmpty(r.Session) && r.CompletionTime == DateTime.MinValue).ToArray().ForEach(r =>
                    {
                        r.Abort(DateTime.MinValue, "Orphaned");

                        var dataRow = dtStatusLog.NewRow();

                        dataRow["Timestamp"] = oldestLogTimeStamp;
                        dataRow["Data Center"] = logGroupItem.DCName;
                        dataRow["Node IPAddress"] = logGroupItem.IPAddress;
                        dataRow["Pool/Cache Type"] = "Read Repair Orphaned";
                        dataRow["Reconciliation Reference"] = groupIndicator;

                        dataRow["Session"] = r.Session;
                        dataRow["Start Token Range (exclusive)"] = r.TokenRangeStart;
                        dataRow["End Token Range (inclusive)"] = r.TokenRangeEnd;
                        dataRow["KeySpace"] = r.Keyspace;
                        dataRow["Nbr GCs"] = r.GCs;
                        dataRow["Nbr Compactions"] = r.Compactions;
                        dataRow["Nbr MemTable Flush Events"] = r.MemTableFlushes;
                        dataRow["Nbr Exceptions"] = r.Exceptions;
                        dataRow["Nbr Solr ReIdxs"] = r.SolrReIndexing == null ? 0 : r.SolrReIndexing.Count();
                        dataRow["Aborted"] = r.Aborted ? 1 : 0;
                        //dataRow["Duration (ms)"] = r.Duration;
                        dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(i => (i.Session == r.Session ? "X" : string.Empty) + i.Session));

                        dtStatusLog.Rows.Add(dataRow);
                    });
                    currentReadRepairs.Clear();
                    optionsReadRepair.Clear();
                    #endregion

                    if (readRepairs.Count > 0)
                    {
                        #region read repairs CFStats and GC/Compaction/Solr collection updates

                        {
                            Common.Patterns.Collections.ThreadSafe.List<ICompactionLogInfo> nodeCompCollection = null;
                            Common.Patterns.Collections.ThreadSafe.List<GCLogInfo> nodeGCCollection = null;
                            Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo> nodeSolrIdxCollection = null;
                            Common.Patterns.Collections.ThreadSafe.List<MemTableFlushLogInfo> nodeMemTblFlushCollection = null;
                            Common.Patterns.Collections.ThreadSafe.List<PerformanceInfo> nodePerfCollection = null;
                            Common.Patterns.Collections.ThreadSafe.List<SolrHardCommitLogInfo> nodeSolrHrdCmtCollection = null;

                            var dcIpAddress = (logGroupItem.DCName == null ? string.Empty : logGroupItem.DCName) + "|" + logGroupItem.IPAddress;

                            CompactionOccurrences.TryGetValue(dcIpAddress, out nodeCompCollection);
                            GCOccurrences.TryGetValue(dcIpAddress, out nodeGCCollection);
                            SolrReindexingOccurrences.TryGetValue(dcIpAddress, out nodeSolrIdxCollection);
                            MemTableFlushOccurrences.TryGetValue(dcIpAddress, out nodeMemTblFlushCollection);
                            PerformanceOccurrences.TryGetValue(dcIpAddress, out nodePerfCollection);
                            SolrHardCommits.TryGetValue(dcIpAddress, out nodeSolrHrdCmtCollection);

                            //(new Common.File.FilePathAbsolute(string.Format(@"[DeskTop]\{0}.txt", dcIpAddress.Replace('|', '-'))))
                            //	.WriteAllText(Newtonsoft.Json.JsonConvert.SerializeObject(nodeSolrIdxCollection, Newtonsoft.Json.Formatting.Indented));

                            readRepairs.ForEach(rrInfo =>
                                {
                                    rrInfo.CompactionList = nodeCompCollection?.UnSafe.Where(c => rrInfo.Keyspace == c.Keyspace
                                                                                                    && rrInfo.StartTime <= c.StartTime
                                                                                                    && c.StartTime < rrInfo.CompletionTime.AddMilliseconds(readrepairThreshold));
                                    rrInfo.GCList = nodeGCCollection?.UnSafe.Where(c => rrInfo.StartTime <= c.StartTime
                                                                                            && c.StartTime < rrInfo.CompletionTime.AddMilliseconds(readrepairThreshold)
                                                                                            && rrInfo.SessionPath == rrInfo.Session);
                                    rrInfo.SolrReIndexing = nodeSolrIdxCollection?.UnSafe.Where(c => rrInfo.Keyspace == c.Keyspace
                                                                                                    && rrInfo.StartTime <= c.StartTime
                                                                                                    && c.StartTime < rrInfo.CompletionTime.AddMilliseconds(readrepairThreshold));
                                    rrInfo.MemTableFlushList = nodeMemTblFlushCollection?.UnSafe.Where(c => rrInfo.Keyspace == c.Keyspace
                                                                                                            && rrInfo.StartTime <= c.StartTime
                                                                                                            && c.StartTime < rrInfo.CompletionTime.AddMilliseconds(readrepairThreshold));
                                    rrInfo.PerformanceWarningList = nodePerfCollection?.UnSafe.Where(c => (c.Keyspace == null || rrInfo.Keyspace == c.Keyspace)
                                                                                                            && rrInfo.StartTime <= c.StartTime
                                                                                                            && c.StartTime < rrInfo.CompletionTime.AddMilliseconds(readrepairThreshold));
                                    rrInfo.SolrHardCommits = nodeSolrHrdCmtCollection?.UnSafe.Where(c => rrInfo.Keyspace == c.Keyspace
                                                                                                            && rrInfo.StartTime <= c.StartTime
                                                                                                            && c.StartTime < rrInfo.CompletionTime.AddMilliseconds(readrepairThreshold));
                                });

                            //(new Common.File.FilePathAbsolute(string.Format(@"[DeskTop]\RR-{0}.txt", dcIpAddress.Replace('|', '-'))))
                            //	.WriteAllText(Newtonsoft.Json.JsonConvert.SerializeObject(readRepairs, Newtonsoft.Json.Formatting.Indented));
                        }

                        var dtCFStats = new DataTable(string.Format("CFStats-ReadRepair-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));

                        if (dtCFStatsStack != null) dtCFStatsStack.Push(dtCFStats);
                        initializeCFStatsDataTable(dtCFStats);

                        Logger.Instance.InfoFormat("Adding Read Repairs ({2}) to CFStats for \"{0}\" \"{1}\"", logGroupItem.DCName, logGroupItem.IPAddress, readRepairs.Count);

                        {
                            var readrepairStats = from repairItem in readRepairs
                                                  where repairItem.CompletionTime != DateTime.MinValue
                                                          && !repairItem.Aborted
                                                  group repairItem by new { repairItem.DataCenter, repairItem.IPAddress, repairItem.Keyspace }
                                                        into g
                                                  select new
                                                  {
                                                      DCName = g.Key.DataCenter,
                                                      IpAdress = g.Key.IPAddress,
                                                      KeySpace = g.Key.Keyspace,
                                                      GrpInds = g.GroupIndicatorString(),
                                                      Max = g.Max(s => s.Duration),
                                                      Min = g.Min(s => s.Duration),
                                                      Avg = (int)g.Average(s => s.Duration),
                                                      Count = g.Count()
                                                  };

                            foreach (var statItem in readrepairStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = statItem.DCName;
                                dataRow["Node IPAddress"] = statItem.IpAdress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Attribute"] = "Read Repair maximum latency";
                                dataRow["Reconciliation Reference"] = statItem.GrpInds;
                                dataRow["Value"] = statItem.Max;
                                dataRow["(Value)"] = statItem.Max;
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = statItem.DCName;
                                dataRow["Node IPAddress"] = statItem.IpAdress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Attribute"] = "Read Repair minimum latency";
                                dataRow["Reconciliation Reference"] = statItem.GrpInds;
                                dataRow["Value"] = statItem.Min;
                                dataRow["(Value)"] = statItem.Min;
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = statItem.DCName;
                                dataRow["Node IPAddress"] = statItem.IpAdress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Attribute"] = "Read Repair mean latency";
                                dataRow["Reconciliation Reference"] = statItem.GrpInds;
                                dataRow["Value"] = statItem.Avg;
                                dataRow["(Value)"] = statItem.Avg;
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = statItem.DCName;
                                dataRow["Node IPAddress"] = statItem.IpAdress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Attribute"] = "Read Repair occurrences";
                                dataRow["Reconciliation Reference"] = statItem.GrpInds;
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;
                                //dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }
                        {
                            var readrepairAbortedStats = from repairItem in readRepairs
                                                         where repairItem.Aborted
                                                         group repairItem by new { repairItem.DataCenter, repairItem.IPAddress, repairItem.Keyspace }
                                                            into g
                                                         select new
                                                         {
                                                             DCName = g.Key.DataCenter,
                                                             IpAdress = g.Key.IPAddress,
                                                             KeySpace = g.Key.Keyspace,
                                                             GrpInds = g.GroupIndicatorString(),
                                                             Exceptions = string.Join(", ", g.DuplicatesRemoved(i => i.Exception).Select(i => i.Exception)),
                                                             Count = g.Count()
                                                         };

                            foreach (var statItem in readrepairAbortedStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = statItem.DCName;
                                dataRow["Node IPAddress"] = statItem.IpAdress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Attribute"] = "Read Repair Aborted occurrences";
                                dataRow["Reconciliation Reference"] = statItem.GrpInds;
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;
                                //dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }
                        #endregion
                    }

                    Logger.Instance.InfoFormat("Completed Parsing of Repairs for {0}|{1}",
                                                    logGroupItem.DCName,
                                                    logGroupItem.IPAddress);
                });
            }//end of scope for logItems

            Logger.Instance.Info("Completed Parsing of Repairs from logs");

            return globalReadRepairs.UnSafe;
        }

        public static void ReadRepairIntoDataTable(IEnumerable<RepairLogInfo> readRepairs,
                                                    DataTable dtReadRepair)
        {
            #region Read Repair Table
            if (dtReadRepair == null)
            {
                return;
            }

            if (dtReadRepair.Columns.Count == 0)
            {
                dtReadRepair.Columns.Add("Start Timestamp", typeof(DateTime)); //a
                dtReadRepair.Columns.Add("Session Path", typeof(string)); //b 2
                dtReadRepair.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Node IPAddress", typeof(string));
                dtReadRepair.Columns.Add("Type", typeof(string)).AllowDBNull = true;
                dtReadRepair.Columns.Add("KeySpace", typeof(string)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Table", typeof(string)).AllowDBNull = true; //g
                dtReadRepair.Columns.Add("Log/Completion Timestamp", typeof(DateTime)).AllowDBNull = true; //h

                //Read Repair
                dtReadRepair.Columns.Add("Session", typeof(string)); //i
                dtReadRepair.Columns.Add("Session Duration", typeof(TimeSpan)).AllowDBNull = true; //j
                dtReadRepair.Columns.Add("Token Range Start", typeof(string)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Token Range End", typeof(string)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Nbr of Repaired Ranges", typeof(int)).AllowDBNull = true; //m
                dtReadRepair.Columns.Add("Updating Nodes", typeof(string)).AllowDBNull = true; //n 14
                dtReadRepair.Columns.Add("Nbr Received Nodes", typeof(int)).AllowDBNull = true; //o
                dtReadRepair.Columns.Add("Received Nodes", typeof(string)).AllowDBNull = true; //p
                dtReadRepair.Columns.Add("Nbr GC Events", typeof(int)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Nbr Compaction Events", typeof(int)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Nbr MemTable Flush Events", typeof(int)).AllowDBNull = true; //s
                dtReadRepair.Columns.Add("Nbr Solr ReIdx Events", typeof(int)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Nbr Solr Hard Commit Events", typeof(int)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Nbr Exceptions", typeof(int)).AllowDBNull = true;
                dtReadRepair.Columns.Add("Options", typeof(string)).AllowDBNull = true; //w
                dtReadRepair.Columns.Add("Requested", typeof(int)).AllowDBNull = true; //x
                dtReadRepair.Columns.Add("Aborted Read Repair", typeof(int)).AllowDBNull = true; //y

                //GC
                dtReadRepair.Columns.Add("GC Time (ms)", typeof(long)).AllowDBNull = true; //z
                dtReadRepair.Columns.Add("Eden Changed (mb)", typeof(decimal)).AllowDBNull = true; //aa
                dtReadRepair.Columns.Add("Survivor Changed (mb)", typeof(decimal)).AllowDBNull = true;//ab
                dtReadRepair.Columns.Add("Old Changed (mb)", typeof(decimal)).AllowDBNull = true; //ac

                //Compaction
                dtReadRepair.Columns.Add("SSTables", typeof(int)).AllowDBNull = true; //ad
                dtReadRepair.Columns.Add("Old Size (mb)", typeof(decimal)).AllowDBNull = true;//ae
                dtReadRepair.Columns.Add("New Size (mb)", typeof(long)).AllowDBNull = true; //af
                dtReadRepair.Columns.Add("Compaction Time (ms)", typeof(int)).AllowDBNull = true; //ag
                dtReadRepair.Columns.Add("Compaction IORate (mb/sec)", typeof(decimal)).AllowDBNull = true; //ah

                //MemTable Flush
                dtReadRepair.Columns.Add("Occurrences", typeof(int)).AllowDBNull = true; //ai
                dtReadRepair.Columns.Add("Flushed to SSTable(s) Size (mb)", typeof(decimal)).AllowDBNull = true;//aj
                dtReadRepair.Columns.Add("Flush Time (ms)", typeof(int)).AllowDBNull = true; //ak
                dtReadRepair.Columns.Add("Write Rate (ops)", typeof(int)).AllowDBNull = true; //al
                dtReadRepair.Columns.Add("Effective Flush IORate (mb/sec)", typeof(decimal)).AllowDBNull = true; //am

                //Performance Warnings
                dtReadRepair.Columns.Add("Perf Warnings", typeof(int)).AllowDBNull = true; //an
                dtReadRepair.Columns.Add("Perf Average Latency", typeof(int)).AllowDBNull = true;//ao

                //Solr Reindexing
                dtReadRepair.Columns.Add("Solr ReIdx Duration", typeof(int)).AllowDBNull = true; //ap
                dtReadRepair.Columns["Solr ReIdx Duration"].Caption = "ReIdx"; //aq
                dtReadRepair.Columns.Add("Solr Hard Commit Duration", typeof(int)).AllowDBNull = true; //ar
                dtReadRepair.Columns["Solr Hard Commit Duration"].Caption = "Hard Commit"; //as

                dtReadRepair.Columns.Add("Reconciliation Reference", typeof(long)).AllowDBNull = true; //at
            }
            #endregion

            if (readRepairs.Count() > 0)
            {
                var readRepairItems = from rrItem in readRepairs
                                      orderby rrItem.StartTime ascending, rrItem.SessionPath ascending, rrItem.DataCenter, rrItem.IPAddress
                                      select rrItem;

                foreach (var rrItem in readRepairItems)
                {
                    var newDataRow = dtReadRepair.NewRow();


                    newDataRow["Start Timestamp"] = rrItem.StartTime;
                    newDataRow["Session Path"] = rrItem.SessionPath;
                    newDataRow["Data Center"] = rrItem.DataCenter;
                    newDataRow["Node IPAddress"] = rrItem.IPAddress;
                    newDataRow["Reconciliation Reference"] = rrItem.GroupIndicator;

                    //Read Repair
                    if (rrItem.Aborted)
                    {
                        if (string.IsNullOrEmpty(rrItem.Exception))
                        {
                            newDataRow["Type"] = "Read Repair (aborted)";
                        }
                        else if (rrItem.Exception.ToLower().Contains("shutdown")
                                 || rrItem.Exception.ToLower().Contains("startup"))
                        {
                            newDataRow["Type"] = "Read Repair (shutdown)";
                        }
                        else
                        {
                            newDataRow["Type"] = "Read Repair (exception)";
                        }
                    }
                    else
                    {
                        newDataRow["Type"] = "Read Repair";
                    }
                    newDataRow["KeySpace"] = rrItem.Keyspace;
                    newDataRow["Session"] = rrItem.Session;
                    if (rrItem.CompletionTime != DateTime.MinValue)
                    {
                        newDataRow["Log/Completion Timestamp"] = rrItem.CompletionTime;
                        if (rrItem.Duration >= 0)
                        {
                            newDataRow["Session Duration"] = TimeSpan.FromMilliseconds(rrItem.Duration);
                        }
                    }
                    newDataRow["Token Range Start"] = rrItem.TokenRangeStart;
                    newDataRow["Token Range End"] = rrItem.TokenRangeEnd;
                    newDataRow["Nbr of Repaired Ranges"] = rrItem.NbrRepairs;
                    newDataRow["Updating Nodes"] = string.Join(", ", rrItem.RepairNodes);
                    newDataRow["Nbr Received Nodes"] = rrItem.ReceivedNodes.Count;
                    newDataRow["Received Nodes"] = string.Join(", ", rrItem.ReceivedNodes);
                    newDataRow["Nbr GC Events"] = rrItem.GCs;
                    newDataRow["Nbr Compaction Events"] = rrItem.Compactions;
                    newDataRow["Nbr Solr ReIdx Events"] = rrItem.SolrReIndexing == null ? 0 : rrItem.SolrReIndexing.Count();
                    newDataRow["Nbr Solr Hard Commit Events"] = rrItem.SolrHardCommits == null ? 0 : rrItem.SolrHardCommits.Count();
                    newDataRow["Nbr MemTable Flush Events"] = rrItem.MemTableFlushes;
                    newDataRow["Nbr Exceptions"] = rrItem.Exceptions;
                    newDataRow["Aborted Read Repair"] = rrItem.Aborted ? 1 : 0;
                    newDataRow["Options"] = rrItem.Options;
                    newDataRow["Requested"] = rrItem.UserRequest ? 1 : 0;

                    dtReadRepair.Rows.Add(newDataRow);

                    if (rrItem.GCList != null)
                    {
                        foreach (var item in (from gc in rrItem.GCList
                                              let startTime = gc.StartTime
                                              orderby startTime ascending
                                              select new { StartTime = startTime, GC = gc }))
                        {
                            newDataRow = dtReadRepair.NewRow();

                            newDataRow["Start Timestamp"] = item.StartTime;
                            newDataRow["Session Path"] = rrItem.SessionPath;
                            newDataRow["Data Center"] = item.GC.DataCenter;
                            newDataRow["Node IPAddress"] = item.GC.IPAddress;
                            newDataRow["Type"] = "GC";
                            newDataRow["Log/Completion Timestamp"] = item.GC.LogTimestamp;
                            newDataRow["Session"] = rrItem.Session;
                            newDataRow["GC Time (ms)"] = item.GC.Duration;
                            newDataRow["Eden Changed (mb)"] = item.GC.GCEdenTo - item.GC.GCEdenFrom;
                            newDataRow["Survivor Changed (mb)"] = item.GC.GCSurvivorTo - item.GC.GCSurvivorFrom;
                            newDataRow["Old Changed (mb)"] = item.GC.GCOldTo - item.GC.GCOldFrom;
                            newDataRow["Reconciliation Reference"] = item.GC.GroupIndicator;

                            dtReadRepair.Rows.Add(newDataRow);
                        }
                    }

                    if (rrItem.CompactionList != null)
                    {
                        foreach (var item in (from comp in rrItem.CompactionList
                                              let startTime = comp.StartTime
                                              orderby startTime ascending
                                              select new { StartTime = startTime, Comp = comp }))
                        {
                            newDataRow = dtReadRepair.NewRow();

                            newDataRow["Start Timestamp"] = item.StartTime;
                            newDataRow["Session Path"] = rrItem.SessionPath;
                            newDataRow["Data Center"] = item.Comp.DataCenter;
                            newDataRow["Node IPAddress"] = item.Comp.IPAddress;
                            newDataRow["Type"] = item.Comp.Type;
                            newDataRow["Log/Completion Timestamp"] = item.Comp.CompletionTime;
                            newDataRow["Session"] = rrItem.Session;
                            newDataRow["KeySpace"] = item.Comp.Keyspace;
                            newDataRow["Table"] = item.Comp.Table;
                            newDataRow["SSTables"] = item.Comp.SSTables;
                            newDataRow["Compaction Time (ms)"] = item.Comp.Duration;
                            newDataRow["Reconciliation Reference"] = item.Comp.GroupIndicator;

                            if (item.Comp is CompactionLogInfo)
                            {
                                newDataRow["Old Size (mb)"] = ((CompactionLogInfo)item.Comp).OldSize;
                                newDataRow["New Size (mb)"] = ((CompactionLogInfo)item.Comp).NewSize;
                                newDataRow["Compaction IORate (mb/sec)"] = ((CompactionLogInfo)item.Comp).IORate;
                            }
                            else if (item.Comp is AntiCompactionLogInfo)
                            {
                                if (((AntiCompactionLogInfo)item.Comp).OldSize.HasValue)
                                {
                                    newDataRow["Old Size (mb)"] = ((AntiCompactionLogInfo)item.Comp).OldSize.Value;
                                }
                                if (((AntiCompactionLogInfo)item.Comp).NewSize.HasValue)
                                {
                                    newDataRow["New Size (mb)"] = ((AntiCompactionLogInfo)item.Comp).NewSize.Value;
                                }
                                if (((AntiCompactionLogInfo)item.Comp).IORate > 0)
                                {
                                    newDataRow["Compaction IORate (mb/sec)"] = ((AntiCompactionLogInfo)item.Comp).IORate;
                                }
                            }

                            dtReadRepair.Rows.Add(newDataRow);
                        }
                    }

                    if (rrItem.MemTableFlushList != null)
                    {
                        foreach (var item in (from memTbl in rrItem.MemTableFlushList
                                              let startTime = memTbl.StartTime
                                              orderby startTime ascending
                                              select new { StartTime = startTime, MemTbl = memTbl }))
                        {
                            newDataRow = dtReadRepair.NewRow();

                            newDataRow["Start Timestamp"] = item.StartTime;
                            newDataRow["Session Path"] = rrItem.SessionPath;
                            newDataRow["Data Center"] = item.MemTbl.DataCenter;
                            newDataRow["Node IPAddress"] = item.MemTbl.IPAddress;
                            newDataRow["Type"] = item.MemTbl.Type;
                            newDataRow["Log/Completion Timestamp"] = item.MemTbl.CompletionTime;
                            newDataRow["Session"] = rrItem.Session;
                            newDataRow["KeySpace"] = item.MemTbl.Keyspace;
                            newDataRow["Table"] = item.MemTbl.Table;
                            newDataRow["Occurrences"] = item.MemTbl.OccurrenceCount;
                            newDataRow["Flushed to SSTable(s) Size (mb)"] = item.MemTbl.FlushedStorage;
                            newDataRow["Flush Time (ms)"] = item.MemTbl.Duration;
                            newDataRow["Write Rate (ops)"] = item.MemTbl.NbrWriteOPS;
                            newDataRow["Effective Flush IORate (mb/sec)"] = item.MemTbl.IORate;
                            newDataRow["Reconciliation Reference"] = item.MemTbl.GroupIndicator;

                            dtReadRepair.Rows.Add(newDataRow);
                        }
                    }

                    if (rrItem.PerformanceWarningList != null)
                    {
                        foreach (var item in (from perfItem in rrItem.PerformanceWarningList
                                              let startTime = perfItem.StartTime
                                              orderby startTime ascending
                                              select new { StartTime = startTime, PerfItem = perfItem }))
                        {
                            newDataRow = dtReadRepair.NewRow();

                            newDataRow["Start Timestamp"] = item.StartTime;
                            newDataRow["Session Path"] = rrItem.SessionPath;
                            newDataRow["Data Center"] = item.PerfItem.DataCenter;
                            newDataRow["Node IPAddress"] = item.PerfItem.IPAddress;
                            newDataRow["Type"] = "Performance Warning(" + item.PerfItem.Type + ")";
                            newDataRow["Log/Completion Timestamp"] = item.PerfItem.LogTimestamp;
                            newDataRow["Session"] = rrItem.Session;
                            newDataRow["KeySpace"] = item.PerfItem.Keyspace;
                            newDataRow["Table"] = item.PerfItem.Table;
                            newDataRow["Reconciliation Reference"] = item.PerfItem.GroupIndicator;
                            newDataRow["Perf Warnings"] = 1;
                            newDataRow["Perf Average Latency"] = item.PerfItem.Latency;

                            dtReadRepair.Rows.Add(newDataRow);
                        }
                    }

                    if (rrItem.SolrReIndexing != null)
                    {
                        foreach (var item in (from solrIdx in rrItem.SolrReIndexing
                                              orderby solrIdx.StartTime ascending
                                              select solrIdx))
                        {
                            newDataRow = dtReadRepair.NewRow();

                            newDataRow["Start Timestamp"] = item.StartTime;
                            newDataRow["Session Path"] = rrItem.SessionPath;
                            newDataRow["Data Center"] = item.DataCenter;
                            newDataRow["Node IPAddress"] = item.IPAddress;
                            newDataRow["Type"] = "Solr ReIndex";
                            newDataRow["Log/Completion Timestamp"] = item.CompletionTime;
                            newDataRow["Session"] = rrItem.Session;
                            newDataRow["KeySpace"] = item.Keyspace;
                            newDataRow["Table"] = item.Table;
                            newDataRow["Solr ReIdx Duration"] = item.Duration;
                            newDataRow["Reconciliation Reference"] = item.GroupIndicator;

                            dtReadRepair.Rows.Add(newDataRow);
                        }
                    }

                    if (rrItem.SolrHardCommits != null)
                    {
                        foreach (var item in (from solrHdrCmt in rrItem.SolrHardCommits
                                              orderby solrHdrCmt.StartTime ascending
                                              select solrHdrCmt))
                        {
                            newDataRow = dtReadRepair.NewRow();

                            newDataRow["Start Timestamp"] = item.StartTime;
                            newDataRow["Session Path"] = rrItem.SessionPath;
                            newDataRow["Data Center"] = item.DataCenter;
                            newDataRow["Node IPAddress"] = item.IPAddress;
                            newDataRow["Type"] = "Solr Hard Commit";
                            newDataRow["Log/Completion Timestamp"] = item.CompletionTime;
                            newDataRow["Session"] = rrItem.Session;
                            newDataRow["KeySpace"] = item.Keyspace;
                            newDataRow["Table"] = string.Join(", ", item.SolrIndexes.Select(i => i.Item1 + '.' + i.Item2));
                            newDataRow["Solr Hard Commit Duration"] = item.Duration;
                            newDataRow["Reconciliation Reference"] = item.GroupIndicator;

                            dtReadRepair.Rows.Add(newDataRow);
                        }
                    }
                }
            }
        }


        public static void ReleaseGlobalLogCollections(bool clearComponentDisabled = true)
        {
            GCOccurrences.Clear();
            CompactionOccurrences.Clear();
            SolrReindexingOccurrences.Clear();
            MemTableFlushOccurrences.Clear();
            PerformanceOccurrences.Clear();
            SolrHardCommits.Clear();
            if(clearComponentDisabled) ComponentDisabled.Clear();
        }

        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,553  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.cardticketmap on 0/[] sstables
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,553  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.mapp2pidtouseridreadmodel on 3/[BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-105-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-22-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-17-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-91-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-104-big-Data.db')] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,554  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,554  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.blobreadmodel on 0/[] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,554  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,554  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-105-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,555  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.schemaversions on 0/[BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/schemaversions-e8647090a43411e6a2a7f19e9c4c25c4/mc-6-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/schemaversions-e8647090a43411e6a2a7f19e9c4c25c4/mc-5-big-Data.db')] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,555  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,555  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.paymentidorderidmap on 0/[] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,556  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,556  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.mapuseridtop2pidreadmodel on 3/[BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-105-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-22-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-104-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-17-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-91-big-Data.db')] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,557  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-104-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,557  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-104-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,558  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-105-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,559  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-91-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,560  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapuseridtop2pidreadmodel-ef77a872a43411e6b05641bd36123114/mc-91-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,562  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,562  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.mapp2paccountidtotestksnumberreadmodel on 0/[] sstables
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,562  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,563  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.symmetrickeyinfomodel on 0/[] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,563  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,563  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.accountnumbertoprivateaccountidreadmodel on 0/[] sstables
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,563  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,563  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.mapp2paccountidtouseridreadmodel on 0/[] sstables
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,564  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:46,564  CompactionManager.java:578 - Completed anticompaction successfully

        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:49,591  CompactionManager.java:511 - Starting anticompaction for testks_usersettings.action_requests on 0/[] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:49,591  CompactionManager.java:511 - Starting anticompaction for testks_usersettings.schemaversions on 1/[BigTableReader(path='/var/lib/cassandra/data/testks_usersettings/schemaversions-5f8694d0baf811e6a2a7f19e9c4c25c4/mc-1-big-Data.db')] sstables
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:49,592  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_usersettings/schemaversions-5f8694d0baf811e6a2a7f19e9c4c25c4/mc-1-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:49,592  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:49,592  CompactionManager.java:511 - Starting anticompaction for testks_usersettings.action_requests_by_userid on 0/[] sstables
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:49,593  CompactionManager.java:578 - Completed anticompaction successfully
        //INFO  [CompactionExecutor:659] 2016-12-11 03:09:49,593  CompactionManager.java:578 - Completed anticompaction successfully

        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,553  CompactionManager.java:511 - Starting anticompaction for testks_synchronization.mapp2pidtouseridreadmodel on 3/[BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-105-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-22-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-17-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-91-big-Data.db'), BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-104-big-Data.db')] sstables
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,554  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-105-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,557  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-104-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,559  CompactionManager.java:540 - SSTable BigTableReader(path='/var/lib/cassandra/data/testks_synchronization/mapp2pidtouseridreadmodel-ebdbe410a43411e6b05641bd36123114/mc-91-big-Data.db') fully contained in range (-9223372036854775808,-9223372036854775808], mutating repairedAt instead of anticompacting
        //INFO  [CompactionExecutor:658] 2016-12-11 03:09:46,562  CompactionManager.java:578 - Completed anticompaction successfully

        //INFO  [AntiCompactionExecutor:12] 2017-07-02 09:05:42,444 CompactionManager.java:1226 - Performing anticompaction on 1 sstables
        //INFO  [AntiCompactionExecutor:12] 2017-07-02 09:05:42,444 CompactionManager.java:1263 - Anticompacting[BigTableReader(path = '/var/lib/cassandra/data/product_v2/schema_updates-6c7c3d50fe0211e6a001c12716c256ee/mc-21-big-Data.db')]
        //INFO  [AntiCompactionExecutor:12] 2017-07-02 09:05:42,471 CompactionManager.java:1242 - Anticompaction completed successfully, anticompacted from 0 to 1 sstable(s).

        //INFO  [AntiCompactionExecutor:12] 2017-07-02 09:05:42,513 CompactionManager.java:1226 - Performing anticompaction on 3 sstables
        //INFO  [AntiCompactionExecutor:12] 2017-07-02 09:05:42,513 CompactionManager.java:1263 - Anticompacting[BigTableReader(path = '/var/lib/cassandra/data/product_v2/clientproducts_by_client_class_cpk_view-852430b0fe0211e6acbe19d8c614acf1/mc-404-big-Data.db'), BigTableReader(path = '/var/lib/cassandra/data/product_v2/clientproducts_by_client_class_cpk_view-852430b0fe0211e6acbe19d8c614acf1/mc-397-big-Data.db')]
        //INFO  [AntiCompactionExecutor:12] 2017-07-02 09:05:48,762 CompactionManager.java:1263 - Anticompacting[BigTableReader(path = '/var/lib/cassandra/data/product_v2/clientproducts_by_client_class_cpk_view-852430b0fe0211e6acbe19d8c614acf1/mc-393-big-Data.db')]
        //INFO  [AntiCompactionExecutor:12] 2017-07-02 09:05:48,801 CompactionManager.java:1242 - Anticompaction completed successfully, anticompacted from 0 to 4 sstable(s).

        static Regex RegExAntiCompStarting = new Regex(@"^Starting\s+anticompaction\s+for\s+([a-z0-9'-_$%+=@!?<>^*&]+)\.([a-z0-9'-_$%+=@!?<>^*&]+)\s+on\s+(\d+)\/",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExAntiCompRange = new Regex(@"^SSTable\s+BigTableReader(?:\(path='[0-9a-z\-_/.]+'\))\sfully\scontained\sin\s+range\s*\(\s*([0-9-]+)\,\s*([0-9-]+)\s*\]",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static Regex RegExAntiCompStarting2 = new Regex(@"^\s*Anticompacting\s*\[(?:\s*BigTableReader\s*\(\s*path\s*\=\s*'([^\'\)]+)'\s*\)\s*\,?)+\s*\]",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static bool HasAntiCompactions = false;

        public static void ParseAntiCompactionFromLog(DataTable dtroCLog,
                                                        DataTable dtCompactionHist,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
                                                        IEnumerable<string> ignoreKeySpaces)
        {
            if (dtroCLog.Rows.Count == 0)
            {
                return;
            }

            Logger.Instance.Info("Begin Review of AntiCompacation from logs");

            ulong recNbr = 0;
            var globalAntiCompactions = CompactionOccurrences; //ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, ThreadSafe.List<ICompactionLogInto>>
            {
                IReadOnlyDictionary<string, IEnumerable<Tuple<DateTime, decimal, decimal>>> compHistSizeInfo = null;

                if (dtCompactionHist != null && dtCompactionHist.Rows.Count > 0)
                {
                    compHistSizeInfo = (from drCompHist in dtCompactionHist.AsEnumerable()
                                        let dc = drCompHist.Field<string>("Data Center")
                                        let nodeIP = drCompHist.Field<string>("Node IPAddress")
                                        let ks = drCompHist.Field<string>("KeySpace")
                                        let tbl = drCompHist.Field<string>("Table")
                                        group drCompHist by new { DC = dc, Node = nodeIP, Keyspace = ks, Table = tbl } into g
                                        select new
                                        {
                                            Key = g.Key.DC + '|' + g.Key.Node + '|' + g.Key.Keyspace + '|' + g.Key.Table,
                                            Info = (from dr in g
                                                    let timestamp = dr.Field<DateTime>("Compaction Timestamp (Node TZ)")
                                                    orderby timestamp ascending
                                                    select new Tuple<DateTime, decimal, decimal>
                                                                (timestamp,
                                                                    dr.Field<decimal>("Before Size (MB)"),
                                                                    dr.Field<decimal>("After Size (MB)")))
                                        }).ToDictionary(i => i.Key, i => i.Info);
                }

                var antiCompactionLogItems = from dr in dtroCLog.AsEnumerable()
                                             let dcName = dr.Field<string>("Data Center")
                                             let ipAddress = dr.Field<string>("Node IPAddress")
                                             let timestamp = dr.Field<DateTime>("Timestamp")
                                             let descr = dr.Field<string>("Description")?.Trim()
                                             let taskId = dr.Field<int?>("TaskId")
                                             let task = dr.Field<string>("Task")
                                             where (task == "CompactionExecutor"
                                                        || task == "AntiCompactionExecutor")
                                                    && dr.Field<string>("Item") == "CompactionManager.java"
                                                    && taskId.HasValue
                                             group new
                                             {
                                                 Timestamp = timestamp,
                                                 Description = descr,
                                                 RecordNbr = ++recNbr
                                             }
                                                by new { dcName, ipAddress, task, taskId.Value } into g
                                             select new
                                             {
                                                 DCName = g.Key.dcName,
                                                 IPAddress = g.Key.ipAddress,
                                                 Task = g.Key.task,
                                                 TaskId = g.Key.Value,
                                                 LogItems = (from l in g orderby l.Timestamp ascending, l.RecordNbr ascending select l)
                                             };

                Parallel.ForEach(antiCompactionLogItems, logGroupItem =>
                //foreach (var logGroupItem in antiCompactionLogItems)
                {
                    AntiCompactionLogInfo currentAntiCompaction = null;
                    Common.Patterns.Collections.ThreadSafe.List<ICompactionLogInfo> antiCompactionDCNodeList = null;
                    var localAntiCompList = new List<AntiCompactionLogInfo>();
                    var groupIndicator = CLogSummaryInfo.IncrementGroupInicator();

                    globalAntiCompactions.AddOrUpdate((logGroupItem.DCName == null ? string.Empty : logGroupItem.DCName) + "|" + logGroupItem.IPAddress,
                                                        ignore =>
                                                            {
                                                                return antiCompactionDCNodeList = new Common.Patterns.Collections.ThreadSafe.List<ICompactionLogInfo>();
                                                            },
                                                        (ignore, gcList) =>
                                                            {
                                                                return antiCompactionDCNodeList = gcList;
                                                            });

                    Logger.Instance.InfoFormat("Start Review of AntiCompacation for {0}|{1} {2}.{3} with {4} items",
                                                    logGroupItem.DCName,
                                                    logGroupItem.IPAddress,
                                                    logGroupItem.Task,
                                                    logGroupItem.TaskId,
                                                    logGroupItem.LogItems.Count());

                    foreach (var item in logGroupItem.LogItems)
                    {
                        if (logGroupItem.Task == "AntiCompactionExecutor")
                        {
                            #region AntiCompactionExecutor
                            if (item.Description.StartsWith("anticompaction completed", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if (currentAntiCompaction != null)
                                {
                                    #region Completed

                                    currentAntiCompaction.CompletionTime = item.Timestamp;
                                    antiCompactionDCNodeList.Add(currentAntiCompaction);

                                    #endregion
                                    currentAntiCompaction = null;
                                }
                            }
                            else
                            {
                                var startingSplit = RegExAntiCompStarting2.Split(item.Description);

                                if (startingSplit.Length == 3)
                                {
                                    #region Starting
                                    var ssTable = RemoveQuotes(startingSplit[1].Trim());
                                    var ksTblItem = DSEDiagnosticLibrary.StringHelpers.ParseSSTableFileIntoKSTableNames(ssTable);

                                    if (currentAntiCompaction != null
                                            && (currentAntiCompaction.Keyspace != ksTblItem.Item1
                                                    || currentAntiCompaction.Table != ksTblItem.Item2))
                                    {
                                        currentAntiCompaction.Aborted = true;
                                        currentAntiCompaction.CompletionTime = item.Timestamp;
                                        currentAntiCompaction = null;
                                    }

                                    if (ignoreKeySpaces.Contains(ksTblItem.Item1))
                                    {
                                        currentAntiCompaction = null;
                                    }
                                    else if (currentAntiCompaction != null)
                                    {
                                        currentAntiCompaction.SSTables += item.Description.Count(c => c == ',') + 1;
                                    }
                                    else
                                    {
                                        currentAntiCompaction = new AntiCompactionLogInfo()
                                        {
                                            DataCenter = logGroupItem.DCName,
                                            IPAddress = logGroupItem.IPAddress,
                                            StartTime = item.Timestamp,
                                            Keyspace = ksTblItem.Item1,
                                            Table = ksTblItem.Item2,
                                            SSTables = item.Description.Count(c => c == ',') + 1,
                                            GroupIndicator = groupIndicator
                                        };
                                        localAntiCompList.Add(currentAntiCompaction);
                                    }
                                    #endregion
                                }
                            }
                            #endregion
                        }
                        else
                        {
                            #region CompactionExecutor
                            if (item.Description.StartsWith("completed anticompaction", StringComparison.CurrentCultureIgnoreCase))
                            {
                                if (currentAntiCompaction != null)
                                {
                                    #region Completed

                                    currentAntiCompaction.CompletionTime = item.Timestamp;
                                    antiCompactionDCNodeList.Add(currentAntiCompaction);

                                    #endregion
                                    currentAntiCompaction = null;
                                }
                            }
                            else
                            {
                                var startingSplit = RegExAntiCompStarting.Split(item.Description);

                                if (startingSplit.Length == 5)
                                {
                                    #region Starting
                                    var keySpace = RemoveQuotes(startingSplit[1].Trim());

                                    if (currentAntiCompaction != null)
                                    {
                                        currentAntiCompaction.Aborted = true;
                                        currentAntiCompaction.CompletionTime = item.Timestamp;
                                    }

                                    if (ignoreKeySpaces.Contains(keySpace))
                                    {
                                        currentAntiCompaction = null;
                                    }
                                    else
                                    {
                                        currentAntiCompaction = new AntiCompactionLogInfo()
                                        {
                                            DataCenter = logGroupItem.DCName,
                                            IPAddress = logGroupItem.IPAddress,
                                            StartTime = item.Timestamp,
                                            Keyspace = keySpace,
                                            Table = RemoveQuotes(startingSplit[2].Trim()),
                                            SSTables = string.IsNullOrEmpty(startingSplit[3].Trim()) ? 0 : int.Parse(startingSplit[3]),
                                            GroupIndicator = groupIndicator
                                        };
                                        localAntiCompList.Add(currentAntiCompaction);
                                    }
                                    #endregion
                                }
                                else if (currentAntiCompaction != null)
                                {
                                    var rangeSplit = RegExAntiCompRange.Split(item.Description);

                                    if (rangeSplit.Length == 4)
                                    {
                                        currentAntiCompaction.AddRange(rangeSplit[1], rangeSplit[2]);
                                    }
                                }
                            }
                            #endregion
                        }
                    }

                    if (currentAntiCompaction != null)
                    {
                        currentAntiCompaction.Aborted = true;
                        currentAntiCompaction = null;
                    }

                    if (localAntiCompList.Count > 0)
                    {
                        HasAntiCompactions = true;

                        #region Determine Compacted Sizes from Compaction History

                        if (compHistSizeInfo != null && compHistSizeInfo.Count > 0)
                        {
                            foreach (var antiComp in localAntiCompList.Where(i => !i.Aborted))
                            {
                                IEnumerable<Tuple<DateTime, decimal, decimal>> compTSSizeInfo;

                                if (compHistSizeInfo.TryGetValue(antiComp.DataCenter + '|' + antiComp.IPAddress + '|' + antiComp.Keyspace + '|' + antiComp.Table, out compTSSizeInfo))
                                {
                                    var sizeItem = compTSSizeInfo.FirstOrDefault(i => i.Item1 >= antiComp.StartTime && i.Item1 <= antiComp.CompletionTime);

                                    if (sizeItem != null)
                                    {
                                        antiComp.OldSize = sizeItem.Item2;
                                        antiComp.NewSize = sizeItem.Item3;
                                    }
                                }
                            }
                        }

                        #endregion

                        #region Log Status DT
                        if (dtLogStatusStack != null)
                        {
                            var dtCStatusLog = new DataTable(string.Format("NodeStatus-AntiCompaction-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));
                            InitializeStatusDataTable(dtCStatusLog);
                            dtLogStatusStack.Push(dtCStatusLog);

                            foreach (var antiComp in localAntiCompList)
                            {
                                var dtCStatusLogRow = dtCStatusLog.NewRow();
                                dtCStatusLogRow["Reconciliation Reference"] = antiComp.GroupIndicator;
                                dtCStatusLogRow["Timestamp"] = antiComp.StartTime;
                                dtCStatusLogRow["Data Center"] = antiComp.DataCenter;
                                dtCStatusLogRow["Node IPAddress"] = antiComp.IPAddress;
                                dtCStatusLogRow["Pool/Cache Type"] = "AntiCompaction";
                                dtCStatusLogRow["KeySpace"] = antiComp.Keyspace;
                                dtCStatusLogRow["Table"] = antiComp.Table;
                                dtCStatusLogRow["SSTables"] = antiComp.SSTables;
                                dtCStatusLogRow["Latency (ms)"] = antiComp.Duration;
                                if (antiComp.TokenRanges != null && antiComp.TokenRanges.Count > 0)
                                {
                                    dtCStatusLogRow["Start Token Range (exclusive)"] = string.Join(",", antiComp.TokenRanges.Select(i => i.Item1));
                                    dtCStatusLogRow["End Token Range (inclusive)"] = string.Join(",", antiComp.TokenRanges.Select(i => i.Item2));
                                }
                                if (antiComp.Aborted)
                                {
                                    dtCStatusLogRow["Aborted"] = 1;
                                }

                                if (antiComp.IORate > 0)
                                {
                                    dtCStatusLogRow["Rate (MB/s)"] = antiComp.IORate;
                                }

                                dtCStatusLog.Rows.Add(dtCStatusLogRow);
                            }
                        }
                        #endregion

                        #region CFStats DT
                        if (dtCFStatsStack != null)
                        {
                            var dtCFStats = new DataTable(string.Format("CFStats-AntiCompaction-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));
                            initializeCFStatsDataTable(dtCFStats);
                            dtCFStatsStack.Push(dtCFStats);

                            //AntiCompaction maximum latency
                            //AntiCompaction mean latency
                            //AntiCompaction minimum latency
                            //AntiCompaction occurrences
                            //AntiCompaction SSTable count

                            var grpStats = from item in localAntiCompList
                                           group new { Latency = item.Duration, SSTables = item.SSTables, Rate = item.IORate, GrpInd = item.GroupIndicator }
                                                      by new { item.DataCenter, item.IPAddress, item.Keyspace, item.Table } into g
                                           let latencyEnum = g.Select(i => i.Latency)
                                           let latencyEnum1 = latencyEnum.Where(i => i > 0).DefaultIfEmpty()
                                           let rateEnum = g.Where(i => i.Rate > 0).Select(i => i.Rate).DefaultIfEmpty()
                                           select new
                                           {
                                               Max = latencyEnum.Max(),
                                               Min = latencyEnum1.Min(),
                                               Mean = latencyEnum1.Average(),
                                               RateMax = rateEnum.Max(),
                                               RateMin = rateEnum.Min(),
                                               RateMean = rateEnum.Average(),
                                               RateOccurrences = rateEnum.Count(),
                                               SSTables = g.Sum(i => i.SSTables),
                                               GrpInds = string.Join(",", g.Select(i => i.GrpInd).DuplicatesRemoved(i => i)),
                                               Count = g.Count(),
                                               DCName = g.Key.DataCenter,
                                               IPAddress = g.Key.IPAddress,
                                               KeySpace = g.Key.Keyspace,
                                               Table = g.Key.Table
                                           };

                            foreach (var stats in grpStats)
                            {
                                var dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction maximum latency";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                if (stats.Max > 0)
                                {
                                    dataRow["Value"] = stats.Max;
                                    dataRow["(Value)"] = stats.Max;
                                }
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction mean latency";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                if (stats.Mean > 0)
                                {
                                    dataRow["Value"] = stats.Mean;
                                    dataRow["(Value)"] = stats.Mean;
                                }
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction minimum latency";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                if (stats.Min > 0)
                                {
                                    dataRow["Value"] = stats.Min;
                                    dataRow["(Value)"] = stats.Min;
                                }
                                dataRow["Unit of Measure"] = "ms";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction occurrences";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                dataRow["Value"] = stats.Count;
                                dataRow["(Value)"] = stats.Count;
                                //dataRow["Unit of Measure"] = "";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction SSTable count";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                dataRow["Value"] = stats.SSTables;
                                dataRow["(Value)"] = stats.SSTables;
                                //dataRow["Unit of Measure"] = "";

                                dtCFStats.Rows.Add(dataRow);

                                //AntiCompaction maximum rate
                                //AntiCompaction mean rate
                                //AntiCompaction minimum rate

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction maximum rate";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                if (stats.RateMax > 0)
                                {
                                    dataRow["Value"] = stats.RateMax;
                                    dataRow["(Value)"] = stats.RateMax;
                                }
                                dataRow["Unit of Measure"] = "mb/sec";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction mean rate";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                if (stats.RateMean > 0)
                                {
                                    dataRow["Value"] = stats.RateMean;
                                    dataRow["(Value)"] = stats.RateMean;
                                }
                                dataRow["Unit of Measure"] = "mb/sec";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction minimum rate";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                if (stats.RateMin > 0)
                                {
                                    dataRow["Value"] = stats.RateMin;
                                    dataRow["(Value)"] = stats.RateMin;
                                }
                                dataRow["Unit of Measure"] = "mb/sec";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = stats.DCName;
                                dataRow["Node IPAddress"] = stats.IPAddress;
                                dataRow["KeySpace"] = stats.KeySpace;
                                dataRow["Table"] = stats.Table;
                                dataRow["Attribute"] = "AntiCompaction rate occurrences";
                                dataRow["Reconciliation Reference"] = stats.GrpInds;
                                dataRow["Value"] = stats.RateOccurrences;
                                dataRow["(Value)"] = stats.RateOccurrences;
                                //dataRow["Unit of Measure"] = "";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }
                        #endregion
                    }

                    Logger.Instance.InfoFormat("Completed Review of AntiCompacation for {0}|{1} {2}.{3}",
                                                    logGroupItem.DCName,
                                                    logGroupItem.IPAddress,
                                                    logGroupItem.Task,
                                                    logGroupItem.TaskId);
                });
            }//end of scope for antiCompactionLogItems

            Logger.Instance.Info("Completed Review of AntiCompacation from logs");
        }

        //INFO[SlabPoolCleaner] 2016-09-11 16:44:55,289  ColumnFamilyStore.java:1211 - Flushing largest CFS(Keyspace= 'homeKS', ColumnFamily= 'homebase_tasktracking_ops_l3') to free up room.Used total: 0.33/0.00, live: 0.33/0.00, flushing: 0.00/0.00, this: 0.07/0.07
        //INFO[SlabPoolCleaner] 2016-09-11 16:44:55,289  ColumnFamilyStore.java:905 - Enqueuing flush of homebase_tasktracking_ops_l3: 315219514 (7%) on-heap, 0 (0%) off-heap
        //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:55,290  Memtable.java:347 - Writing Memtable-homebase_tasktracking_ops_l3@994827943(53.821MiB serialized bytes, 857621 ops, 7%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:56,558  Memtable.java:382 - Completed flushing /mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3-tmp-ka-15175-Data.db (11.901MiB) for commitlog position ReplayPosition(segmentId= 1473433813485, position= 31065241)
        //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:56,619  Memtable.java:347 - Writing Memtable-homebase_tasktracking_ops_l3.tasktracking_l3_idx1@2015847477(1.824MiB serialized bytes, 65562 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:56,684  Memtable.java:382 - Completed flushing /mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3.tasktracking_l3_idx1-tmp-ka-15175-Data.db (607.611KiB) for commitlog position ReplayPosition(segmentId= 1473433813485, position= 31065241)
        //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:56,702  Memtable.java:347 - Writing Memtable-homebase_tasktracking_ops_l3.tasktracking_l3_idx2@649911595(1.816MiB serialized bytes, 68460 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:1169] 2016-09-11 16:44:56,766  Memtable.java:382 - Completed flushing /mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3.tasktracking_l3_idx2-tmp-ka-15170-Data.db (591.472KiB) for commitlog position ReplayPosition(segmentId= 1473433813485, position= 31065241)

        //INFO  [SlabPoolCleaner] 2016-09-12 10:40:18,320  ColumnFamilyStore.java:1211 - Flushing largest CFS(Keyspace='testks', ColumnFamily='opus_ln_borrinfo') to free up room. Used total: 0.33/0.00, live: 0.33/0.00, flushing: 0.00/0.00, this: 0.02/0.02
        //INFO[SlabPoolCleaner] 2016-09-12 10:40:18,320  ColumnFamilyStore.java:905 - Enqueuing flush of opus_ln_borrinfo: 81624365 (2%) on-heap, 0 (0%) off-heap
        //INFO[MemtableFlushWriter:1305] 2016-09-12 10:40:18,323  Memtable.java:347 - Writing Memtable-opus_ln_borrinfo@771558518(12.749MiB serialized bytes, 919772 ops, 2%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:1305] 2016-09-12 10:40:18,978  Memtable.java:382 - Completed flushing /mnt/dse/data1/testks/opus_ln_borrinfo-d7f40650ec5411e5bca01f8c6828163a/testks-opus_ln_borrinfo-tmp-ka-1533-Data.db (4.631MiB) for commitlog position ReplayPosition(segmentId= 1473454297127, position= 20482314)
        //INFO[MemtableFlushWriter:1305] 2016-09-12 10:40:19,024  Memtable.java:347 - Writing Memtable-opus_ln_borrinfo.opus_ln_borrinfo_flag@1190857189(24.898KiB serialized bytes, 4724 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:1305] 2016-09-12 10:40:19,027  Memtable.java:382 - Completed flushing /mnt/dse/data1/testks/opus_ln_borrinfo-d7f40650ec5411e5bca01f8c6828163a/testks-opus_ln_borrinfo.opus_ln_borrinfo_flag-tmp-ka-1622-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId= 1473454297127, position= 20482314)

        //INFO[ScheduledTasks:1] 2016-10-25 04:07:19,781  ColumnFamilyStore.java:917 - Enqueuing flush of peers: 3036 (0%) on-heap, 35032 (0%) off-heap
        //INFO[MemtableFlushWriter:1698] 2016-10-25 04:07:19,782  Memtable.java:347 - Writing Memtable-peers@2118996997(0.473KiB serialized bytes, 1344 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:1698] 2016-10-25 04:07:19,783  Memtable.java:382 - Completed flushing /data/3/dse/data/system/peers-37f71aca7dc2383ba70672528af04d4f/system-peers-tmp-ka-404-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId= 1476849887341, position= 1392720)

        //INFO[ValidationExecutor:82] 2016-10-24 18:31:32,529  ColumnFamilyStore.java:917 - Enqueuing flush of rtics_inquiry: 956 (0%) on-heap, 964 (0%) off-heap
        //INFO[MemtableFlushWriter:1522] 2016-10-24 18:31:32,530  Memtable.java:347 - Writing Memtable-rtics_inquiry@694978413(0.735KiB serialized bytes, 23 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:1522] 2016-10-24 18:31:32,531  Memtable.java:382 - Completed flushing /data/2/dse/data/prod_fcra/rtics_inquiry-9af27501901611e6a0d90dbb03ae0b81/prod_fcra-rtics_inquiry-tmp-ka-430-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId= 1476849887309, position= 1639529)

        //DEBUG [ValidationExecutor:33] 2017-06-29 05:14:36,413  ColumnFamilyStore.java:850 - Enqueuing flush of pymt_soc_by_setlid: 102357 (0%) on-heap, 0 (0%) off-heap
        //DEBUG[MemtableFlushWriter:167] 2017-06-29 05:14:36,413  Memtable.java:368 - Writing Memtable-pymt_soc_by_setlid@1578363409(16.938KiB serialized bytes, 79 ops, 0%/0% of on/off-heap limit)
        //DEBUG[MemtableFlushWriter:167] 2017-06-29 05:14:36,415  Memtable.java:401 - Completed flushing /apps/cassandra/data/data1/cksprocp1/pymt_soc_by_setlid-dc3a4960f00c11e6b7494d6a36aa5e20/mc-12525-big-Data.db (8.132KiB) for commitlog position ReplayPosition(segmentId= 1498723221746, position= 14670565)

        //DEBUG[MemtableFlushWriter:23] 2017-08-11 13:03:04,864  Memtable.java:364 - Writing Memtable-logmnemonicrecentvalue@1160967395(268.396KiB serialized bytes, 3174 ops, 0%/0% of on/off-heap limit)
        //DEBUG[MemtableFlushWriter:23] 2017-08-11 13:03:04,872  Memtable.java:397 - Completed flushing /d5/data/rts_data/logmnemonicrecentvalue-bb61e781f7d111e692c82747a9704109/mc-7750112-big-Data.db (95.876KiB) for commitlog position ReplayPosition(segmentId= 1502112368275, position= 18085469)


        //INFO  [BatchlogTasks:1] 2017-01-18 08:41:09,879  ColumnFamilyStore.java:905 - Enqueuing flush of batchlog: 116840 (0%) on-heap, 0 (0%) off-heap
        //INFO[MemtableFlushWriter:16472] 2017-01-18 08:41:09,879  Memtable.java:347 - Writing Memtable-batchlog@1865343912(61.708KiB serialized bytes, 425 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:16472] 2017-01-18 08:41:09,880  Memtable.java:393 - Completed flushing /var/lib/cassandra/data/system/batchlog-0290003c977e397cac3efdfdc01d626b/system-batchlog-tmp-ka-46277-Data.db; nothing needed to be retained.Commitlog position was ReplayPosition(segmentId= 1483652760453, position= 4126122)

        //INFO  [ValidationExecutor:306] 2017-01-16 08:07:41,862  ColumnFamilyStore.java:905 - Enqueuing flush of inode: 971 (0%) on-heap, 0 (0%) off-heap
        //INFO[MemtableFlushWriter:340] 2017-01-16 08:07:41,862  Memtable.java:347 - Writing Memtable-inode.cfs_parent_path@663956474(0.051KiB serialized bytes, 2 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:340] 2017-01-16 08:07:41,863  Memtable.java:382 - Completed flushing /var/lib/cassandra/data/cfs/inode-76298b94ca5f375cab5bb674eddd3d51/cfs-inode.cfs_parent_path-tmp-ka-71-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId= 1484061347184, position= 32293737)
        //INFO[MemtableFlushWriter:340] 2017-01-16 08:07:41,872  Memtable.java:347 - Writing Memtable-inode.cfs_path@911188026(0.051KiB serialized bytes, 1 ops, 0%/0% of on/off-heap limit)
        //INFO[MemtableFlushWriter:340] 2017-01-16 08:07:41,873  Memtable.java:382 - Completed flushing /var/lib/cassandra/data/cfs/inode-76298b94ca5f375cab5bb674eddd3d51/cfs-inode.cfs_path-tmp-ka-107-Data.db (0.000KiB) for commitlog position ReplayPosition(segmentId= 1484061347184, position= 32293737)

        //INFO [ValidationExecutor:221] 2017-03-06 17:40:28,151 ColumnFamilyStore.java (line 808) Enqueuing flush of Memtable-user_ids.user_ids_gid@1844633142(189/1890 serialized/live bytes, 7 ops)
        //INFO[FlushWriter:263] 2017-03-06 17:40:28,151 Memtable.java (line 362) Writing Memtable-user_ids.user_ids_gid@1844633142(189/1890 serialized/live bytes, 7 ops)
        //INFO[FlushWriter:263] 2017-03-06 17:40:28,178 Memtable.java (line 402) Completed flushing /var/lib/cassandra/data/user_id_data/user_ids/user_id_data-user_ids.user_ids_gid-jb-3492-Data.db (501 bytes) for commitlog position ReplayPosition(segmentId= 1488732865093, position= 31029258)

        //INFO[MemtablePostFlush:22429] 2017-05-19 08:51:41,572  ColumnFamilyStore.java:1006 - Flushing SecondaryIndex Cql3SolrSecondaryIndex{columnDefs=[ColumnDefinition{name=appownrtxt, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_appownrtxt_index, indexType=CUSTOM}, ColumnDefinition{name=cntrtrecvddt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_cntrtrecvddt_index, indexType=CUSTOM}, ColumnDefinition{name=appstatlkupid, type=org.apache.cassandra.db.marshal.Int32Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_appstatlkupid_index, indexType=CUSTOM}, ColumnDefinition{name=fundddt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_fundddt_index, indexType=CUSTOM}, ColumnDefinition{name=appstatdt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_appstatdt_index, indexType=CUSTOM}, ColumnDefinition{name=dcsnstatlkupid, type=org.apache.cassandra.db.marshal.Int32Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_dcsnstatlkupid_index, indexType=CUSTOM}, ColumnDefinition{name=chnlid, type=org.apache.cassandra.db.marshal.Int32Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_chnlid_index, indexType=CUSTOM}, ColumnDefinition{name=dcsndt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_dcsndt_index, indexType=CUSTOM}, ColumnDefinition{name=notecretddtfltr, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_notecretddtfltr_index, indexType=CUSTOM}, ColumnDefinition{name=cmtmentcd, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_cmtmentcd_index, indexType=CUSTOM}, ColumnDefinition{name=maxapptaskaudtcretddt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_maxapptaskaudtcretddt_index, indexType=CUSTOM}, ColumnDefinition{name=maxaudtcretddt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_maxaudtcretddt_index, indexType=CUSTOM}, ColumnDefinition{name=cntrtcretddt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_cntrtcretddt_index, indexType=CUSTOM}, ColumnDefinition{name=dlrshporgid, type=org.apache.cassandra.db.marshal.LongType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_dlrshporgid_index, indexType=CUSTOM}, ColumnDefinition{name=hrflag, type=org.apache.cassandra.db.marshal.BooleanType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_hrflag_index, indexType=CUSTOM}, ColumnDefinition{name=appcretddt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_appcretddt_index, indexType=CUSTOM}, ColumnDefinition{name=maxappasgnmtaudtdt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_maxappasgnmtaudtdt_index, indexType=CUSTOM}, ColumnDefinition{name=lobid, type=org.apache.cassandra.db.marshal.Int32Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_lobid_index, indexType=CUSTOM}, ColumnDefinition{name=apptaskaudtdtfltr, type=org.apache.cassandra.db.marshal.SetType(org.apache.cassandra.db.marshal.TimestampType), kind=REGULAR, componentIndex=0, indexName=coafstatim_application_apptaskaudtdtfltr_index, indexType=CUSTOM}, ColumnDefinition{name=apprecvddt, type=org.apache.cassandra.db.marshal.TimestampType, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_apprecvddt_index, indexType=CUSTOM}, ColumnDefinition{name=solr_query, type=org.apache.cassandra.db.marshal.UTF8Type, kind=REGULAR, componentIndex=0, indexName=coafstatim_application_solr_query_index, indexType=CUSTOM}]}
        //INFO[MemtablePostFlush:22429] 2017-05-19 08:51:41,573  AbstractSolrSecondaryIndex.java:1133 - Executing hard commit on index coafstatim.application
        //INFO[MemtablePostFlush:22429] 2017-05-19 08:51:41,573  IndexWriter.java:3429 - commitInternalStart startTime = 1495198301573
        //INFO[MemtablePostFlush:22429] 2017-05-19 08:51:41,770  IndexWriter.java:3454 - commitInternalComplete duration = 197 ms startTime = 1495198301573

        //Group1					Group2
        //"'homeKS'"	"'homebase_tasktracking_ops_l3'"
        static Regex RegexMFFlushing = new Regex(@"^Flushing[a-z ]+\(\s*Keyspace\s*=\s*([a-z0-9'-_$%+=@!?<>^*&]+)\s*\,\s*ColumnFamily\s*=\s*([a-z0-9'-_$%+=@!?<>^*&]+)",
                                                    RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //Enqueuing flush of inode: 971 (0%) on-heap, 0 (0%) off-heap
        //"inode"	"971"	"0"		"%"		"0"		"0"		"%"
        //
        //Enqueuing flush of Memtable-user_ids.user_ids_gid@1844633142(189/1890 serialized/live bytes, 7 ops)
        //"user_ids.user_ids_gid"	"1890"
        static Regex RegexMFEnqueuing = new Regex(@"^Enqueuing\s+flush\s+of\s+(?:Memtable\s*\-\s*([a-z0-9'-_$%+=@!?<>^*&]+)\@\d+|([a-z0-9'-_$%+=@!?<>^*&]+))\s*(?:\:\s+([0-9,.]+)\s+\(\s*([0-9,.]+)\s*(\%*)\s*\)\s+on-heap\s*,\s+([0-9,.]+)\s+\(\s*([0-9,.]+)\s*(\%*)\s*\)|\(([0-9,.]+))",
                                                    RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //Group1							Group2		Group3	    Group4
        //"homebase_tasktracking_ops_l3"	"53.821"	"MiB"|NULL	"85762"
        static Regex RegexMFWritingMemTbl = new Regex(@"^Writing\s+Memtable\s*\-\s*([a-z0-9'-_$%+=@!?<>^*&]+)\s*\@\d+\(([0-9.]+)\s*([a-z]{1,5})?/?\d*\s+serialized(?:/live)?\s+bytes\,\s+([0-9,.]+)\s+ops",
                                                        RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //Group1																																			Group2		    Group3
        //"/mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3-tmp-ka-15175-Data.db"	"11.901"|NULL	"MiB"|NULL
        static Regex RegexMFCompletedMemTbl = new Regex(@"^Completed\s+flushing\s+([0-9a-z\-_/.]+)(?:(?:\s+\(([0-9,.]+)\s*([a-z]{0,5}))|\;\s+nothing\s+needed[a-z ]+retained)",
                                                            RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public static void ParseMemTblFlushFromLog(DataTable dtCLog,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStack,
                                                        List<CKeySpaceTableNames> kstblExists,
                                                        IEnumerable<string> ignoreKeySpaces,
                                                        int flushFlagThresholdInMS,
                                                        decimal flushFlagThresholdAsIORate)
        {
            if (dtCLog.Rows.Count == 0)
            {
                return;
            }

            Logger.Instance.Info("Begin Parsing Memtable flushing from logs");

            ulong recNbr = 0;
            var flushLogItems = from dr in dtCLog.AsEnumerable()
                                let dcName = dr.Field<string>("Data Center")
                                let ipAddress = dr.Field<string>("Node IPAddress")
                                let flagged = dr.Field<int?>("Flagged")
                                let taskId = dr.Field<int?>("TaskId")
                                where flagged.HasValue && flagged == (int)LogFlagStatus.MemTblFlush
                                group new
                                {
                                    Timestamp = dr.Field<DateTime>("Timestamp"),
                                    Description = dr.Field<string>("Description")?.Trim(),
                                    Task = dr.Field<string>("Task"),
                                    TaskId = taskId,
                                    RecordNbr = ++recNbr,
                                    DataRow = dr
                                }
                                    by new { dcName, ipAddress } into g
                                select new
                                {
                                    DCName = g.Key.dcName,
                                    IPAddress = g.Key.ipAddress,
                                    LogItems = (from l in g orderby l.Timestamp ascending, l.RecordNbr ascending select l)
                                };

            Parallel.ForEach(flushLogItems, logGroupItem =>
            //foreach (var logGroupItem in flushLogItems)
            {
                var currentFlushes = new List<MemTableFlushLogInfo>();
                var groupIndicator = (decimal)CLogSummaryInfo.IncrementGroupInicator();

                Logger.Instance.InfoFormat("Start Parsing Memtable flushing for {0}|{1} with Memtable Log items {2}",
                                                logGroupItem.DCName,
                                                logGroupItem.IPAddress,
                                                logGroupItem.LogItems.Count());

                foreach (var item in logGroupItem.LogItems)
                {
                    #region Memtable Flush Processing
                    //New Flush Event
                    //Group1							Group2		Group3	Group4	Group5	Group6	Group 7
                    //"homebase_tasktracking_ops_l3"	"315219514"	"7"		"%"		"0"		"0"		"%"
                    var splitInfo = RegexMFEnqueuing.Split(item.Description);

                    if (splitInfo.Length > 1)
                    {
                        var flushInfo = new MemTableFlushLogInfo()
                        {
                            DataCenter = logGroupItem.DCName,
                            IPAddress = logGroupItem.IPAddress,
                            EnqueuingStart = item.Timestamp,
                            Type = item.Task,
                            GroupIndicator = groupIndicator += ReferenceIncrementValue,
                            Table = RemoveQuotes(splitInfo[1].Trim()),
                            LogDataRow = item.DataRow
                        };

                        currentFlushes.Where(i => !i.Completed
                                                    && i.Table == flushInfo.Table
                                                    && !i.Occurrences.IsEmpty()
                                                    && i.Occurrences.All(o => o.CompletionTime != DateTime.MinValue))
                                        .ForEach(i => i.Completed = true);
                        currentFlushes.Add(flushInfo);
                        continue;
                    }

                    //Group1							Group2		Group3	    Group4
                    //"homebase_tasktracking_ops_l3"	"53.821"	"MiB"|NULL	"857621"
                    splitInfo = RegexMFWritingMemTbl.Split(item.Description);

                    if (splitInfo.Length > 1)
                    {
                        var tableName = RemoveQuotes(splitInfo[1].Trim());
                        var flushInfo = currentFlushes.Find(i => (tableName == i.Table || tableName.StartsWith(i.Table + "."))
                                                                        && (i.TaskId == 0 || (i.TaskId == item.TaskId.Value && !i.Completed))
                                                                        && item.Timestamp >= i.StartTime);

                        if (flushInfo == null)
                        {
                            flushInfo = new MemTableFlushLogInfo()
                            {
                                DataCenter = logGroupItem.DCName,
                                IPAddress = logGroupItem.IPAddress,
                                EnqueuingStart = item.Timestamp,
                                Type = item.Task,
                                GroupIndicator = groupIndicator += ReferenceIncrementValue,
                                Table = tableName,
                                LogDataRow = item.DataRow
                            };
                            currentFlushes.Add(flushInfo);
                        }

                        flushInfo.AddUpdateOccurrence(tableName, item.Timestamp, int.Parse(splitInfo[4]), item.TaskId.Value, groupIndicator);
                        continue;
                    }

                    //Group1																																			Group2		Group3
                    //"/mnt/dse/data1/homeKS/homebase_tasktracking_ops_l3-737682f0599311e6ad0fa12fb1b6cb6e/homeKS-homebase_tasktracking_ops_l3-tmp-ka-15175-Data.db"	"11.901"	"MiB"
                    // /var/lib/cassandra/data/cfs/inode-76298b94ca5f375cab5bb674eddd3d51/cfs-inode.cfs_parent_path-tmp-ka-71-Data.db
                    splitInfo = RegexMFCompletedMemTbl.Split(item.Description);

                    if (splitInfo.Length > 1)
                    {
                        var ksItems = DSEDiagnosticLibrary.StringHelpers.ParseSSTableFileIntoKSTableNames(splitInfo[1]);

                        if (ksItems == null)
                        {
                            Logger.Instance.WarnFormat("Invalid SSTable Path of \"{0}\" detected. Ignoring SSTable for MemTable Flush", splitInfo[1]);
                        }
                        else
                        {
                            var flushInfo = currentFlushes.Find(i => !i.Completed
                                                                        && (ksItems.Item2 == i.Table)
                                                                        && i.TaskId == item.TaskId.Value
                                                                        && item.Timestamp >= i.StartTime);

                            if (flushInfo != null)
                            {
                                if (ignoreKeySpaces.Contains(ksItems.Item1))
                                {
                                    currentFlushes.Remove(flushInfo);
                                    continue;
                                }

                                flushInfo.AddUpdateOccurrence(ksItems.Item1,
                                                                ksItems.Item2,
                                                                splitInfo[1],
                                                                item.Timestamp,
                                                                splitInfo.Length > 3 ? ConvertInToMB(splitInfo[2], splitInfo[3]) : 0,
                                                                item.TaskId.Value,
                                                                groupIndicator);
                                continue;
                            }
                        }
                    }
                    #endregion
                }

                Logger.Instance.InfoFormat("Parsed Memtable flushing for {0}|{1} resulting in {2} flushes",
                                                logGroupItem.DCName,
                                                logGroupItem.IPAddress,
                                                currentFlushes.Count);

                currentFlushes.Where(i => !i.Completed && !i.Occurrences.IsEmpty() && i.Occurrences.All(o => o.CompletionTime != DateTime.MinValue))
                                .ForEach(i => i.Completed = true);
                MemTableFlushOccurrences.AddOrUpdate((logGroupItem.DCName == null ? string.Empty : logGroupItem.DCName) + "|" + logGroupItem.IPAddress,
                                                        ignore => { return new Common.Patterns.Collections.ThreadSafe.List<MemTableFlushLogInfo>(currentFlushes.Where(i => i.Completed)); },
                                                        (ignore, gcList) =>
                                                        {
                                                            gcList.AddRange(currentFlushes.Where(i => i.Completed));
                                                            return gcList;
                                                        });

                #region Log Status DT and Warnings
                if (dtLogStatusStack != null)
                {
                    var dtStatusLog = new DataTable(string.Format("NodeStatus-MemTableFlush-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));
                    InitializeStatusDataTable(dtStatusLog);
                    dtLogStatusStack.Push(dtStatusLog);

                    foreach (var memTblFlush in currentFlushes)
                    {
                        var dtStatusLogRow = dtStatusLog.NewRow();
                        dtStatusLogRow["Reconciliation Reference"] = memTblFlush.GroupIndicator;
                        dtStatusLogRow["Timestamp"] = memTblFlush.LogTimestamp;
                        dtStatusLogRow["Data Center"] = memTblFlush.DataCenter;
                        dtStatusLogRow["Node IPAddress"] = memTblFlush.IPAddress;
                        dtStatusLogRow["Pool/Cache Type"] = memTblFlush.Type;
                        dtStatusLogRow["KeySpace"] = memTblFlush.Keyspace;
                        dtStatusLogRow["Table"] = memTblFlush.Table;

                        dtStatusLogRow["Session"] = memTblFlush.Session;
                        dtStatusLogRow["Nbr MemTable Flush Events"] = memTblFlush.OccurrenceCount;
                        dtStatusLogRow["Duration (ms)"] = memTblFlush.Duration;
                        dtStatusLogRow["MemTable OPS"] = memTblFlush.NbrWriteOPS;
                        //dtStatusLogRow["Size(mb)"] = memTblFlush.
                        dtStatusLogRow["Data (mb)"] = memTblFlush.FlushedStorage;
                        dtStatusLogRow["Rate (MB/s)"] = memTblFlush.IORate;
                        dtStatusLogRow["Aborted"] = !memTblFlush.Completed;

                        dtStatusLog.Rows.Add(dtStatusLogRow);

                        foreach (var memTblOccurrence in memTblFlush.Occurrences)
                        {
                            dtStatusLogRow = dtStatusLog.NewRow();

                            dtStatusLogRow["Reconciliation Reference"] = memTblOccurrence.GroupIndicator;
                            dtStatusLogRow["Timestamp"] = memTblOccurrence.StartTime;
                            dtStatusLogRow["Data Center"] = memTblOccurrence.DataCenter;
                            dtStatusLogRow["Node IPAddress"] = memTblOccurrence.IPAddress;
                            dtStatusLogRow["Pool/Cache Type"] = "Memtable Flush";
                            dtStatusLogRow["KeySpace"] = memTblOccurrence.Keyspace;
                            dtStatusLogRow["Table"] = memTblOccurrence.Table;

                            dtStatusLogRow["Session"] = memTblOccurrence.Session;
                            dtStatusLogRow["Duration (ms)"] = memTblOccurrence.Duration;
                            dtStatusLogRow["MemTable OPS"] = memTblOccurrence.NbrWriteOPS;
                            //dtStatusLogRow["Size(mb)"] = memTblOccurrence
                            dtStatusLogRow["Data (mb)"] = memTblOccurrence.FlushedStorage;
                            dtStatusLogRow["Rate (MB/s)"] = memTblOccurrence.IORate;
                            dtStatusLogRow["Partitions Merged"] = memTblOccurrence.SSTableFilePath;

                            dtStatusLog.Rows.Add(dtStatusLogRow);
                        }

                        if (memTblFlush.LogDataRow != null
                                && string.IsNullOrEmpty(memTblFlush.LogDataRow.Field<string>("Exception"))
                                && memTblFlush.Completed
                                && memTblFlush.Duration > 0)
                        {
                            if (flushFlagThresholdInMS > 0
                                    && memTblFlush.Duration >= flushFlagThresholdInMS)
                            {
                                lock (memTblFlush.LogDataRow.Table)
                                {
                                    memTblFlush.LogDataRow.SetField<string>("Exception", "Memtable Flush Latency Warning");
                                }
                            }
                            if (flushFlagThresholdAsIORate > 0
                                    && memTblFlush.IORate < flushFlagThresholdAsIORate)
                            {
                                lock (memTblFlush.LogDataRow.Table)
                                {
                                    memTblFlush.LogDataRow.SetField<string>("Exception", "Memtable Flush IO Rate Warning");
                                }
                            }
                        }
                    }
                }
                #endregion

                #region CFStats DT

                if (dtCFStack != null)
                {
                    var dtCFStats = new DataTable(string.Format("CFStats-MemTableFlush-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));
                    initializeCFStatsDataTable(dtCFStats);
                    dtCFStack.Push(dtCFStats);

                    //Memtable Flush maximum latency
                    //Memtable Flush mean latency
                    //Memtable Flush minimum latency
                    //Memtable Flush occurrences
                    //Memtable Flush maximum IORate
                    //Memtable Flush mean IORate
                    //Memtable Flush minimum IORate
                    //Memtable Flush maximum Storage
                    //Memtable Flush mean Storage
                    //Memtable Flush minimum Storage
                    //Memtable Flush total Storage

                    var grpStats = from item in currentFlushes
                                   where item.Completed && item.Duration > 0
                                   group new { Latency = item.Duration, IORate = item.IORate, FlushedStorage = item.FlushedStorage, GrpInd = item.GroupIndicator }
                                                by new { item.DataCenter, item.IPAddress, item.Keyspace, item.Table, item.Type } into g
                                   let latencyEnum = g.Select(i => i.Latency)
                                   let latencyEnum1 = latencyEnum.Where(i => i > 0).DefaultIfEmpty()
                                   let iorateEnum = g.Select(i => i.IORate)
                                   let iorateEnum1 = iorateEnum.Where(i => i > 0).DefaultIfEmpty()
                                   let sizeEnum = g.Select(i => i.FlushedStorage)
                                   let sizeEnum1 = sizeEnum.Where(i => i > 0).DefaultIfEmpty()
                                   select new
                                   {
                                       MaxLatency = latencyEnum.Max(),
                                       MinLatency = latencyEnum1.Min(),
                                       MeanLatency = latencyEnum1.Average(),
                                       MaxIORate = iorateEnum.Max(),
                                       MinIORate = iorateEnum1.Min(),
                                       MeanIORate = iorateEnum1.Average(),
                                       MaxSize = sizeEnum.Max(),
                                       MinSize = sizeEnum1.Min(),
                                       MeanSize = sizeEnum1.Average(),
                                       TotalSize = sizeEnum.Sum(),
                                       GrpInds = string.Join(",", g.Select(i => i.GrpInd).DuplicatesRemoved(i => i)),
                                       Count = g.Count(),
                                       DCName = g.Key.DataCenter,
                                       IPAddress = g.Key.IPAddress,
                                       KeySpace = g.Key.Keyspace,
                                       Table = g.Key.Table,
                                       Type = g.Key.Type
                                   };

                    foreach (var stats in grpStats)
                    {
                        var dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush maximum latency";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MaxLatency > 0)
                        {
                            dataRow["Value"] = stats.MaxLatency;
                            dataRow["(Value)"] = stats.MaxLatency;
                        }
                        dataRow["Unit of Measure"] = "ms";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush mean latency";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MeanLatency > 0)
                        {
                            dataRow["Value"] = stats.MeanLatency;
                            dataRow["(Value)"] = stats.MeanLatency;
                        }
                        dataRow["Unit of Measure"] = "ms";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush minimum latency";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MinLatency > 0)
                        {
                            dataRow["Value"] = stats.MinLatency;
                            dataRow["(Value)"] = stats.MinLatency;
                        }
                        dataRow["Unit of Measure"] = "ms";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush occurrences";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        dataRow["Value"] = stats.Count;
                        dataRow["(Value)"] = stats.Count;
                        //dataRow["Unit of Measure"] = "";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush maximum IORate";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MaxIORate > 0)
                        {
                            dataRow["Value"] = stats.MaxIORate;
                            dataRow["(Value)"] = stats.MaxIORate;
                        }
                        dataRow["Unit of Measure"] = "MB/sec";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush mean IORate";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MeanIORate > 0)
                        {
                            dataRow["Value"] = stats.MeanIORate;
                            dataRow["(Value)"] = stats.MeanIORate;
                        }
                        dataRow["Unit of Measure"] = "MB/sec";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush minimum IORate";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MinIORate > 0)
                        {
                            dataRow["Value"] = stats.MinIORate;
                            dataRow["(Value)"] = stats.MinIORate;
                        }
                        dataRow["Unit of Measure"] = "MB/sec";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush minimum Storage";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MinSize > 0)
                        {
                            dataRow["Value"] = stats.MinSize;
                            dataRow["(Value)"] = stats.MinSize;
                        }
                        dataRow["Unit of Measure"] = "MB";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush maximum Storage";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MaxSize > 0)
                        {
                            dataRow["Value"] = stats.MaxSize;
                            dataRow["(Value)"] = stats.MaxSize;
                        }
                        dataRow["Unit of Measure"] = "MB";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush mean Storage";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        if (stats.MeanSize > 0)
                        {
                            dataRow["Value"] = stats.MeanSize;
                            dataRow["(Value)"] = stats.MeanSize;
                        }
                        dataRow["Unit of Measure"] = "MB";

                        dtCFStats.Rows.Add(dataRow);

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = string.Format("Cassandra Log ({0})", stats.Type);
                        dataRow["Data Center"] = stats.DCName;
                        dataRow["Node IPAddress"] = stats.IPAddress;
                        dataRow["KeySpace"] = stats.KeySpace;
                        dataRow["Table"] = stats.Table;
                        dataRow["Attribute"] = "Memtable Flush total Storage";
                        dataRow["Reconciliation Reference"] = stats.GrpInds;
                        dataRow["Value"] = stats.TotalSize;
                        dataRow["(Value)"] = stats.TotalSize;
                        dataRow["Unit of Measure"] = "MB";

                        dtCFStats.Rows.Add(dataRow);
                    }
                }
                #endregion

                Logger.Instance.InfoFormat("Completed Parsing Memtable flushing for {0}|{1}",
                                                logGroupItem.DCName,
                                                logGroupItem.IPAddress);
            });

            Logger.Instance.Info("Completed Parsing Memtable flushing from logs");
        }

        private class ConcurrentInfo
        {
            public ConcurrentInfo(ILogRateInfo itemInfo, DateTime? startTime = null)
            {
                this.DataCenter = itemInfo.DataCenter;
                this.IPAddress = itemInfo.IPAddress;
                this.StartFinish = new DateTimeRange(startTime.HasValue ? startTime.Value : itemInfo.StartTime, itemInfo.CompletionTime);
                this.ConcurrentList.Add(itemInfo);
            }

            public string DataCenter;
            public string IPAddress;
            public DateTimeRange StartFinish = null;
            public List<ILogRateInfo> ConcurrentList = new List<ILogRateInfo>();
        }

        public static void ConcurrentCompactionFlush(Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtNodeStatsStack,
                                                        DataTable dtLog)
        {
            Logger.Instance.Info("Begin Detecting Concurrent Compactions/Flushes Occurrences");

            ConcurrentInfo currentConcurrentItem = null;
            var concurrentCollection = from info in CompactionOccurrences.UnSafe.Values.SelectMany(i => i).Cast<ILogRateInfo>()
                                                        .Union(MemTableFlushOccurrences.UnSafe.Values.SelectMany(i => i).Cast<ILogRateInfo>())
                                       group new { Item = info } by new { info.DataCenter, info.IPAddress } into g
                                       select (from i in g orderby i.Item.StartTime ascending, i.Item.CompletionTime ascending select i.Item)
                                                .SelectWithPrevious((prevInfo, currentInfo)
                                                                            =>
                                                                        {
                                                                            if (prevInfo == null)
                                                                            {
                                                                                currentConcurrentItem = null;
                                                                                return null;
                                                                            }

                                                                            ConcurrentInfo returnValue = null;

                                                                            if (currentConcurrentItem != null)
                                                                            {
                                                                                if (currentConcurrentItem.StartFinish.Includes(currentInfo.StartTime))
                                                                                {
                                                                                    currentConcurrentItem.ConcurrentList.Add(currentInfo);
                                                                                    return null;
                                                                                }

                                                                                returnValue = currentConcurrentItem;
                                                                                currentConcurrentItem = null;
                                                                            }
                                                                            if (prevInfo.CompletionTime > currentInfo.StartTime)
                                                                            {
                                                                                currentConcurrentItem = new ConcurrentInfo(prevInfo, currentInfo.StartTime);
                                                                                currentConcurrentItem.ConcurrentList.Add(currentInfo);
                                                                            }

                                                                            return returnValue;
                                                                        })
                                                .Append(currentConcurrentItem)
                                                .Where(i => i != null);
            var concurrentCollectionCnt = concurrentCollection.Count();


            if (concurrentCollectionCnt > 0)
            {
                DataTable dtNodeStats = null;
                DataTable dtCStatusLog = null;

                Logger.Instance.InfoFormat("Processing Concurrent Compactions/Flushes for {0} Nodes", concurrentCollectionCnt);

                if (dtLogStatusStack != null)
                {
                    dtCStatusLog = new DataTable(ParserSettings.ExcelWorkSheetNodeStats + " Workbook - Concurrent Compaction");
                    InitializeStatusDataTable(dtCStatusLog);
                    dtLogStatusStack.Push(dtCStatusLog);
                }

                if (dtNodeStatsStack != null)
                {
                    dtNodeStats = new System.Data.DataTable(ParserSettings.ExcelWorkSheetNodeStats + "-" + "Concurrent Compaction");
                    dtNodeStatsStack.Push(dtNodeStats);

                    initializeTPStatsDataTable(dtNodeStats);
                }

                var concurrentTotalItems = concurrentCollection.SelectMany(i => i);
                decimal grpRef = CLogSummaryInfo.IncrementGroupInicator();
                int nbrAdded = 0;

                foreach (var item in concurrentTotalItems)
                {
                    try
                    {
                        grpRef += ReferenceIncrementValue;
                        ++nbrAdded;

                        var listItemsDuration = item.ConcurrentList.Select(i => i.Duration);
                        var listItemsIORate = item.ConcurrentList.Select(i => i.IORate);
                        var maxDuration = listItemsDuration.Max();
                        var maxIORate = listItemsIORate.Max();
                        var minDuration = listItemsDuration.Where(i => i > 0).DefaultIfEmpty().Min();
                        var minIORate = listItemsIORate.Where(i => i > 0).DefaultIfEmpty().Min();
                        var avgDuration = (int)listItemsDuration.Where(i => i > 0).DefaultIfEmpty().Average();
                        var avgIORate = listItemsIORate.Where(i => i > 0).DefaultIfEmpty().Average();
                        var stdDevDuration = (int)listItemsDuration.Where(i => i > 0).DefaultIfEmpty().StandardDeviationP();
                        var stdDevIORate = (decimal)listItemsIORate.Where(i => i > 0).DefaultIfEmpty().StandardDeviationP();

                        Common.Patterns.Collections.ThreadSafe.List<PerformanceInfo> nodePerfCollection = null;
                        var dcIpAddress = (item.DataCenter == null ? string.Empty : item.DataCenter) + "|" + item.IPAddress;
                        PerformanceOccurrences.TryGetValue(dcIpAddress, out nodePerfCollection);
                        var perfItems = nodePerfCollection?.UnSafe.Where(c => item.StartFinish.Includes(c.StartTime));
                        Common.Patterns.Collections.ThreadSafe.List<GCLogInfo> nodeGCCollection = null;
                        GCOccurrences.TryGetValue(dcIpAddress, out nodeGCCollection);
                        var gcItems = nodeGCCollection?.UnSafe.Where(c => item.StartFinish.Includes(c.StartTime));

                        if (dtCStatusLog != null)
                        {
                            var compflushType = item.ConcurrentList.First().Type;
                            var keyspaces = string.Join(", ", item.ConcurrentList.Select(i => i.Keyspace).DuplicatesRemoved(i => i));

                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.StartFinish.Min;
                            dataRow["Data Center"] = item.DataCenter;
                            dataRow["Node IPAddress"] = item.IPAddress;
                            dataRow["Pool/Cache Type"] = "Concurrent Compaction/Flush (" + compflushType + ") Start";
                            dataRow["Reconciliation Reference"] = grpRef;
                            dataRow["Active"] = item.ConcurrentList.Count;
                            dataRow["KeySpace"] = keyspaces;

                            dtCStatusLog.Rows.Add(dataRow);

                            dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.StartFinish.Max;
                            dataRow["Data Center"] = item.DataCenter;
                            dataRow["Node IPAddress"] = item.IPAddress;
                            dataRow["Pool/Cache Type"] = "Concurrent Compaction/Flush (" + compflushType + ") Finish";
                            dataRow["Reconciliation Reference"] = grpRef;
                            dataRow["Completed"] = item.ConcurrentList.Count();
                            dataRow["KeySpace"] = keyspaces;
                            dataRow["Nbr GCs"] = gcItems == null ? 0 : gcItems.Count();
                            dataRow["Nbr Compactions"] = compflushType.StartsWith("Compaction") ? item.ConcurrentList.Count : 0;
                            dataRow["Nbr MemTable Flush Events"] = compflushType.StartsWith("MemTable") ? item.ConcurrentList.Count : 0;
                            dataRow["Nbr Exceptions"] = perfItems == null ? 0 : perfItems.Count();
                            dataRow["Latency (ms)"] = avgDuration;
                            dataRow["Rate (MB/s)"] = avgIORate;
                            dataRow["Duration (ms)"] = item.StartFinish.TimeSpan().TotalMilliseconds;

                            dtCStatusLog.Rows.Add(dataRow);
                        }

                        if (dtNodeStats != null)
                        {
                            #region NodeStats

                            {
                                decimal totalIORate;
                                var dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush maximum";
                                dataRow["Reconciliation Reference"] = grpRef;
                                if (maxDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = maxDuration;
                                }
                                if (maxIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = maxIORate;
                                }
                                dataRow["Occurrences"] = item.ConcurrentList.Count();

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush minimum";
                                dataRow["Reconciliation Reference"] = grpRef;
                                if (minDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = minDuration;
                                }
                                if (minIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = minIORate;
                                }
                                dataRow["Occurrences"] = item.ConcurrentList.Count();

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush mean";
                                dataRow["Reconciliation Reference"] = grpRef;
                                if (avgDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = avgDuration;
                                }
                                if (avgIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = avgIORate;
                                }
                                dataRow["Occurrences"] = item.ConcurrentList.Count();

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush standard deviation";
                                dataRow["Reconciliation Reference"] = grpRef;
                                if (stdDevDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = stdDevDuration;
                                }
                                if (stdDevIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = stdDevIORate;
                                }
                                dataRow["Occurrences"] = item.ConcurrentList.Count();

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush Total";
                                dataRow["Reconciliation Reference"] = grpRef;
                                dataRow["Latency (ms)"] = item.ConcurrentList.Sum(i => i.Duration);
                                dataRow["IORate (mb/sec)"] = totalIORate = item.ConcurrentList.Sum(i => i.IORate);
                                dataRow["Occurrences"] = item.ConcurrentList.Count();

                                dtNodeStats.Rows.Add(dataRow);

                                if (ParserSettings.ConcurrentAccumulativeIORateThreshold > 0
                                        && totalIORate < ParserSettings.ConcurrentAccumulativeIORateThreshold)
                                {
                                    lock (dtLog)
                                    {
                                        dataRow = dtLog.NewRow();

                                        dataRow["Data Center"] = item.DataCenter;
                                        dataRow["Node IPAddress"] = item.IPAddress;
                                        dataRow["Timestamp"] = item.StartFinish.Min;
                                        dataRow["Exception"] = "Concurrent Compaction/Flush Total IO Rate Warning";
                                        dataRow["Associated Value"] = totalIORate;
                                        dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                        dataRow["Indicator"] = "WARN";
                                        dataRow["Task"] = "ConcurrentCompactionFlush";
                                        dataRow["Item"] = "Generated";
                                        dataRow["Description"] = string.Format("Concurrent Compaction/Flush Total IO Rate Warning where a total of {0} detected that had an ending timestamp of {1}",
                                                                                item.ConcurrentList.Count,
                                                                                item.StartFinish.Max);

                                        dtLog.Rows.Add(dataRow);
                                    }
                                }
                            }

                            var concurrentTypes = from typeItem in item.ConcurrentList
                                                  group typeItem by typeItem.Type into g
                                                  let durationEnum = g.Select(i => i.Duration)
                                                  let iorateEnum = g.Select(i => i.IORate)
                                                  let durationEnum1 = durationEnum.Where(i => i > 0).DefaultIfEmpty()
                                                  let iorateEnum1 = iorateEnum.Where(i => i > 0).DefaultIfEmpty()
                                                  let refIds = g.Select(i => i.GroupIndicator).DuplicatesRemoved(id => id)
                                                  select new
                                                  {
                                                      DCName = item.DataCenter,
                                                      IPAddress = item.IPAddress,
                                                      Type = g.Key,
                                                      RefIds = refIds,
                                                      RefId = grpRef.ToString() + "|" + string.Join(",", refIds),
                                                      TimeStamps = g.Select(i => i.StartTime),
                                                      MaxDuration = durationEnum.Max(),
                                                      MinDuration = durationEnum1.Min(),
                                                      AvgDuration = (int)durationEnum1.Average(),
                                                      StdDuration = (int)durationEnum1.StandardDeviationP(),
                                                      TotalDuration = durationEnum.Sum(),
                                                      MaxIORate = iorateEnum.Max(),
                                                      MinIORate = iorateEnum1.Min(),
                                                      AvgIORate = iorateEnum1.Average(),
                                                      StdIORate = (decimal)iorateEnum1.StandardDeviationP(),
                                                      TotalIORate = iorateEnum.Sum(),
                                                      Occurrences = g.Count()
                                                  };

                            foreach (var typeItem in concurrentTypes)
                            {
                                var dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = typeItem.DCName;
                                dataRow["Node IPAddress"] = typeItem.IPAddress;
                                dataRow["Attribute"] = string.Format("Concurrent {0} maximum", typeItem.Type);
                                dataRow["Reconciliation Reference"] = typeItem.RefId;
                                if (typeItem.MaxDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = typeItem.MaxDuration;
                                }
                                if (typeItem.MaxIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = typeItem.MaxIORate;
                                }
                                dataRow["Occurrences"] = typeItem.Occurrences;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = typeItem.DCName;
                                dataRow["Node IPAddress"] = typeItem.IPAddress;
                                dataRow["Attribute"] = string.Format("Concurrent {0} minimum", typeItem.Type);
                                dataRow["Reconciliation Reference"] = typeItem.RefId;
                                if (typeItem.MinDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = typeItem.MinDuration;
                                }
                                if (typeItem.MinIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = typeItem.MinIORate;
                                }
                                dataRow["Occurrences"] = typeItem.Occurrences;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = typeItem.DCName;
                                dataRow["Node IPAddress"] = typeItem.IPAddress;
                                dataRow["Attribute"] = string.Format("Concurrent {0} mean", typeItem.Type);
                                dataRow["Reconciliation Reference"] = typeItem.RefId;
                                if (typeItem.AvgDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = typeItem.AvgDuration;
                                }
                                if (typeItem.AvgIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = typeItem.AvgIORate;
                                }
                                dataRow["Occurrences"] = typeItem.Occurrences;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = typeItem.DCName;
                                dataRow["Node IPAddress"] = typeItem.IPAddress;
                                dataRow["Attribute"] = string.Format("Concurrent {0} standard deviation", typeItem.Type);
                                dataRow["Reconciliation Reference"] = typeItem.RefId;
                                if (typeItem.StdDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = typeItem.StdDuration;
                                }
                                if (typeItem.StdIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = typeItem.StdIORate;
                                }
                                dataRow["Occurrences"] = typeItem.Occurrences;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = typeItem.DCName;
                                dataRow["Node IPAddress"] = typeItem.IPAddress;
                                dataRow["Attribute"] = string.Format("Concurrent {0} Total", typeItem.Type);
                                dataRow["Reconciliation Reference"] = typeItem.RefId;
                                if (typeItem.TotalDuration > 0)
                                {
                                    dataRow["Latency (ms)"] = typeItem.TotalDuration;
                                }
                                if (typeItem.TotalIORate > 0)
                                {
                                    dataRow["IORate (mb/sec)"] = typeItem.TotalIORate;
                                }
                                dataRow["Occurrences"] = typeItem.Occurrences;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = typeItem.DCName;
                                dataRow["Node IPAddress"] = typeItem.IPAddress;
                                dataRow["Attribute"] = string.Format("Concurrent {0} occurrences", typeItem.Type);
                                dataRow["Reconciliation Reference"] = grpRef.ToString() + "|["
                                                                        + string.Join(", ", typeItem.RefIds
                                                                                                .SelectWithIndex((refId, idx)
                                                                                                    => string.Format("[{0}, {1:yyyy-MM-dd HH:mm:ss.ff}]",
                                                                                                                        refId,
                                                                                                                        typeItem.TimeStamps.ElementAtOrDefault(idx)))) + "]";
                                dataRow["Occurrences"] = typeItem.Occurrences;

                                dtNodeStats.Rows.Add(dataRow);
                            }

                            if (perfItems != null && perfItems.Count() > 0)
                            {
                                //Concurrent Compaction/Flush Performance Warnings maximum
                                // Concurrent Compaction/Flush Performance Warnings minimum
                                // Concurrent Compaction/Flush Performance Warnings mean
                                //Concurrent Compaction/Flush Performance Warnings standard deviation

                                var perfCount = perfItems.Count();
                                var perfLatencies = perfItems.Select(i => i.Latency);
                                var maxPerfLatency = perfLatencies.Max();
                                var minPerfLatency = perfLatencies.Where(i => i > 0).DefaultIfEmpty().Min();
                                var avgPerfLatency = (int)perfLatencies.Where(i => i > 0).DefaultIfEmpty().Average();
                                var stddevPerfLatency = (int)perfLatencies.Where(i => i > 0).DefaultIfEmpty().StandardDeviationP();
                                var refIds = grpRef.ToString() + "|" + perfItems.GroupIndicatorLogTimestamp();
                                var dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush Performance Warnings maximum";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (maxPerfLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = maxPerfLatency;
                                }

                                dataRow["Occurrences"] = perfCount;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush Performance Warnings minimum";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (minPerfLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = minPerfLatency;
                                }

                                dataRow["Occurrences"] = perfCount;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush Performance Warnings mean";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (minPerfLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = avgPerfLatency;
                                }

                                dataRow["Occurrences"] = perfCount;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush Performance Warnings standard deviation";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (stddevPerfLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = stddevPerfLatency;
                                }

                                dataRow["Occurrences"] = perfCount;

                                dtNodeStats.Rows.Add(dataRow);

                            }

                            if (gcItems != null && gcItems.Count() > 0)
                            {
                                //Concurrent Compaction/Flush GC maximum
                                // Concurrent Compaction/Flush GC minimum
                                // Concurrent Compaction/Flush GC mean
                                //Concurrent Compaction/Flush GC standard deviation

                                var gcCount = gcItems.Count();
                                var gcLatencies = gcItems.Select(i => i.Duration);
                                var maxGCLatency = gcLatencies.Max();
                                var minGCLatency = gcLatencies.Where(i => i > 0).DefaultIfEmpty().Min();
                                var avgGCLatency = (int)gcLatencies.Where(i => i > 0).DefaultIfEmpty().Average();
                                var stddevGCLatency = (int)gcLatencies.Where(i => i > 0).DefaultIfEmpty().StandardDeviationP();
                                var refIds = grpRef.ToString() + "|["
                                                + string.Join(", ", gcItems
                                                                        .SelectWithIndex((refId, idx)
                                                                            => string.Format("[{0}, {1:yyyy-MM-dd HH:mm:ss.ff}]",
                                                                                                refId.GroupIndicator,
                                                                                                gcItems.ElementAtOrDefault(idx).LogTimestamp))) + "]";
                                var dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush GC maximum";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (maxGCLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = maxGCLatency;
                                }

                                dataRow["Occurrences"] = gcCount;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush GC minimum";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (minGCLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = minGCLatency;
                                }

                                dataRow["Occurrences"] = gcCount;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush GC mean";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (minGCLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = avgGCLatency;
                                }

                                dataRow["Occurrences"] = gcCount;

                                dtNodeStats.Rows.Add(dataRow);

                                dataRow = dtNodeStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = item.DataCenter;
                                dataRow["Node IPAddress"] = item.IPAddress;
                                dataRow["Attribute"] = "Concurrent Compaction/Flush GC standard deviation";
                                dataRow["Reconciliation Reference"] = refIds;

                                if (stddevGCLatency > 0)
                                {
                                    dataRow["Latency (ms)"] = stddevGCLatency;
                                }

                                dataRow["Occurrences"] = gcCount;

                                dtNodeStats.Rows.Add(dataRow);

                            }

                            #endregion
                        }
                    }
                    catch (System.Exception ex)
                    {
                        Logger.Instance.ErrorFormat("Concurrent Compactions/Flushes Exception occurred for {0}.{1} on Range {2} with {3} items. Current Items Added {4}, Total Nbr Items {5}",
                                                        item.DataCenter,
                                                        item.IPAddress,
                                                        item.StartFinish,
                                                        item.ConcurrentList.Count,
                                                        nbrAdded,
                                                        concurrentTotalItems.Count());
                        Logger.Instance.Error("Concurrent Compactions/Flushes Exception", ex);

                        if (ex is System.OutOfMemoryException)
                        {
                            ComponentDisabled.Add(new Tuple<DateTime, string, string, string, int>(
                                                        item.StartFinish.Min,
                                                        string.Format("{0}|{1}", item.DataCenter, item.IPAddress),
                                                        null,
                                                        "Concurrent Compactions/Flushes analysis disabled due to OOM",
                                                        concurrentTotalItems.Count()));
                            throw;
                        }
                    }
                }

                Logger.Instance.InfoFormat("Added Concurrent Compactions/Flushes Occurrences ({0}) to NodeStats",
                                            nbrAdded);
            }

            Logger.Instance.Info("Completed Detecting Concurrent Compactions/Flushes Occurrences");
        }

        public static Task<DataTable> ReviewDropsBlocksThresholdsOtherComponentIssues(Task<DataTable> logTask,
                                                        Task<DataTable> statLogTask,
                                                        Task[] waitOnAdditionalTasks,
                                                        int warningThreshold,
                                                        int warningPeriodInMins)
        {
            return Task<DataTable>.Factory
                           .ContinueWhenAll((new List<Task> { logTask, statLogTask }).Append(waitOnAdditionalTasks).ToArray(),
                                               tasks =>
                                               {
                                                   Program.ConsoleParsingLog.Increment("Processing Review of Drops/Blocks");

                                                   var dtLog = ((Task<DataTable>)tasks[0]).Result;
                                                   var allCompleted = tasks.All(t => t.Status == TaskStatus.RanToCompletion);

                                                   if (allCompleted)
                                                   {
                                                       if (warningThreshold > 0
                                                            && ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled())
                                                       {
                                                           ReviewDropsBlocksThresholdsOtherComponentIssues(dtLog,
                                                                                ((Task<DataTable>)tasks[1]).Result,
                                                                                warningThreshold,
                                                                                warningPeriodInMins);
                                                       }

                                                       ReviewLogThresholds(dtLog);
                                                   }
                                                   else
                                                   {                                                                                                            
                                                       if(tasks.Any(t => t.Exception != null
                                                                            && t.Exception.InnerExceptions.Any(e => e is System.OutOfMemoryException)))
                                                       {
                                                           ReleaseGlobalLogCollections(false);
                                                           System.GC.Collect();
                                                       } 

                                                       Logger.Instance.ErrorFormat("Component Task Failure(s) detected with status \"{0}\"",
                                                                                    string.Join(", ", tasks.Select(t => string.Format("{{{0}{1}}}",
                                                                                                                                        t.Status,
                                                                                                                                        (t.Exception == null || t.Exception.InnerException == null
                                                                                                                                            ? string.Empty
                                                                                                                                            : string.Join(", ", t.Exception.InnerExceptions.Select(e => string.Format(", {0}, {1}",
                                                                                                                                                                                                                    e.GetType().Name,
                                                                                                                                                                                                                    e.Message))))
                                                                                                                                            ))));                                                      
                                                       ComponentDisabled.Add(new Tuple<DateTime, string, string, string, int>(
                                                                                   DateTime.Now,
                                                                                   "<Component>",
                                                                                   null,
                                                                                   "Component Failure",
                                                                                   1));                                                       
                                                   }

                                                   ReviewComponentDisabledItems(dtLog);

                                                   dtLog.AcceptChanges();

                                                   Program.ConsoleParsingLog.TaskEnd("Processing Review of Drops/Blocks");
                                                   return dtLog;
                                               },
                                               TaskContinuationOptions.AttachedToParent
                                                    | TaskContinuationOptions.LongRunning);
        }

        public static void ReviewDropsBlocksThresholdsOtherComponentIssues(DataTable dtLog,
                                                DataTable dtStatLog,
                                                int warningThreshold,
                                                int warningPeriodInMins)
        {

            if (dtLog == null || dtStatLog == null || dtStatLog.Rows.Count == 0)
            {
                return;
            }

            Logger.Instance.Info("Begin Review of Drops/Blocks from logs");

            /* var droppedFromLog = from logRow in dtLog.AsEnumerable()
                                    let tag = logRow.Field<string>("Exception")
                                    where tag != null && tag.StartsWith("Dropped ") && !logRow.IsNull("Associated Value")
                                  select new
                                    {
                                        DataCenter = logRow.Field<string>("Data Center"),
                                        Node = logRow.Field<string>("Node IPAddress"),
                                        Timestamp = logRow.Field<DateTime>("Timestamp"),
                                        Items = (long)logRow.Field<int>("Associated Value")
                                    };
             */
            var blocksFromStats = from statRow in dtStatLog.AsEnumerable()
                                  let nbrBlocks = statRow.Field<long?>("Blocked")
                                  let nbrAllBlocks = statRow.Field<long?>("All Time Blocked")
                                  let nbrCompleted = statRow.Field<long?>("Completed")
                                  let attribute = statRow.Field<string>("Pool/Cache Type")
                                  where (nbrBlocks.HasValue && nbrBlocks.Value > 0)
                                            || (nbrAllBlocks.HasValue && nbrAllBlocks.Value > 0)
                                            || (nbrCompleted.HasValue && nbrCompleted.Value > 0
                                                    && attribute == "Dropped Mutation")
                                  select new
                                  {
                                      DataCenter = statRow.Field<string>("Data Center"),
                                      Node = statRow.Field<string>("Node IPAddress"),
                                      Timestamp = statRow.Field<DateTime>("Timestamp"),
                                      Items = (nbrAllBlocks.HasValue ? nbrAllBlocks.Value : 0)
                                                + (nbrBlocks.HasValue ? nbrBlocks.Value : 0)
                                                + (nbrCompleted.HasValue ? nbrCompleted.Value : 0)
                                  };

            var allItems = blocksFromStats; // droppedFromLog.Concat(blocksFromStats);

            var warningItems = from item in allItems
                               group item by new { item.DataCenter, item.Node } into grp
                               select new
                               {
                                   DataCenter = grp.Key.DataCenter,
                                   Node = grp.Key.Node,
                                   Warnings = warningPeriodInMins > 0
                                                ? (from tsValue in grp
                                                   let timestamp = tsValue.Timestamp
                                                   let groupTS1 = timestamp.AddMinutes(-(timestamp.Minute % warningPeriodInMins))
                                                   let groupTS = groupTS1.AddMilliseconds(-groupTS1.Millisecond - 1000 * groupTS1.Second)
                                                   group tsValue by groupTS into grpTS
                                                   select new
                                                   {
                                                       TimeStampGrp = grpTS.Key,
                                                       Total = grpTS.Sum(i => i.Items)
                                                   }).Where(i => i.Total >= warningThreshold)
                                                : (from tsValue in grp
                                                   where tsValue.Items >= warningThreshold
                                                   select new
                                                   {
                                                       TimeStampGrp = tsValue.Timestamp,
                                                       Total = tsValue.Items
                                                   })
                               };

            foreach (var warningItem in warningItems)
            {
                foreach (var warningValue in warningItem.Warnings)
                {
                    DataRow dataRow = null;
                    long runningTotal = 0;

                    for (long i = 0; i < warningValue.Total / warningThreshold; i++)
                    {
                        dataRow = dtLog.NewRow();

                        dataRow["Data Center"] = warningItem.DataCenter;
                        dataRow["Node IPAddress"] = warningItem.Node;
                        dataRow["Timestamp"] = warningValue.TimeStampGrp;
                        dataRow["Exception"] = "Dropped/Blocked Warning";
                        dataRow["Associated Value"] = warningThreshold;
                        runningTotal += warningThreshold;
                        dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                        dataRow["Indicator"] = "WARN";
                        dataRow["Task"] = "DroppedBlockedReview";
                        dataRow["Item"] = "Generated";
                        dataRow["Description"] = string.Format("Dropped/Blocked Warning where a total of {0} detected that exceed threshold {1} during period of {2} in mintues",
                                                                warningValue.Total,
                                                                warningThreshold,
                                                                warningPeriodInMins);
                        dtLog.Rows.Add(dataRow);
                    }

                    if (dataRow != null && warningValue.Total > runningTotal)
                    {
                        dataRow.BeginEdit();
                        dataRow["Associated Value"] = warningThreshold + (warningValue.Total - runningTotal);
                        dataRow.EndEdit();
                    }
                }
            }

            Logger.Instance.Info("Completed Review of Drops/Blocks from logs");
        }

        public static void ReviewLogThresholds(DataTable dtLog)
        {
            if (ParserSettings.SolrHardCommitLatencyThresholdInMS > 0)
            {
                Logger.Instance.Info("Begin Review of Solr Hard Commit Thresholds");

                foreach (var item in SolrHardCommits.UnSafe)
                {
                    foreach (var solrItem in item.Value)
                    {
                        if (solrItem.Duration >= ParserSettings.SolrHardCommitLatencyThresholdInMS)
                        {
                            var dataRow = dtLog.NewRow();

                            dataRow["Data Center"] = solrItem.DataCenter;
                            dataRow["Node IPAddress"] = solrItem.IPAddress;
                            dataRow["Timestamp"] = solrItem.LogTimestamp;
                            dataRow["Exception"] = "Solr Hard Commit duration Warning";
                            dataRow["Associated Value"] = solrItem.Duration;
                            dataRow["Associated Item"] = solrItem.Table == null
                                            ? solrItem.Keyspace
                                            : solrItem.Keyspace + '.' + solrItem.Table;
                            dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                            dataRow["Indicator"] = "WARN";
                            dataRow["Task"] = "ReviewLogThresholds";
                            dataRow["Item"] = "Generated";
                            dataRow["Description"] = string.Format("Solr Hard Commit duration Warning where a duration of {0} ms detected for {1}",
                                                solrItem.Duration,
                                                dataRow["Associated Item"]);

                            dtLog.Rows.Add(dataRow);
                        }
                    }
                }

                Logger.Instance.Info("Completed Review of Solr Hard Commit Thresholds");
            }

            if (ParserSettings.SolrReIndexLatencyThresholdInMS > 0)
            {
                Logger.Instance.Info("Begin Review of Solr Reindexing Thresholds");

                foreach (var item in SolrReindexingOccurrences.UnSafe)
                {
                    foreach (var solrItem in item.Value)
                    {
                        if (solrItem.Duration >= ParserSettings.SolrReIndexLatencyThresholdInMS)
                        {
                            var dataRow = dtLog.NewRow();

                            dataRow["Data Center"] = solrItem.DataCenter;
                            dataRow["Node IPAddress"] = solrItem.IPAddress;
                            dataRow["Timestamp"] = solrItem.LogTimestamp;
                            dataRow["Exception"] = "Solr reindex duration Warning";
                            dataRow["Associated Value"] = solrItem.Duration;
                            dataRow["Associated Item"] = solrItem.Table == null
                                            ? solrItem.Keyspace
                                            : solrItem.Keyspace + '.' + solrItem.Table;
                            dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                            dataRow["Indicator"] = "WARN";
                            dataRow["Task"] = "ReviewLogThresholds";
                            dataRow["Item"] = "Generated";
                            dataRow["Description"] = string.Format("Solr reindex duration Warning where a duration of {0} ms detected for {1}",
                                                solrItem.Duration,
                                                dataRow["Associated Item"]);

                            dtLog.Rows.Add(dataRow);
                        }
                    }
                }

                Logger.Instance.Info("Begin Review of Solr ReIndexing Thresholds");
            }

            if (ParserSettings.AntiCompactionLatencyThresholdMS > 0
                    && HasAntiCompactions)
            {
                Logger.Instance.Info("Begin Review of AntiCompacation Thresholds");

                foreach (var item in CompactionOccurrences.UnSafe)
                {
                    foreach (var compactionItem in item.Value)
                    {
                        if (compactionItem is AntiCompactionLogInfo
                                && compactionItem.Duration >= ParserSettings.AntiCompactionLatencyThresholdMS)
                        {
                            var dataRow = dtLog.NewRow();

                            dataRow["Data Center"] = compactionItem.DataCenter;
                            dataRow["Node IPAddress"] = compactionItem.IPAddress;
                            dataRow["Timestamp"] = compactionItem.LogTimestamp;
                            dataRow["Exception"] = "AntiCompaction Latency Warning";
                            dataRow["Associated Value"] = compactionItem.Duration;
                            dataRow["Associated Item"] = compactionItem.Table == null
                                            ? compactionItem.Keyspace
                                            : compactionItem.Keyspace + '.' + compactionItem.Table;
                            dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                            dataRow["Indicator"] = "WARN";
                            dataRow["Task"] = "ReviewLogThresholds";
                            dataRow["Item"] = "Generated";
                            dataRow["Description"] = string.Format("AntiCompaction duration Warning where a duration of {0} ms detected for {1}",
                                                compactionItem.Duration,
                                                dataRow["Associated Item"]);

                            dtLog.Rows.Add(dataRow);
                        }
                    }
                }

                Logger.Instance.Info("Completed Review of AntiCompacation Thresholds");
            }
        }

        public static void ReviewComponentDisabledItems(DataTable dtLog)
        {
            if (ComponentDisabled.UnSafe.Count > 0)
            {
                /// Log Timestamp, DC|Node, Keyspace.Table, Component (e.g., GC, flushing, etc.), nbr of detected items
                foreach (var componentItem in ComponentDisabled.UnSafe)
                {
                    var dataRow = dtLog.NewRow();
                    var ksTblSplit = componentItem.Item2.Split('|');

                    dataRow["Data Center"] = ksTblSplit[0];
                    dataRow["Node IPAddress"] = ksTblSplit[1];
                    dataRow["Timestamp"] = componentItem.Item1;
                    dataRow["Exception"] = string.Format("Component Warning {0}", componentItem.Item4);
                    dataRow["Associated Value"] = componentItem.Item5;
                    dataRow["Associated Item"] = componentItem.Item3;
                    dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                    dataRow["Indicator"] = "WARN";
                    dataRow["Task"] = "DisableComponent";
                    dataRow["Item"] = "Generated";
                    dataRow["Description"] = string.Format("Component Warning for {0} at {1}",
                                        componentItem.Item4,
                                        componentItem.Item5);

                    dtLog.Rows.Add(dataRow);
                }
            }
        }
    }
    public static class GroupIndicatorHelpers
	{
		public static string GroupIndicatorString<T>(this IEnumerable<T> grpindCollection)
			where T : ProcessFileTasks.ILogInfo
		{
			var grpIndValues = grpindCollection?.Select(i => i.GroupIndicator.ToString()).Where(i => !string.IsNullOrEmpty(i)).DuplicatesRemoved(i => i);

			return grpIndValues == null ? null : string.Join(",", grpIndValues);
		}

		public static string GroupIndicatorLogTimestamp<T>(this IEnumerable<T> grpindCollection)
			where T : ProcessFileTasks.ILogInfo
		{
			var grpIndValues = grpindCollection?
								.SelectWithIndex((refId, idx)
													=> string.Format("[{0}, {1:yyyy-MM-dd HH:mm:ss.ff}]",
																		refId.GroupIndicator,
																		grpindCollection.ElementAtOrDefault(idx).LogTimestamp));

			return "[" + grpIndValues == null ? string.Empty : string.Join(", ", grpIndValues) + "]";
		}
	}
}
