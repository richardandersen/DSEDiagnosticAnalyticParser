﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using System.Text.RegularExpressions;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
        static public Task<int> ProcessLogFileTasks(IFilePath logFilePath,
                                                        string excelWorkSheetLogCassandra,
                                                        string dcName,
                                                        string ipAddress,
                                                        DateTime includeLogEntriesAfterThisTimeFrame,
                                                        DateTimeRange maxminMaxLogDate,
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
            DateTime maxLogTimestamp = DateTime.MinValue;
            var dtLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
			Task statusTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();
            Task<int> archTask = Common.Patterns.Tasks.CompletionExtensions.CompletedTask(0);

            var logTask = Task.Factory.StartNew(() =>
                                {
                                    Logger.Instance.InfoFormat("Processing File \"{0}\"", logFilePath.Path);
                                    Program.ConsoleLogReadFiles.Increment(string.Format("{0} - {1}", ipAddress, logFilePath.FileName));

                                    dtLogsStack.Push(dtLog);
                                    var linesRead = ReadCassandraLogParseIntoDataTable(logFilePath,
                                                                                        ipAddress,
                                                                                        dcName,
                                                                                        includeLogEntriesAfterThisTimeFrame,
                                                                                        dtLog,
                                                                                        out maxLogTimestamp,
                                                                                        gcPausedFlagThresholdInMS,
                                                                                        compactionFllagThresholdInMS,
                                                                                        compactionFlagThresholdAsIORate,
                                                                                        slowLogQueryThresholdInMS);

                                    lock (maxminMaxLogDate)
                                    {
                                        maxminMaxLogDate.SetMinMax(maxLogTimestamp);
                                    }

                                    Program.ConsoleLogReadFiles.TaskEnd(string.Format("{0} - {1}", ipAddress, logFilePath.FileName));

                                    return linesRead;
                                },
                                TaskCreationOptions.LongRunning);

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
                                | TaskContinuationOptions.LongRunning
                                | TaskContinuationOptions.OnlyOnRanToCompletion);
            }

            if (archiveFilePaths != null
                        && ParserSettings.LogParsingExcelOptions.ParseArchivedLogs.IsEnabled())
            {
                foreach (IFilePath archiveElement in archiveFilePaths)
                {
                    if (archiveElement.PathResolved != logFilePath.PathResolved)
                    {
                        archTask = ProcessLogFileTasks(archiveElement,
                                                            excelWorkSheetLogCassandra,
                                                            dcName,
                                                            ipAddress,
                                                            includeLogEntriesAfterThisTimeFrame,
                                                            maxminMaxLogDate,
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
                    .ContinueWhenAll(new Task[] { logTask, statusTask, archTask }, tasks => logTask.Result + archTask.Result);
        }

        public static Task<Tuple<DataTable, DataTable, DateTimeRange>> ParseCassandraLogIntoSummaryDataTable(Task<DataTable> logTask,
                                                                                                                string excelWorkSheetLogCassandra,
                                                                                                                Tuple<DateTime, TimeSpan>[] logSummaryPeriods,
                                                                                                                Tuple<TimeSpan, TimeSpan>[] logSummaryPeriodRanges,
                                                                                                                bool summarizeOnlyOverlappingDateRangesForNodes,
                                                                                                                IDictionary<string, List<Common.DateTimeRange>> nodeLogDateRanges,
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

                                            nodeLogRanges.Value.ForEach(range =>
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

									Tuple <DateTime,TimeSpan>[] summaryPeriods = logSummaryPeriods;

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

                                    return new Tuple<DataTable,DataTable, DateTimeRange>(dtSummaryLog, dtExceptionSummaryLog, maxminLogDate);
                                },
                                TaskContinuationOptions.AttachedToParent
                                    | TaskContinuationOptions.LongRunning
                                    | TaskContinuationOptions.OnlyOnRanToCompletion);
            }

            return summaryTask;
        }

        static public DateTimeRange LogCassandraMaxMinTimestamp = new Common.DateTimeRange();
        static public Common.Patterns.Collections.ThreadSafe.Dictionary<string, List<Common.DateTimeRange>> LogCassandraNodeMaxMinTimestamps = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, List<Common.DateTimeRange>>();

        static Regex RegExCLogCL = new Regex(@".*Cannot achieve consistency level\s+(\w+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCLogTO = new Regex(@".*(?:No response after timeout:|Operation timed out - received only).*\s+(\d+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExExpErrClassName = new Regex(@"^[^\[\(\']\S+(exception|error)",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        //INFO[SharedPool - Worker - 1] 2016 - 09 - 24 16:33:58,099  Message.java:532 - Unexpected exception during request; channel = [id: 0xa6a28fb0, / 10.14.50.24:44796 => / 10.14.50.24:9042]
        //io.netty.handler.ssl.NotSslRecordException: not an SSL / TLS record: 0300000001000000160001000b43514c5f56455253494f4e0005332e302e30

        static Regex RegExExceptionDesc = new Regex(@"(.+?)(?:(\/\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3}\:?\d{1,5})|(?:\:)|(?:\;)|(?:\$)|(?:\#)|(?:\[\G\])|(?:\(\G\))|(?:0x\w+)|(?:\w+\-\w+\-\w+\-\w+\-\w+)|(\'.+\')|(?:\s+\-?\d+\s+)|(?:[0-9a-zA-z]{12,}))",
                                                        RegexOptions.IgnoreCase | RegexOptions.Compiled);

		//returns only numeric values in string via Matches
		static Regex RegExNumerics = new Regex(@"([0-9-.,]+)",
												RegexOptions.IgnoreCase | RegexOptions.Compiled);

		static Regex RegExSolrSecondaryIndex = new Regex(@"SolrSecondaryIndex\s+([^\s]+\.[^\s]+)\s+(.+)",
															RegexOptions.IgnoreCase | RegexOptions.Compiled);

        public enum LogFlagStatus
        {
            None = 0,
            Exception = 1, //Includes Errors, Warns, etc. (Summary only)
            Stats = 2, //Stats and Summary
            ReadRepair = 3,
			StatsOnly = 4 //Only Stats (no summary)
        }

        static void CreateCassandraLogDataTable(System.Data.DataTable dtCLog, bool includeGroupIndiator = false)
        {
            if (dtCLog.Columns.Count == 0)
            {
                if (includeGroupIndiator)
                {
                    dtCLog.Columns.Add("Reconciliation Reference", typeof(long)).AllowDBNull = true;
                }
                dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCLog.Columns.Add("Node IPAddress", typeof(string));
                dtCLog.Columns.Add("Timestamp", typeof(DateTime));
                dtCLog.Columns.Add("Indicator", typeof(string));
                dtCLog.Columns.Add("Task", typeof(string));
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
                                                        DateTime onlyEntriesAfterThisTimeFrame,
                                                        System.Data.DataTable dtCLog,
                                                        out DateTime maxTimestamp,
                                                        int gcPausedFlagThresholdInMS,
                                                        int compactionFllagThresholdInMS,
                                                        decimal compactionFlagThresholdAsIORate,
                                                        int slowLogQueryThresholdInMS)
        {
			maxTimestamp = DateTime.MinValue;

			if (ParserSettings.IgnoreLogFileExtensions.Contains(clogFilePath.FileExtension))
			{
				return 0;
			}

            CreateCassandraLogDataTable(dtCLog);

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

            //int tableItemValuePos = -1;

            using (var readStream = clogFilePath.StreamReader())
            {
                readNextLine = readStream.ReadLine();

                while(readNextLine != null)
                {
                    readLine = readNextLine;
                    readNextLine = readStream.ReadLine();

                    if(skipNextRead)
                    {
                        skipNextRead = false;
                        continue;
                    }

                    line = readLine.Trim();

                    if (string.IsNullOrEmpty(line)
                            || line.Length < 3)
                    {
                        continue;
                    }

                    if (line.Substring(0, 4).ToLower() == "... "
                            || (!assertError && line.Substring(0, 3).ToLower() == "at "))
                    {
                        continue;
                    }

                    Program.ConsoleLogCount.Increment();

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

							if(lastRow["Flagged"] == DBNull.Value || (int) lastRow["Flagged"] == 0)
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
                            line.Dump(Logger.DumpType.Warning, "assertionerror found but no associated previous line");
                        }
                        else
                        {
                            var exception = parsedValues[0][parsedValues[0].Length - 1] == ':'
                                                    ? parsedValues[0].Substring(0, parsedValues[0].Length - 1)
                                                    : parsedValues[0];

                            lastRow["Exception Description"] = line;
                            lastRow["Flagged"] = (int) LogFlagStatus.Exception;
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
                            line.Dump(Logger.DumpType.Warning, "exception found but no associated previous line");
                        }
                        else
                        {
                            ParseExceptions(ipAddress, parsedValues[0], lastRow, string.Join(" ", parsedValues.Skip(1)), null);
                            lastRow.AcceptChanges();
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
                            lastRow.AcceptChanges();
                        }
                        exceptionOccurred = true;
                        continue;

                        #endregion
                    }

                    assertError = false;

                    #endregion

                    if (parsedValues.Count < 6)
                    {
                        if (lastRow != null && !exceptionOccurred)
                        {
                            line.Dump(Logger.DumpType.Warning, "Invalid Log Line File: {0}", clogFilePath.PathResolved);
                            Program.ConsoleWarnings.Increment("Invalid Log Line:", line);
                        }
                        continue;
                    }

                    #region Timestamp/Number of lines Parsing
                    if (DateTime.TryParse(parsedValues[2] + ' ' + parsedValues[3].Replace(',', '.'), out lineDateTime))
                    {
                        if (lineDateTime < onlyEntriesAfterThisTimeFrame)
                        {
                            Program.ConsoleLogCount.Decrement();
                            continue;
                        }
                    }
                    else
                    {
                        if (!exceptionOccurred)
                        {
                            line.Dump(Logger.DumpType.Warning, "Invalid Log Date/Time File: {0}", clogFilePath.PathResolved);
                            Program.ConsoleWarnings.Increment("Invalid Log Date/Time:", line);
                        }
                        continue;
                    }


                    List<Common.DateTimeRange> nodeRangers;
                    if (LogCassandraNodeMaxMinTimestamps.TryGetValue(ipAddress, out nodeRangers))
                    {
                        lock (nodeRangers)
                        {
                            if (nodeRangers.Any(r => r.IsBetween(lineDateTime)))
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

                    dataRow[0] = dcName;
                    dataRow[1] = ipAddress;
                    dataRow["Timestamp"] = lineDateTime;

                    minmaxDate.SetMinMax(lineDateTime);

                    dataRow["Indicator"] = parsedValues[0];

                    if (parsedValues[1][0] == '[')
                    {
                        string strItem = parsedValues[1];
                        int nPos = strItem.IndexOf(':');

                        if (nPos > 2)
                        {
                            strItem = strItem.Substring(1, nPos - 1);
                        }
                        else
                        {
                            strItem = strItem.Substring(1, strItem.Length - 2);
                        }

                        dataRow["Task"] = strItem;
                    }
                    else
                    {
                        dataRow["Task"] = parsedValues[1];
                    }

                    if (parsedValues[4][parsedValues[4].Length - 1] == ')')
                    {
                        var startPos = parsedValues[4].IndexOf('(');

                        if (startPos >= 0)
                        {
                            parsedValues[4] = parsedValues[4].Substring(0, startPos);
                        }
                    }
                    else if (parsedValues[4].Contains(":"))
                    {
                        var startPos = parsedValues[4].LastIndexOf(':');

                        if (startPos >= 0)
                        {
                            parsedValues[4] = parsedValues[4].Substring(0, startPos);
                        }
                    }

                    dataRow["Item"] = parsedValues[4];

                    if (parsedValues[4] != tableItem)
                    {
                        tableItemPos = -1;
                    }

                    #endregion

                    #region Describe Info

                    int itemPos = -1;
                    int itemValuePos = -1;

                    var logDesc = new StringBuilder();
                    var startRange = parsedValues[5] == "-" ? 6 : 5;
                    bool handled = false;

                    if (parsedValues[startRange][0] == '(')
                    {
                        ++startRange;
                    }

					if(((string)dataRow["Task"]).StartsWith("SolrSecondaryIndex "))
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

                        if (parsedValues[4] == "CompactionController.java")
                        {
                            #region CompactionController.java
                            //Compacting large row billing/account_payables:20160726:FMCC (348583137 bytes)

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
                                dataRow["Exception"] = "Compacting large row";
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[4] == "SSTableWriter.java")
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
                        else if (parsedValues[4] == "GCInspector.java")
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
                            else if (parsedValues[0] == "WARN" && parsedValues[nCell] == "Heap" && parsedValues[nCell + 3] == "full")
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
                        else if (parsedValues[4] == "FailureDetector.java")
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
                        else if (parsedValues[4] == "BatchStatement.java")
                        {
							#region BatchStatement.java
							//BatchStatement.java (line 226) Batch of prepared statements for [clearcore.documents_case] is of size 71809, exceeding specified threshold of 65536 by 6273.
							//WARN  [SharedPool-Worker-3] 2016-12-03 00:11:32,802  BatchStatement.java:252 - Batch of prepared statements for [Sandy.referral_source] is of size 71016, exceeding specified threshold of 65536 by 5480.

							if (nCell == itemPos)
                            {
                                var splitItems = SplitTableName(parsedValues[nCell]);

                                dataRow["Associated Item"] = splitItems.Item1 + '.' + splitItems.Item2;
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
                        else if (parsedValues[0] == "WARN" && parsedValues[4] == "NoSpamLogger.java")
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
                        else if (parsedValues[4] == "SliceQueryFilter.java")
                        {
                            #region SliceQueryFilter.java
                            //SliceQueryFilter.java (line 231) Read 14 live and 1344 tombstone cells in cma.mls_records_property (see tombstone_warn_threshold). 5000 columns was requested, slices=[-]
                            // Scanned over 100000 tombstones in capitalonehomeloans.homebase_he_operations_pt; query aborted (see tombstone_failure_threshold)
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
                            else if (parsedValues[0] == "ERROR" && parsedValues[nCell] == "Scanned" && parsedValues[nCell + 1] == "over")
                            {
                                itemPos = nCell + 5;
                                itemValuePos = 2;
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Exception"] = "Query Tombstones Aborted";
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[4] == "HintedHandoffMetrics.java" || parsedValues[4] == "HintedHandOffManager.java")
                        {
							#region HintedHandoffMetrics.java
							//		WARN  [HintedHandoffManager:1] 2016-07-25 04:26:10,445  HintedHandoffMetrics.java:79 - /10.170.110.191 has 1711 dropped hints, because node is down past configured hint window.
							//INFO  [HintedHandoff:2] 2016-10-29 09:04:51,254  HintedHandOffManager.java:486 - Timed out replaying hints to /10.12.51.20; aborting (0 delivered)
							if (parsedValues[nCell] == "dropped")
                            {
                                //dataRow["Associated Item"] = "Dropped Hints";
                                dataRow["Exception"] = "Dropped Hints (node down)";
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                handled = true;

                                if (LookForIPAddress(parsedValues[nCell - 3], ipAddress, out lineIPAddress))
                                {
                                    dataRow["Associated Item"] = lineIPAddress;
                                }

                                dataRow["Associated Value"] = int.Parse(parsedValues[nCell - 1]);
                            }
							else if (parsedValues[nCell] == "Timed"
										&& parsedValues[nCell + 1] == "out")
							{
								dataRow["Exception"] = "Hints (timeout)";
								dataRow["Flagged"] = (int)LogFlagStatus.Stats;
								handled = true;

								if (LookForIPAddress(parsedValues[nCell + 5].Replace(";", string.Empty), ipAddress, out lineIPAddress))
								{
									dataRow["Associated Item"] = lineIPAddress;
								}

								var matchNbrs = RegExNumerics.Matches(parsedValues[nCell + 7]);

								if(matchNbrs.Count > 0)
								{
									dataRow["Associated Value"] = int.Parse(matchNbrs[0].Value);
								}
								else
								{
									dataRow["Associated Value"] = 0;
								}

							}
							#endregion
						}
                        else if (parsedValues[4] == "StorageService.java")
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

                            if (parsedValues[0] == "WARN" && parsedValues[nCell] == "Flushing")
                            {
                                //dataRow["Associated Item"] = "Flushing CFS";
                                dataRow["Exception"] = "CFS Flush";
                                itemValuePos = nCell + 1;
                                handled = true;
                            }
                            else if (parsedValues[0] == "INFO" && parsedValues[nCell] == "Node")
                            {
                                itemValuePos = nCell + 3;
                            }
							else if((parsedValues[nCell] == "starting"
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
                        else if (parsedValues[4] == "StatusLogger.java")
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

                            if (parsedValues[nCell] == "ColumnFamily")
                            {
                                tableItem = parsedValues[4];
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
                        else if (parsedValues[4] == "MessagingService.java")
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
                        else if (parsedValues[4] == "CompactionTask.java")
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
                                        && compactionFllagThresholdInMS >= 0
                                        && time is int
                                        && (int)time >= compactionFllagThresholdInMS)
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
                        else if (parsedValues[4] == "RepairSession.java" || parsedValues[4] == "RepairJob.java")
                        {
                            #region RepairSession.java RepairJob.java
                            //ERROR [AntiEntropySessions:1857] 2016-06-10 21:56:53,281  RepairSession.java:276 - [repair #dc161200-2f4d-11e6-bd0c-93368bf2a346] Cannot proceed on repair because a neighbor (/10.27.34.54) is dead: session failed
                            //INFO[AntiEntropySessions: 9665] 2016 - 08 - 10 07:08:06, 218  RepairJob.java:163 - [repair #cde0eaa0-5ec0-11e6-8767-f5197346a00e] requesting merkle trees for memberfundingeventaggregate (to [/10.211.34.167, /10.211.34.165, /10.211.34.164, /10.211.34.158, /10.211.34.150])

                            if (parsedValues[0] == "ERROR")
                            {
                                if (parsedValues[nCell] == "Failed")
                                {
                                    dataRow["Exception"] = "Read Repair Failed";
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
                        else if (parsedValues[4] == "CqlSlowLogWriter.java")
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
                        else if (parsedValues[4] == "CqlSolrQueryExecutor.java")
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
                        else if (parsedValues[4] == "SolrCore.java")
                        {
                            #region SolrCore.java
                            //WARN  [SolrSecondaryIndex ks_invoice.invoice index initializer.] 2016-08-17 00:36:22,480  SolrCore.java:1726 - [ks_invoice.invoice] PERFORMANCE WARNING: Overlapping onDeckSearchers=2

                            if (parsedValues[0] == "WARN" && parsedValues[nCell] == "PERFORMANCE" && parsedValues[nCell + 1] == "WARNING:")
                            {
                                var splitItems = SplitTableName(parsedValues[nCell - 1]);
                                var ksTableName = splitItems.Item1 + '.' + splitItems.Item2;

                                dataRow["Exception"] = "Solr Performance Warning";
                                dataRow["Associated Item"] = ksTableName;
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[4] == "JVMStabilityInspector.java")
                        {
                            #region JVMStabilityInspector.java
                            //ERROR [MessagingService-Incoming-/10.12.49.27] 2016-09-28 18:53:54,898  JVMStabilityInspector.java:106 - JVM state determined to be unstable.  Exiting forcefully due to:
                            //java.lang.OutOfMemoryError: Java heap space

                            if (parsedValues[0] == "ERROR" && parsedValues[nCell] == "JVM" && parsedValues.ElementAtOrDefault(nCell + 5) == "unstable")
                            {
                                dataRow["Flagged"] = (int)LogFlagStatus.Stats;
                                dataRow["Associated Item"] = string.Join(" ", parsedValues.Skip(nCell + 5));
                                handled = true;
                            }
                            #endregion
                        }
                        else if (parsedValues[4] == "WorkPool.java")
                        {
                            #region WorkPool.java
                            //WARN  [commitScheduler-4-thread-1] 2016-09-28 18:53:32,436  WorkPool.java:413 - Timeout while waiting for workers when flushing pool Index; current timeout is 300000 millis, consider increasing it, or reducing load on the node.
                            //Failure to flush may cause excessive growth of Cassandra commit log.

                            if (parsedValues[0] == "WARN" && parsedValues[nCell] == "Timeout"
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
                        else if (parsedValues[4] == "QueryProcessor.java")
                        {
                            #region QueryProcessor.java
                            ////QueryProcessor.java:139 - 21 prepared statements discarded in the last minute because cache limit reached (66270208 bytes)
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
                        else if (parsedValues[4] == "ThriftServer.java")
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
                        else if (parsedValues[4] == "DseAuthenticator.java" && parsedValues[0] == "WARN")
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
                        else if ((parsedValues[4] == "PasswordAuthenticator.java"
                                    || parsedValues[4] == "Auth.java")
                                && parsedValues[0] == "WARN")
                        {
                            #region PasswordAuthenticator|Auth.java
                            //Auth.java					 Skipped default superuser setup: some nodes were not ready
                            //PasswordAuthenticator.java PasswordAuthenticator skipped default user setup: some nodes were not ready
                            if ((parsedValues[nCell] == "PasswordAuthenticator" && parsedValues[nCell + 1] == "skipped")
                                    || (parsedValues[nCell] == "Skipped" && parsedValues[nCell + 2] == "superuser"))
                            {
                                var lastOccurence = (from dr in dtCLog.AsEnumerable().TakeLast(10)
                                                     where dr.Field<string>("Item") == (parsedValues[4] == "PasswordAuthenticator.java"
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
						else if(parsedValues[4] == "AbstractSolrSecondaryIndex.java")
						{
							#region AbstractSolrSecondaryIndex
							//INFO  [SolrSecondaryIndex prod_fcra.rtics_contribution index reloader.] 2016-10-12 21:52:23,011  AbstractSolrSecondaryIndex.java:1566 - Finished reindexing on keyspace prod_fcra and column family rtics_contribution
							//INFO  [SolrSecondaryIndex prod_fcra.rtics_contribution index reloader.] 2016-10-12 21:12:02,937  AbstractSolrSecondaryIndex.java:1539 - Reindexing on keyspace prod_fcra and column family rtics_contribution
							//INFO  [SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016-10-18 23:03:09,999  AbstractSolrSecondaryIndex.java:546 - Reindexing 1117 commit log updates for core prod_fcra.bics_contribution
							//INFO[SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016 - 10 - 18 23:03:10,567  AbstractSolrSecondaryIndex.java:1133 - Executing hard commit on index prod_fcra.bics_contribution
							//INFO[SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016 - 10 - 18 23:03:13,616  AbstractSolrSecondaryIndex.java:581 - Reindexed 1117 commit log updates for core prod_fcra.bics_contribution
							//INFO[SolrSecondaryIndex prod_fcra.bics_contribution index initializer.] 2016 - 10 - 18 23:03:13, 618  AbstractSolrSecondaryIndex.java:584 - Truncated commit log for core prod_fcra.bics_contribution

							if (parsedValues[nCell] == "Reindexing"
								|| (parsedValues[nCell] == "Finished" && parsedValues[nCell + 1] == "reindexing")
								|| (parsedValues[nCell] == "Executing" && parsedValues[nCell + 1] == "hard")
								|| (parsedValues[nCell] == "Truncated" && parsedValues[nCell + 1] == "commit"))
							{
								dataRow["Flagged"] = (int)LogFlagStatus.StatsOnly;
							}
							#endregion
						}
						else if (dataRow["Associated Value"] == DBNull.Value
                                    && LookForIPAddress(parsedValues[nCell], ipAddress, out lineIPAddress))
                        {
                            dataRow["Associated Value"] = lineIPAddress;
                        }

                        if (!handled
                            && (parsedValues[0] == "WARN"
                                    || parsedValues[0] == "ERROR")
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

                    #endregion

                    dtCLog.Rows.Add(dataRow);
                    ++nbrRows;
                    lastRow = dataRow;
                }
            }

            maxTimestamp = minmaxDate.Max;

            if (!minmaxDate.IsEmpty())
            {
                lock (LogCassandraMaxMinTimestamp)
                {
                    LogCassandraMaxMinTimestamp.SetMinMax(minmaxDate.Min);
                    LogCassandraMaxMinTimestamp.SetMinMax(minmaxDate.Max);
                }

                LogCassandraNodeMaxMinTimestamps.AddOrUpdate(ipAddress,
                                                                strAddress => new List<Common.DateTimeRange>() { new Common.DateTimeRange(minmaxDate.Max, minmaxDate.Min) },
                                                                (strAddress, dtRanges) => { lock (dtRanges) { dtRanges.Add(minmaxDate); } return dtRanges; });
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
            if(exceptionClass.StartsWith("error...") && exceptionClass.Length > 8)
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

            if(checkLastException
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

				if(lastException == null)
				{
					ParseExceptions(ipAddress, (string)dataRow["Indicator"], dataRow, dataRow["Description"] as string, additionalUpdates, false);
					lastException = dataRow["Exception"] as string;
				}
            }

            dataRow.BeginEdit();

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

                            if(protocolPos > 0)
                            {
                                exceptionDescSplits[nIndex] = exceptionDescSplits[nIndex].Substring(0, protocolPos) + ":####";
                            }
                        }
                    }
                    if(lastException == null || !lastException.Contains(exceptionDescSplits[nIndex]))
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
                dtCSummaryLog.Columns.Add("Reconciliation Reference", typeof(long)).AllowDBNull = true; //L
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

                Parallel.ForEach(segments, element =>
                //foreach (var element in segments)
                {
					Program.ConsoleParsingLog.Increment(string.Format("Summary Parallel Segment Processing {0} to {1}", element.Item1, element.Item2));

					var segmentView = (from dr in dtroCLog.AsEnumerable()
									   let timeStamp = dr.Field<DateTime>("Timestamp")
									   let flagged = dr.Field<int?>("Flagged")
									   let indicator = dr.Field<string>("Indicator")
									   where element.Item1 >= timeStamp && element.Item2 < timeStamp
												&& ((flagged.HasValue && (flagged.Value == 1 || flagged.Value == 2))
														|| !dr.IsNull("Exception")
														|| indicator == "ERROR"
														|| indicator == "WARN")
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
									   });

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
                                if(string.IsNullOrEmpty(dataView.AssocItem))
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
                            else if(logAggregateAdditionalTaskExceptionItems.Contains(dataView.Item) && dataView.AssocItem != null)
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
                                    dataSummaryRow["Last Occurrence"] = item.MaxTimeStamp.HasValue ? (object) item.MaxTimeStamp.Value : DBNull.Value;
                                    dataSummaryRow["Occurrences"] = item.AggregationCount;
                                    dataSummaryRow["Reconciliation Reference"] = item.GroupIndicator;

                                    if(item.AssociatedItems.Count > 0)
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
            var exceptionNameSplit = RegExSummaryLogExceptionName.Split(keyNode);
            var exceptionName = exceptionNameSplit.Length < 2 ? keyNode : exceptionNameSplit[1];

            if (defaultNodes)
            {
                return exceptionName;
            }

            if(ParserSettings.SummaryIgnoreExceptions.Contains(exceptionName.ToLower()))
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
		static Regex RegExRepairNewSessionLine = new Regex(@"\s*\[repair\s+#(.+)\]\s+new session:.+on range \(([0-9-]+)\,\s*([0-9-]+)\]\s+for\s+(.+)\.\[.+\]",
												RegexOptions.IgnoreCase | RegexOptions.Compiled);
		//starting user-requested repair of range [(-1537228672809129313,-1531223873305968652]] for keyspace prod_fcra and column families [ifps_inquiry, ifps_contribution, rtics_contribution, rtics_inquiry, bics_contribution, avlo_inquiry, bics_inquiry]
		static Regex RegExRepairUserRequest = new Regex(@".*starting\s+user-requested\s+repair.+range\s+\[?\(\s*([0-9-]+)\s*\,\s*([0-9-]+)\s*\]\]?\s+.+keyspace\s+(.+)\s+and.+",
															RegexOptions.IgnoreCase | RegexOptions.Compiled);
		//Starting repair command #9, repairing 1 ranges for keyspace prod_fcra (parallelism=PARALLEL, full=true)
		static Regex RegExRepairUserRequest1 = new Regex(@".*Starting\s+repair\s+command\s.+keyspace\s+(.+)\s+\((.+)\)",
															RegexOptions.IgnoreCase | RegexOptions.Compiled);
		//[repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] session completed successfully
		static Regex RegExRepairEndSessionLine = new Regex(@"\s*\[repair\s+#(.+)\]\s+session completed\s+(.+)",
												RegexOptions.IgnoreCase | RegexOptions.Compiled);
		//[streaming task #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] Performing streaming repair of 1 ranges with /10.12.51.29
		static Regex RegExRepairNbrRangesLine = new Regex(@"\[.+\s+#(.+)\]\s+Performing streaming repair.+\s+(\d+)\s+ranges.+\/(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})",
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

		public class GCLogInfo
		{
			public DateTime LogTimestamp;
			public string DataCenter;
			public string IPAddress;
			public int GCLatency;
			public decimal GCEdenFrom;
			public decimal GCEdenTo;
			public decimal GCSurvivorFrom;
			public decimal GCSurvivorTo;
			public decimal GCOldFrom;
			public decimal GCOldTo;
			public long GroupIndicator;

			public DateTime StartTime { get { return this.LogTimestamp.Subtract(new TimeSpan(0, 0, 0, 0, this.GCLatency)); } }
		}

		public class CompactionLogInfo
		{
			public DateTime LogTimestamp;
			public string DataCenter;
			public string IPAddress;
			public string Keyspace;
			public string Table;
			public int SSTables;
			public decimal OldSize;
			public decimal NewSize;
			public int Latency;
			public decimal IORate;
			public string PartitionsMerged;
			public string MergeCounts;
			public long GroupIndicator;

			public DateTime StartTime { get { return this.LogTimestamp.Subtract(new TimeSpan(0, 0, 0, 0, this.Latency)); } }
		}

		public class SolrReIndexingLogInfo
		{
			public DateTime Start;
			public DateTime Finish;
			public string DataCenter;
			public string IPAddress;
			public string Keyspace;
			public string Table;
			public int NbrUpdates;
			public long GroupIndicator;

			public int Duration { get { return (int)(Finish - Start).TotalMilliseconds; } }
		}

		public class ReadRepairLogInfo
		{
			public string SessionPath;
			public string Session;
			public string DataCenter;
			public string IPAddress;
			public string Keyspace;
			public string Options;
			public bool UserRequest;
			public DateTime Start;
			public DateTime Finish;
			public int Duration { get { return (int) (Finish - Start).TotalMilliseconds; } }
			public string TokenRangeStart;
			public string TokenRangeEnd;
			public int GCs {  get { return this.GCList == null ? 0 : this.GCList.Count(); } }
			public int Compactions { get { return this.CompactionList == null ? 0 : this.CompactionList.Count(); } }
			public int Exceptions;

			public long GroupInd;
			public bool Aborted;
			public string Exception;
			public int NbrRepairs;
			public List<string> ReceivingNodes = new List<string>();
			public IEnumerable<SolrReIndexingLogInfo> SolrReIndexing = null;
			public IEnumerable<GCLogInfo> GCList = null;
			public IEnumerable<CompactionLogInfo> CompactionList = null;

			public static string DCIPAddress(string dcName, string ipAdress)
			{
				return (dcName == null ? string.Empty : dcName) + "|" + ipAdress;
			}

			public ReadRepairLogInfo Add()
			{
				return this;
			}

			public ReadRepairLogInfo Completed(DateTime timestamp)
			{
				this.Finish = timestamp;

				return this;
			}

			public ReadRepairLogInfo Abort(DateTime timestamp, string exception = null)
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

        static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<GCLogInfo>> GCOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<GCLogInfo>>();
		static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<CompactionLogInfo>> CompactionOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<CompactionLogInfo>>();
		static Common.Patterns.Collections.ThreadSafe.Dictionary<string /*DataCenter|Node IPAddress*/, Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo>> SolrReindexingOccurrences = new Common.Patterns.Collections.ThreadSafe.Dictionary<string, Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo>>();

		static void InitializeStatusDataTable(DataTable dtCStatusLog)
		{
			if (dtCStatusLog.Columns.Count == 0)
			{
				dtCStatusLog.Columns.Add("Reconciliation Reference", typeof(long)).AllowDBNull = true;

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
				dtCStatusLog.Columns.Add("Nbr Solr ReIdxs", typeof(int)).AllowDBNull = true; //al
				dtCStatusLog.Columns.Add("Nbr Exceptions", typeof(int)).AllowDBNull = true; //am
				dtCStatusLog.Columns.Add("Requested", typeof(bool)).AllowDBNull = true; //an
				dtCStatusLog.Columns.Add("Aborted", typeof(int)).AllowDBNull = true; //ao
				dtCStatusLog.Columns.Add("Session Path", typeof(string)).AllowDBNull = true; //ap

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
                var groupIndicator = CLogSummaryInfo.IncrementGroupInicator();
				var statusLogItem = from dr in dtroCLog.AsEnumerable()
									let item = dr.Field<string>("Item")
									let timestamp = dr.Field<DateTime>("Timestamp")
									let flagged = dr.Field<int?>("Flagged")
									let descr = dr.Field<string>("Description")?.Trim()
									where ((flagged.HasValue && (flagged.Value == 2 || flagged.Value == 4))
											|| item == "GCInspector.java"
											|| item == "StatusLogger.java"
											|| item == "CompactionTask.java")
									orderby timestamp ascending, item ascending
									select new { Task = dr.Field<string>("Task"),
													Item = item,
													Timestamp = timestamp,
													Flagged = flagged,
													Exception = dr.Field<string>("Exception"),
													AssocItem = dr.Field<string>("Associated Item"),
													AssocValue = dr.Field<object>("Associated Value"),
													Description = descr };


                var gcLatencies = new List<int>();
                var pauses = new List<long>();
                var compactionLatencies = new List<Tuple<string, string, int>>();
                var compactionRates = new List<Tuple<string, string, decimal>>();
                var partitionLargeSizes = new List<Tuple<string, string, decimal>>();
                var tombstoneCounts = new List<Tuple<string, string, string, int>>();
                var tpStatusCounts = new List<Tuple<string, long, long, long, long, long>>();
                var statusMemTables = new List<Tuple<string, string, long, decimal>>();
                var tpSlowQueries = new List<int>();
                var batchSizes = new List<Tuple<string, string, string, int>>();
                var jvmFatalErrors = new List<string>();
                var workPoolErrors = new List<string>();
                var nodeStatus = new List<Tuple<string,string,string>>();
                var droppedHints = new List<int>();
				var timedoutHints = new List<int>();
				var droppedMutations = new List<int>();
                var maxMemoryAllocFailed = new List<int>();
				var solrReindexing = new List<SolrReIndexingLogInfo>();

				foreach (var item in statusLogItem)
                {
                    if (string.IsNullOrEmpty(item.Item))
                    {
                        continue;
                    }

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
                            dataRow["Reconciliation Reference"] = groupIndicator;

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
                            dataRow["GC Time (ms)"] = (long) ((dynamic) time);
                            dataRow["Reconciliation Reference"] = groupIndicator;

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
                            dataRow["GC Time (ms)"] = (long) ((dynamic) time);
                            dataRow["Reconciliation Reference"] = groupIndicator;

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
								GCLatency = (int)((dynamic)time),
								GroupIndicator = groupIndicator,
								GCEdenFrom = dataRow.IsNull("Eden-From (mb)") ? 0 : dataRow.Field<decimal>("Eden-From (mb)"),
								GCEdenTo = dataRow.IsNull("Eden-To (mb)") ? 0 : dataRow.Field<decimal>("Eden-To (mb)"),
								GCSurvivorFrom = dataRow.IsNull("Survivor-From (mb)") ? 0 : dataRow.Field<decimal>("Survivor-From (mb)"),
								GCSurvivorTo = dataRow.IsNull("Survivor-To (mb)") ? 0 : dataRow.Field<decimal>("Survivor-To (mb)"),
								GCOldFrom = dataRow.IsNull("Old-From (mb)") ? 0 : dataRow.Field<decimal>("Old-From (mb)"),
								GCOldTo = dataRow.IsNull("Old-To (mb)") ? 0 : dataRow.Field<decimal>("Old-To (mb)"),
							};

							GCOccurrences.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
                                                        ignore => { return new Common.Patterns.Collections.ThreadSafe.List<GCLogInfo>() { gcLogInfo }; },
                                                        (ignore, gcList) => {
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
                            dataRow["Reconciliation Reference"] = groupIndicator;

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
                                    dataRow["Reconciliation Reference"] = groupIndicator;
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

                                    dataRow["Timestamp"] = item.Timestamp;
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = "ColumnFamily";
                                    dataRow["KeySpace"] = ksTable.Item1;
                                    dataRow["Table"] = ksTable.Item2;
                                    dataRow["Reconciliation Reference"] = groupIndicator;
                                    dataRow["MemTable OPS"] = long.Parse(splits[2]);
                                    dataRow["Data (mb)"] = ConvertInToMB(splits[3], "bytes");

                                    dtCStatusLog.Rows.Add(dataRow);

                                    statusMemTables.Add(new Tuple<string, string, long, decimal>(ksTable.Item1,
                                                                                                    ksTable.Item2,
                                                                                                    (long)dataRow["MemTable OPS"],
                                                                                                    (decimal)dataRow["Data (mb)"]));
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
                                    dataRow["Reconciliation Reference"] = groupIndicator;

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
                                var msg = string.Format("StatusLogger Invalid Line \"{0}\" for {1}", descr, ipAddress);
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
                            var fileNamePos = ((string)splits[2]).LastIndexOf('/');
                            var ssTableFileName = ((string)splits[2]).Substring(fileNamePos + 1);
                            var ksItem = kstblExists
                                            .Where(e => ssTableFileName.StartsWith(e.LogName))
                                            .OrderByDescending(e => e.LogName.Length).FirstOrDefault();

                            if (ksItem != null)
                            {
                                if (ignoreKeySpaces.Contains(ksItem.KeySpaceName))
                                {
                                    continue;
                                }

                                var dataRow = dtCStatusLog.NewRow();
                                var time = DetermineTime(splits[6]);
                                var rate = decimal.Parse(splits[7].Replace(",", string.Empty));

                                dataRow["Timestamp"] = item.Timestamp;
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["Pool/Cache Type"] = "Compaction";
                                dataRow["Reconciliation Reference"] = groupIndicator;

                                dataRow["KeySpace"] = ksItem.KeySpaceName;
                                dataRow["Table"] = ksItem.TableName;
                                dataRow["SSTables"] = int.Parse(splits[1].Replace(",", string.Empty));
                                dataRow["From (mb)"] = ConvertInToMB(splits[3], "bytes");
                                dataRow["To (mb)"] = ConvertInToMB(splits[4], "bytes");
                                dataRow["Latency (ms)"] = time;
                                dataRow["Rate (MB/s)"] = rate;
                                dataRow["Partitions Merged"] = splits[8] + ":" + splits[9];
                                dataRow["Merge Counts"] = splits[10];

                                dtCStatusLog.Rows.Add(dataRow);
                                compactionLatencies.Add(new Tuple<string, string, int>(ksItem.KeySpaceName, ksItem.TableName, (int)time));
                                compactionRates.Add(new Tuple<string, string, decimal>(ksItem.KeySpaceName, ksItem.TableName, rate));

								var compactionLogInfo = new CompactionLogInfo()
								{
									LogTimestamp = item.Timestamp,
									DataCenter = dcName,
									IPAddress = ipAddress,
									GroupIndicator = groupIndicator,
									Keyspace = ksItem.KeySpaceName,
									Table = ksItem.TableName,
									SSTables = dataRow.Field<int>("SSTables"),
									OldSize = dataRow.Field<decimal>("From (mb)"),
									NewSize = dataRow.Field<decimal>("To (mb)"),
									Latency = (int)time,
									IORate = rate,
									PartitionsMerged = dataRow.Field<string>("Partitions Merged"),
									MergeCounts = dataRow.Field<string>("Merge Counts")
								};

								CompactionOccurrences.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress,
									ignore =>
									{
										return new Common.Patterns.Collections.ThreadSafe.List<CompactionLogInfo>() { compactionLogInfo };
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
                    else if (item.Item == "CompactionController.java" || item.Item == "SSTableWriter.java")
                    {
                        #region CompactionController or SSTableWriter

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
                        dataRow["Reconciliation Reference"] = groupIndicator;

                        dataRow["KeySpace"] = kstblSplit.Item1;
                        dataRow["Table"] = kstblSplit.Item2;
                        dataRow["Size (mb)"] = partSize.Value / BytesToMB;

                        dtCStatusLog.Rows.Add(dataRow);

                        partitionLargeSizes.Add(new Tuple<string, string, decimal>(kstblSplit.Item1, kstblSplit.Item2, partSize.Value));

                        #endregion
                    }
                    else if (item.Item == "SliceQueryFilter.java")
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
                        dataRow["Reconciliation Reference"] = groupIndicator;

                        dataRow["KeySpace"] = kstblSplit.Item1;
                        dataRow["Table"] = kstblSplit.Item2;
                        dataRow["Completed"] = (long) partSize.Value;

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
                            dataRow["Reconciliation Reference"] = groupIndicator;
                            dataRow["Latency (ms)"] = time.Value;

                            dtCStatusLog.Rows.Add(dataRow);

                            tpSlowQueries.Add(time.Value);
                        }

                        #endregion
                    }
                    else if(item.Item == "BatchStatement.java")
                    {
                        #region BatchSize

                        var kstblName = item.AssocItem;
                        var batchSize = item.AssocValue as int?;

                        if (kstblName == null || !batchSize.HasValue)
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
                        dataRow["Pool/Cache Type"] = "Batch size";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["KeySpace"] = kstblSplit.Item1;
                        dataRow["Table"] = kstblSplit.Item2;
                        dataRow["Completed"] = (long)batchSize.Value;

                        dtCStatusLog.Rows.Add(dataRow);

                        batchSizes.Add(new Tuple<string, string, string, int>("Batch size", kstblSplit.Item1, kstblSplit.Item2, batchSize.Value));

                        #endregion
                    }
                    else if (item.Item == "JVMStabilityInspector.java")
                    {
						#region JVMStabilityInspector

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
                            dataRow["Reconciliation Reference"] = groupIndicator;

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
                            dataRow["Reconciliation Reference"] = groupIndicator;

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
                            dataRow["Reconciliation Reference"] = groupIndicator;
                            dataRow["KeySpace"] = ksName;
                            dataRow["Table"] = tblName;

                            dtCStatusLog.Rows.Add(dataRow);

                            nodeStatus.Add(new Tuple<string,string,string>(item.Exception, ksName, tblName));
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
								dataRow["Reconciliation Reference"] = groupIndicator;
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

                            if(string.IsNullOrEmpty(strTables) || !assocValue.HasValue)
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
                                    if(ignoreKeySpaces.Contains(keyTbl.Keyspace))
                                    {
                                        continue;
                                    }

                                    var dataRow = dtCStatusLog.NewRow();

                                    dataRow["Timestamp"] = item.Timestamp;
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = item.Exception + " Count";
                                    dataRow["Reconciliation Reference"] = groupIndicator;
                                    dataRow["KeySpace"] = keyTbl.Keyspace;
                                    dataRow["Table"] = keyTbl.Table;
                                    dataRow["Completed"] = (long)assocValue.Value;

                                    dtCStatusLog.Rows.Add(dataRow);

                                    batchSizes.Add(new Tuple<string, string, string, int>(item.Exception + " Count", keyTbl.Keyspace, keyTbl.Table, assocValue.Value));
                                }
                            }
                        }
                        else if(item.Exception == "Maximum Memory Reached cannot Allocate")
                        {
                            //"Associated Value" -- bytes
                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "Allocation Failed Maximum Memory Reached";
                            dataRow["Reconciliation Reference"] = groupIndicator;

                            if (assocValue.HasValue)
                            {
                                dataRow["Capacity (mb)"] = ((decimal) assocValue.Value) / BytesToMB;
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

                        if(item.Exception == "Dropped Mutations")
                        {
                            var assocValue = item.AssocValue as int?;
                            var dataRow = dtCStatusLog.NewRow();

                            dataRow["Timestamp"] = item.Timestamp;
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "Dropped Mutation";
                            dataRow["Reconciliation Reference"] = groupIndicator;

                            dataRow["Completed"] = (long) assocValue.Value;
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
											&& solrReIndx.Finish == DateTime.MinValue))
									{
										solrReIndx = solrReindexing
														.Find(s => s.Keyspace == regExSolrReIndex[1]
																		&& s.Table == regExSolrReIndex[2]
																		&& s.Finish == DateTime.MinValue);
									}

									if(solrReIndx != null)
									{
										solrReIndx.Finish = item.Timestamp;
										solrReIndx.GroupIndicator = groupIndicator;

										var dataRow = dtCStatusLog.NewRow();

										dataRow["Timestamp"] = item.Timestamp;
										dataRow["Data Center"] = dcName;
										dataRow["Node IPAddress"] = ipAddress;
										dataRow["Pool/Cache Type"] = "Solr ReIndex Finish";
										dataRow["KeySpace"] = solrReIndx.Keyspace;
										dataRow["Table"] = solrReIndx.Table;
										dataRow["Reconciliation Reference"] = groupIndicator;
										dataRow["Latency (ms)"] = solrReIndx.Duration;

										dtCStatusLog.Rows.Add(dataRow);
									}
								}
							}
							else
							{
								var solrReIndx = new SolrReIndexingLogInfo()
								{
									Start = item.Timestamp,
									DataCenter = dcName,
									IPAddress = ipAddress,
									Keyspace = regExSolrReIndex[1].Trim(),
									Table = regExSolrReIndex[2].Trim(),
									GroupIndicator = groupIndicator
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

						if(regExSolrReIndex.Length > 2)
						{
							if (ignoreKeySpaces.Contains(regExSolrReIndex[2].Trim()))
							{
								continue;
							}

							var solrReIndx = new SolrReIndexingLogInfo()
							{
								Start = item.Timestamp,
								DataCenter = dcName,
								IPAddress = ipAddress,
								Keyspace = regExSolrReIndex[2].Trim(),
								Table = regExSolrReIndex[3].Trim(),
								NbrUpdates = int.Parse(regExSolrReIndex[1]),
								GroupIndicator = groupIndicator
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
										&& solrReIndx.Finish == DateTime.MinValue))
								{
									solrReIndx = solrReindexing
													.Find(s => s.Keyspace == regExSolrReIndex[1]
																	&& s.Table == regExSolrReIndex[2]
																	&& s.Finish == DateTime.MinValue);
								}

								if (solrReIndx != null)
								{
									solrReIndx.Finish = item.Timestamp;
									solrReIndx.GroupIndicator = groupIndicator;

									var dataRow = dtCStatusLog.NewRow();

									dataRow["Timestamp"] = item.Timestamp;
									dataRow["Data Center"] = dcName;
									dataRow["Node IPAddress"] = ipAddress;
									dataRow["Pool/Cache Type"] = "Solr ReIndex Finish";
									dataRow["KeySpace"] = solrReIndx.Keyspace;
									dataRow["Table"] = solrReIndx.Table;
									dataRow["Reconciliation Reference"] = groupIndicator;
									dataRow["Latency (ms)"] = solrReIndx.Duration;
									dtCStatusLog.Rows.Add(dataRow);
								}
							}
						}

						#endregion

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
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = gcMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC maximum latency";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = gcMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC mean latency";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = gcAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC occurrences";
                        dataRow["Reconciliation Reference"] = groupIndicator;
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
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = slowMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query maximum latency";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = slowMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query mean latency";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = slowAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query occurrences";
                        dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
                            dataRow["Occurrences"] = tpItem.Count;

                            dtTPStats.Rows.Add(dataRow);
                        }
                    }

                    #endregion

                    #region Pause

                    if (pauses.Count > 0)
                    {
                        Logger.Instance.InfoFormat("Adding Pause ({2}) to TPStats for \"{0}\" \"{1}\"", dcName, ipAddress, pauses.Count);

                        var gcMax = (int) pauses.Max();
                        var gcMin = (int) pauses.Min();
                        var gcAvg = (int) pauses.Average();

                        var dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause minimum latency";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = gcMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause maximum latency";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = gcMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause mean latency";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Latency (ms)"] = gcAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause occurrences";
                        dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
                        var droppedAvgNbr = (int) droppedHints.Average();
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
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedTotalNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints maximum";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedMaxNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints mean";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedAvgNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints minimum";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedMinNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Hints occurrences";
                        dataRow["Reconciliation Reference"] = groupIndicator;
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
						dataRow["Reconciliation Reference"] = groupIndicator;
						dataRow["Completed"] = timedoutTotalNbr;
						//dataRow["Occurrences"] = statusGrp.Count;

						dtTPStats.Rows.Add(dataRow);

						dataRow = dtTPStats.NewRow();

						dataRow["Source"] = "Cassandra Log";
						dataRow["Data Center"] = dcName;
						dataRow["Node IPAddress"] = ipAddress;
						dataRow["Attribute"] = "Timedout Hints maximum";
						dataRow["Reconciliation Reference"] = groupIndicator;
						dataRow["Completed"] = timedoutMaxNbr;
						//dataRow["Occurrences"] = statusGrp.Count;

						dtTPStats.Rows.Add(dataRow);

						dataRow = dtTPStats.NewRow();

						dataRow["Source"] = "Cassandra Log";
						dataRow["Data Center"] = dcName;
						dataRow["Node IPAddress"] = ipAddress;
						dataRow["Attribute"] = "Timedout Hints mean";
						dataRow["Reconciliation Reference"] = groupIndicator;
						dataRow["Completed"] = timedoutAvgNbr;
						//dataRow["Occurrences"] = statusGrp.Count;

						dtTPStats.Rows.Add(dataRow);

						dataRow = dtTPStats.NewRow();

						dataRow["Source"] = "Cassandra Log";
						dataRow["Data Center"] = dcName;
						dataRow["Node IPAddress"] = ipAddress;
						dataRow["Attribute"] = "Timedout Hints minimum";
						dataRow["Reconciliation Reference"] = groupIndicator;
						dataRow["Completed"] = timedoutMinNbr;
						//dataRow["Occurrences"] = statusGrp.Count;

						dtTPStats.Rows.Add(dataRow);

						dataRow = dtTPStats.NewRow();

						dataRow["Source"] = "Cassandra Log";
						dataRow["Data Center"] = dcName;
						dataRow["Node IPAddress"] = ipAddress;
						dataRow["Attribute"] = "Timedout Hints occurrences";
						dataRow["Reconciliation Reference"] = groupIndicator;
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
                        var allocAvgMem = (decimal) maxMemoryAllocFailed.Average();
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
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Size (mb)"] = ((decimal) allocTotalMem) / BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached maximum";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Size (mb)"] = ((decimal)allocMaxMem) / BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached mean";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Size (mb)"] = allocAvgMem/BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached minimum";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Size (mb)"] = ((decimal) allocMinMem)/BytesToMB;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Allocation Failed Maximum Memory Reached occurrences";
                        dataRow["Reconciliation Reference"] = groupIndicator;
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
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedTotalNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation maximum";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedMaxNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation mean";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedAvgNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation minimum";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        dataRow["Dropped"] = droppedMinNbr;
                        //dataRow["Occurrences"] = statusGrp.Count;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Dropped Mutation occurrences";
                        dataRow["Reconciliation Reference"] = groupIndicator;
                        //dataRow["Value"] = droppedMinNbr;
                        dataRow["Occurrences"] = droppedOccurences;

                        dtTPStats.Rows.Add(dataRow);
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Attribute"] = "MemTable OPS maximum";
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Attribute"] = "MemTable OPS minimum";
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Attribute"] = "MemTable OPS mean";
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Attribute"] = "MemTable occurrences";
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Attribute"] = "MemTable Size maximum";
                                dataRow["Reconciliation Reference"] = groupIndicator;
                                dataRow["Value"] = (int)(statItem.maxItem4 * BytesToMB);
                                dataRow["Size in MB"] = statItem.maxItem4;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Size minimum";
                                dataRow["Reconciliation Reference"] = groupIndicator;
                                dataRow["Value"] = (int)(statItem.minItem4 * BytesToMB);
                                dataRow["Size in MB"] = statItem.minItem4;
                                dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);

                                dataRow = dtCFStats.NewRow();

                                dataRow["Source"] = "Cassandra Log";
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["KeySpace"] = statItem.KeySpace;
                                dataRow["Table"] = statItem.Table;
                                dataRow["Attribute"] = "MemTable Size mean";
                                dataRow["Reconciliation Reference"] = groupIndicator;
                                dataRow["Value"] = (int)(statItem.avgItem4 * BytesToMB);
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                                dataRow["Reconciliation Reference"] = groupIndicator;
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
                            dataRow["Reconciliation Reference"] = groupIndicator;
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
							dataRow["Reconciliation Reference"] = groupIndicator;
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
							dataRow["Reconciliation Reference"] = groupIndicator;
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
							dataRow["Reconciliation Reference"] = groupIndicator;
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
							dataRow["Reconciliation Reference"] = groupIndicator;
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
							dataRow["Reconciliation Reference"] = groupIndicator;
							dataRow["Value"] = statusGrp.Count;
							dataRow["(value)"] = statusGrp.Count;

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
            public List<long> GroupRefIds;
            public List<DateTime> Timestamps;
			public List<SpaceChanged> GCSpacePoolChanges;
            public string Type;
            public decimal Percentage;
            public bool Deleted = false;
        };

        public static void DetectContinuousGCIntoNodeStats(DataTable dtNodeStats,
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

            System.Threading.Tasks.Parallel.ForEach(GCOccurrences, gcInfo =>
            //foreach (var gcInfo in GCOccurrences)
                {
                    var gcInfoTimeLine = gcInfo.Value.OrderBy(item => item.LogTimestamp);
                    DateTime timeFrame = gcDetectionPercent < 0 || gcTimeframeDetection == TimeSpan.Zero
                                            ? DateTime.MinValue
                                            : gcInfoTimeLine.First().LogTimestamp + gcTimeframeDetection;
                    GCContinuousInfo detectionTimeFrame = timeFrame == DateTime.MinValue
                                                            ? null
                                                            : new GCContinuousInfo()
                                                            {
                                                                Node = gcInfo.Key,
                                                                Latencies = new List<int>() { gcInfoTimeLine.First().GCLatency },
                                                                GroupRefIds = new List<long>() { gcInfoTimeLine.First().GroupIndicator },
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

                    for (int nIndex = 1; nIndex < gcInfoTimeLine.Count(); ++nIndex)
                    {
                        #region GC Continous (overlapping)

                        if (overlapToleranceInMS >= 0
                                && gcInfoTimeLine.ElementAt(nIndex - 1).LogTimestamp.AddMilliseconds(gcInfoTimeLine.ElementAt(nIndex).GCLatency + overlapToleranceInMS)
                                            >= gcInfoTimeLine.ElementAt(nIndex).LogTimestamp)
                        {
                            if (overLapped)
                            {
                                currentGCOverlappingInfo.Latencies.Add(gcInfoTimeLine.ElementAt(nIndex).GCLatency);
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
                                    Latencies = new List<int>() { gcInfoTimeLine.ElementAt(nIndex - 1).GCLatency, gcInfoTimeLine.ElementAt(nIndex).GCLatency },
                                    GroupRefIds = new List<long>() { gcInfoTimeLine.ElementAt(nIndex - 1).GroupIndicator },
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
                            detectionTimeFrame.Latencies.Add(gcInfoTimeLine.ElementAt(nIndex).GCLatency);
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
                                Latencies = new List<int>() { gcInfoTimeLine.ElementAt(nIndex).GCLatency },
                                GroupRefIds = new List<long>() { gcInfoTimeLine.ElementAt(nIndex).GroupIndicator },
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
                            #endregion
                        }

                        ++nbrAdded;
                    }
                }

                Logger.Instance.InfoFormat("Adding GC Continuous Occurrences ({0}) to TPStats", nbrAdded);

            }
        }

		public static IEnumerable<ReadRepairLogInfo> ParseReadRepairFromLog(DataTable dtroCLog,
																			Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
																			Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
																			IEnumerable<string> ignoreKeySpaces)
		{

			if (dtroCLog.Rows.Count == 0)
			{
				return Enumerable.Empty<ReadRepairLogInfo>();
			}

			var globalReadRepairs = new Common.Patterns.Collections.ThreadSafe.List<ReadRepairLogInfo>();
			{
				var logItems = from dr in dtroCLog.AsEnumerable()
							   let dcName = dr.Field<string>("Data Center")
							   let ipAddress = dr.Field<string>("Node IPAddress")
							   let item = dr.Field<string>("Item")
							   let timestamp = dr.Field<DateTime>("Timestamp")
							   let flagged = dr.Field<int?>("Flagged")
							   let descr = dr.Field<string>("Description")?.Trim()
							   let exception = dr.Field<string>("Exception")
							   where ((flagged.HasValue && (flagged.Value == 3 || flagged.Value == 1))
									   || (item == "RepairSession.java"
											   && (descr.Contains("new session") || descr.Contains("session completed successfully")))
									   || (item == "StreamingRepairTask.java"
												&& descr.Contains("Performing streaming repair"))
										|| (exception != null
												&& (exception.StartsWith("Node Shutdown")
														|| exception.StartsWith("Node Startup"))))
							   group new
							   {
								   Task = dr.Field<string>("Task"),
								   Item = item,
								   Timestamp = timestamp,
								   Flagged = flagged,
								   Exception = exception,
								   Description = descr
							   }
							   by new { dcName, ipAddress } into g
							   select new
							   {
								   DCName = g.Key.dcName,
								   IPAddress = g.Key.ipAddress,
								   LogItems = (from l in g orderby l.Timestamp ascending select l)
							   };

				//Parallel.ForEach(logItems, logGroupItem =>
				foreach (var logGroupItem in logItems)
				{
					DateTime oldestLogTimeStamp = DateTime.MaxValue;
					var dtStatusLog = new DataTable(string.Format("NodeStatus-ReadRepair-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));
					var currentReadRepairs = new List<ReadRepairLogInfo>();
					var optionsReadRepair = new List<Tuple<string, string>>(); //Keyspace, Options
					var userRequests = new List<Tuple<string, string, bool>>(); //Start Range, Keyspace, User Request
					var readRepairs = new List<ReadRepairLogInfo>();
					var groupIndicator = CLogSummaryInfo.IncrementGroupInicator();

					if(dtLogStatusStack != null) dtLogStatusStack.Push(dtStatusLog);

					InitializeStatusDataTable(dtStatusLog);

					foreach (var item in logGroupItem.LogItems)
					{
						oldestLogTimeStamp = item.Timestamp;

						if (item.Item == "RepairSession.java")
						{
							#region RepairSession.java
							//[repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] new session: will sync c1249170.ews.int/10.12.49.11, /10.12.51.29 on range (7698152963051967815,7704157762555128476] for OpsCenter.[rollups7200, settings, events, rollups60, rollups86400, rollups300, events_timeline, backup_reports, bestpractice_results, pdps]
							var regExNewSession = RegExRepairNewSessionLine.Split(item.Description);

							if (regExNewSession.Length > 4)
							{
								#region New Session
								if (ignoreKeySpaces.Contains(regExNewSession[4].Trim()))
								{
									continue;
								}

								ReadRepairLogInfo logInfo = currentReadRepairs.Find(r => r.Keyspace == regExNewSession[4].Trim()
																							&& string.IsNullOrEmpty(r.Session));
								if (logInfo == null)
								{
									logInfo = new ReadRepairLogInfo()
									{
										Session = regExNewSession[1].Trim(),
										DataCenter = logGroupItem.DCName,
										IPAddress = logGroupItem.IPAddress,
										Keyspace = regExNewSession[4].Trim(),
										Start = item.Timestamp,
										//Finish;
										TokenRangeStart = regExNewSession[2].Trim(),
										TokenRangeEnd = regExNewSession[3].Trim(),
										GroupInd = groupIndicator
									};
								}
								else
								{
									logInfo.Session = regExNewSession[1].Trim();
									logInfo.Start = item.Timestamp;
									logInfo.TokenRangeStart = regExNewSession[2].Trim();
									logInfo.TokenRangeEnd = regExNewSession[3].Trim();
								}

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

								dataRow["Session Path"] = logInfo.SessionPath = string.Join("=>", currentReadRepairs.Select(r => r.Session));
								dtStatusLog.Rows.Add(dataRow);
								#endregion
							}
							else
							{
								//[repair #eb7a25d0-94ae-11e6-a056-0dbb03ae0b81] session completed successfully
								var regExEndSession = RegExRepairEndSessionLine.Split(item.Description);

								if (regExEndSession.Length > 1)
								{
									#region Session Completed
									var logInfo = currentReadRepairs.Find(r => r.Session == regExEndSession[1]);

									if (logInfo != null)
									{
										var rrOption = optionsReadRepair.FindAll(u => u.Item1 == logInfo.Keyspace).LastOrDefault();
										var userRequest = userRequests.FindAll(u => u.Item1 == logInfo.TokenRangeStart && u.Item2 == logInfo.Keyspace).LastOrDefault();
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
										dataRow["Nbr Exceptions"] = logInfo.Exceptions;
										dataRow["Latency (ms)"] = logInfo.Duration;
										if (logInfo.UserRequest) dataRow["Requested"] = logInfo.UserRequest;

										currentReadRepairs.Remove(logInfo);

										if(rrOption != null)
										{
											logInfo.Options = rrOption.Item2;
											optionsReadRepair.Remove(rrOption);
										}
										if(userRequest != null)
										{
											logInfo.UserRequest = userRequest.Item3;
											userRequests.Remove(userRequest);
										}

										logInfo.Completed(item.Timestamp);

										dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(r => r.Session)) + "X" + logInfo.Session;

										dtStatusLog.Rows.Add(dataRow);
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

									if(!gcInfo.ReceivingNodes.Contains(regExNbrSession[3]))
									{
										string ipAddress;
										if (IPAddressStr(regExNbrSession[3], out ipAddress))
										{
											gcInfo.ReceivingNodes.Add(ipAddress);
										}
									}
								}
							}

							#endregion
						}
						else if(item.Item == "StorageService.java")
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
							if (item.Exception.StartsWith("Node Shutdown") || item.Exception.StartsWith("Node Startup"))
							{
								currentReadRepairs.AsEnumerable().Reverse().ToArray().ForEach(r =>
								{
									if (!string.IsNullOrEmpty(r.Session))
									{
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
										dataRow["Nbr Exceptions"] = r.Exceptions;
										dataRow["Latency (ms)"] = r.Duration;
										if (r.UserRequest) dataRow["Requested"] = r.UserRequest;
										dataRow["Aborted"] = 1;
										dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(i => "X" + i.Session));
										currentReadRepairs.Remove(r);
										dtStatusLog.Rows.Add(dataRow);
									}

									r.Abort(item.Timestamp, item.Exception);
								});

								currentReadRepairs.Clear();
							}
							else
							{
								currentReadRepairs.Where(r => r.Session != null && item.Description.Contains(r.Session)).ToArray().ForEach(r =>
								{
									var dataRow = dtStatusLog.NewRow();

									dataRow["Timestamp"] = item.Timestamp;
									dataRow["Data Center"] = logGroupItem.DCName;
									dataRow["Node IPAddress"] = logGroupItem.IPAddress;
									dataRow["Pool/Cache Type"] = "Read Repair (Exception) Aborted";
									dataRow["Reconciliation Reference"] = groupIndicator;

									dataRow["Session"] = r.Session;
									dataRow["Start Token Range (exclusive)"] = r.TokenRangeStart;
									dataRow["End Token Range (inclusive)"] = r.TokenRangeEnd;
									dataRow["KeySpace"] = r.Keyspace;
									dataRow["Nbr GCs"] = r.GCs;
									dataRow["Nbr Compactions"] = r.Compactions;
									dataRow["Nbr Exceptions"] = r.Exceptions;
									dataRow["Latency (ms)"] = r.Duration;
									if (r.UserRequest) dataRow["Requested"] = r.UserRequest;
									dataRow["Aborted"] = 1;
									dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(i => (i.Session == r.Session ? "X" : string.Empty) + i.Session));

									currentReadRepairs.Remove(r);
									dtStatusLog.Rows.Add(dataRow);

									r.Abort(item.Timestamp, item.Exception);
								});

								currentReadRepairs.ForEach(r =>
								{
									++r.Exceptions;
								});
							}
						}
						#endregion
					}

					#region Orphaned RRs
					currentReadRepairs.Where(r => !string.IsNullOrEmpty(r.Session) && r.Finish == DateTime.MinValue).ToArray().ForEach(r =>
					{
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
						dataRow["Nbr Exceptions"] = r.Exceptions;
						dataRow["Nbr Solr ReIdxs"] = r.SolrReIndexing == null ? 0 : r.SolrReIndexing.Count();
						dataRow["Aborted"] = r.Aborted ? 1 : 0;
						dataRow["Latency (ms)"] = r.Duration;
						dataRow["Session Path"] = string.Join("=>", currentReadRepairs.Select(i => (i.Session == r.Session ? "X" : string.Empty) + i.Session));

						dtStatusLog.Rows.Add(dataRow);

						r.Abort(oldestLogTimeStamp, "Orphaned");
					});
					currentReadRepairs.Clear();
					optionsReadRepair.Clear();
					#endregion

					if (readRepairs.Count > 0)
					{
						#region read repairs CFStats

						{
							Common.Patterns.Collections.ThreadSafe.List<CompactionLogInfo> nodeCompCollection = null;
							Common.Patterns.Collections.ThreadSafe.List<GCLogInfo> nodeGCCollection = null;
							Common.Patterns.Collections.ThreadSafe.List<SolrReIndexingLogInfo> nodeSolrIdxCollection = null;
							var dcIpAddress = (logGroupItem.DCName == null ? string.Empty : logGroupItem.DCName) + "|" + logGroupItem.IPAddress;

							CompactionOccurrences.TryGetValue(dcIpAddress, out nodeCompCollection);
							GCOccurrences.TryGetValue(dcIpAddress, out nodeGCCollection);
							SolrReindexingOccurrences.TryGetValue(dcIpAddress, out nodeSolrIdxCollection);

							//(new Common.File.FilePathAbsolute(string.Format(@"[DeskTop]\{0}.txt", dcIpAddress.Replace('|', '-'))))
							//	.WriteAllText(Newtonsoft.Json.JsonConvert.SerializeObject(nodeSolrIdxCollection, Newtonsoft.Json.Formatting.Indented));

							readRepairs.ForEach(rrInfo =>
								{
									rrInfo.CompactionList = nodeCompCollection?.UnSafe.Where(c => rrInfo.Keyspace == c.Keyspace
																									&& rrInfo.Start <= c.StartTime
																									&& c.StartTime < rrInfo.Finish);
									rrInfo.GCList = nodeGCCollection?.UnSafe.Where(c => rrInfo.Start <= c.StartTime
																							&& c.StartTime < rrInfo.Finish);
									rrInfo.SolrReIndexing = nodeSolrIdxCollection?.UnSafe.Where(c => rrInfo.Keyspace == c.Keyspace
																									&& rrInfo.Start <= c.Start
																									&& c.Start < rrInfo.Finish);
								});

							//(new Common.File.FilePathAbsolute(string.Format(@"[DeskTop]\RR-{0}.txt", dcIpAddress.Replace('|', '-'))))
							//	.WriteAllText(Newtonsoft.Json.JsonConvert.SerializeObject(readRepairs, Newtonsoft.Json.Formatting.Indented));
						}

						var dtCFStats = new DataTable(string.Format("CFStats-ReadRepair-{0}|{1}", logGroupItem.DCName, logGroupItem.IPAddress));

						if(dtCFStatsStack != null) dtCFStatsStack.Push(dtCFStats);
						initializeCFStatsDataTable(dtCFStats);

						Logger.Instance.InfoFormat("Adding Read Repairs ({2}) to CFStats for \"{0}\" \"{1}\"", logGroupItem.DCName, logGroupItem.IPAddress, readRepairs.Count);

						{
							var readrepairStats = from repairItem in readRepairs
												  where repairItem.Finish != DateTime.MinValue
														  && !repairItem.Aborted
												  group repairItem by new { repairItem.DataCenter, repairItem.IPAddress, repairItem.Keyspace }
														into g
												  select new
												  {
													  DCName = g.Key.DataCenter,
													  IpAdress = g.Key.IPAddress,
													  KeySpace = g.Key.Keyspace,
													  GrpInds = string.Join(", ", g.DuplicatesRemoved(i => i.GroupInd).Select(i => i.GroupInd)),
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
															 GrpInds = string.Join(", ", g.DuplicatesRemoved(i => i.GroupInd).Select(i => i.GroupInd)),
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
				}//);
			}//end of scope for logItems

			return globalReadRepairs.UnSafe;
		}

		public static void ReadRepairIntoDataTable(IEnumerable<ReadRepairLogInfo> readRepairs,
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
				dtReadRepair.Columns.Add("Session Path", typeof(string));
				dtReadRepair.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Node IPAddress", typeof(string));
				dtReadRepair.Columns.Add("Type", typeof(string)).AllowDBNull = true;
				dtReadRepair.Columns.Add("KeySpace", typeof(string)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Table", typeof(string)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Log/Completion Timestamp", typeof(DateTime)).AllowDBNull = true; //h

				//Read Repair
				dtReadRepair.Columns.Add("Session", typeof(string)); //i
				dtReadRepair.Columns.Add("Session Duration", typeof(TimeSpan)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Token Range Start", typeof(string)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Token Range End", typeof(string)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Nbr of Repairs", typeof(int)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Receiving Nodes", typeof(string)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Nbr GC Events", typeof(int)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Nbr Compaction Events", typeof(int)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Nbr Solr ReIdx Events", typeof(int)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Nbr Exceptions", typeof(int)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Options", typeof(string)).AllowDBNull = true; //s
				dtReadRepair.Columns.Add("Requested", typeof(int)).AllowDBNull = true; //t
				dtReadRepair.Columns.Add("Aborted Read Repair", typeof(int)).AllowDBNull = true; //u

				//GC
				dtReadRepair.Columns.Add("GC Time (ms)", typeof(long)).AllowDBNull = true; //v
				dtReadRepair.Columns.Add("Eden Changed (mb)", typeof(decimal)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Survivor Changed (mb)", typeof(decimal)).AllowDBNull = true;
				dtReadRepair.Columns.Add("Old Changed (mb)", typeof(decimal)).AllowDBNull = true; //y

				//Compaction
				dtReadRepair.Columns.Add("SSTables", typeof(int)).AllowDBNull = true; //z
				dtReadRepair.Columns.Add("Old Size (mb)", typeof(decimal)).AllowDBNull = true;//aa
				dtReadRepair.Columns.Add("New Size (mb)", typeof(long)).AllowDBNull = true; //ab
				dtReadRepair.Columns.Add("Compaction Time (ms)", typeof(int)).AllowDBNull = true; //ac
				dtReadRepair.Columns.Add("Compaction IORate (mb/sec)", typeof(decimal)).AllowDBNull = true; //ad

				//Solr Reindexing
				dtReadRepair.Columns.Add("Solr ReIdx Duration", typeof(int)).AllowDBNull = true; //ae

				dtReadRepair.Columns.Add("Reconciliation Reference", typeof(long)).AllowDBNull = true; //af
			}
		#endregion

			if (readRepairs.Count() > 0)
			{
				var readRepairItems = from rrItem in readRepairs
									  orderby rrItem.Start ascending, rrItem.SessionPath ascending, rrItem.DataCenter, rrItem.IPAddress
									  select rrItem;

				foreach (var rrItem in readRepairItems)
				{
					var newDataRow = dtReadRepair.NewRow();


					newDataRow["Start Timestamp"] = rrItem.Start;
					newDataRow["Session Path"] = rrItem.SessionPath;
					newDataRow["Data Center"] = rrItem.DataCenter;
					newDataRow["Node IPAddress"] = rrItem.IPAddress;
					newDataRow["Reconciliation Reference"] = rrItem.GroupInd;

					//Read Repair
					newDataRow["Type"] = "Read Repair";
					newDataRow["KeySpace"] = rrItem.Keyspace;
					newDataRow["Log/Completion Timestamp"] = rrItem.Finish;
					newDataRow["Session"] = rrItem.Session;
					newDataRow["Session Duration"] = new TimeSpan(0, 0, 0, 0, rrItem.Duration);
					newDataRow["Token Range Start"] = rrItem.TokenRangeStart;
					newDataRow["Token Range End"] = rrItem.TokenRangeEnd;
					newDataRow["Nbr of Repairs"] = rrItem.NbrRepairs;
					newDataRow["Receiving Nodes"] = string.Join(", ", rrItem.ReceivingNodes);
					newDataRow["Nbr GC Events"] = rrItem.GCs;
					newDataRow["Nbr Compaction Events"] = rrItem.Compactions;
					newDataRow["Nbr Solr ReIdx Events"] = rrItem.SolrReIndexing == null ? 0 : rrItem.SolrReIndexing.Count();
					newDataRow["Nbr Exceptions"] = rrItem.Exceptions;
					newDataRow["Aborted Read Repair"] = rrItem.Aborted ? 1 : 0;
					newDataRow["Options"] = rrItem.Options;
					newDataRow["Requested"] = rrItem.UserRequest ? 1 : 0;

					dtReadRepair.Rows.Add(newDataRow);

					if (rrItem.GCList != null)
					{
						foreach (var item in (from gc in rrItem.GCList
											  let startTime = gc.LogTimestamp.Subtract(new TimeSpan(0, 0, 0, 0, gc.GCLatency))
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
							newDataRow["GC Time (ms)"] = item.GC.GCLatency;
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
											  let startTime = comp.LogTimestamp.Subtract(new TimeSpan(0, 0, 0, 0, comp.Latency))
											  orderby startTime ascending
											  select new { StartTime = startTime, Comp = comp }))
						{
							newDataRow = dtReadRepair.NewRow();

							newDataRow["Start Timestamp"] = item.StartTime;
							newDataRow["Session Path"] = rrItem.SessionPath;
							newDataRow["Data Center"] = item.Comp.DataCenter;
							newDataRow["Node IPAddress"] = item.Comp.IPAddress;
							newDataRow["Type"] = "Compaction";
							newDataRow["Log/Completion Timestamp"] = item.Comp.LogTimestamp;
							newDataRow["Session"] = rrItem.Session;
							newDataRow["KeySpace"] = item.Comp.Keyspace;
							newDataRow["Table"] = item.Comp.Table;
							newDataRow["SSTables"] = item.Comp.SSTables;
							newDataRow["Old Size (mb)"] = item.Comp.OldSize;
							newDataRow["New Size (mb)"] = item.Comp.NewSize;
							newDataRow["Compaction Time (ms)"] = item.Comp.Latency;
							newDataRow["Compaction IORate (mb/sec)"] = item.Comp.IORate;
							newDataRow["Reconciliation Reference"] = item.Comp.GroupIndicator;

							dtReadRepair.Rows.Add(newDataRow);
						}
					}

					if (rrItem.SolrReIndexing != null)
					{
						foreach (var item in (from solrIdx in rrItem.SolrReIndexing
											  orderby solrIdx.Start ascending
											  select solrIdx))
						{
							newDataRow = dtReadRepair.NewRow();

							newDataRow["Start Timestamp"] = item.Start;
							newDataRow["Session Path"] = rrItem.SessionPath;
							newDataRow["Data Center"] = item.DataCenter;
							newDataRow["Node IPAddress"] = item.IPAddress;
							newDataRow["Type"] = "Solr ReIndex";
							newDataRow["Log/Completion Timestamp"] = item.Finish;
							newDataRow["Session"] = rrItem.Session;
							newDataRow["KeySpace"] = item.Keyspace;
							newDataRow["Table"] = item.Table;
							newDataRow["Solr ReIdx Duration"] = item.Duration;
							newDataRow["Reconciliation Reference"] = item.GroupIndicator;

							dtReadRepair.Rows.Add(newDataRow);
						}
					}
				}
			}
		}


		public static void ReleaseGlobalLogCollections()
		{
			GCOccurrences.Clear();
			CompactionOccurrences.Clear();
			SolrReindexingOccurrences.Clear();
		}
	}
}
