using System;
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
                                                        int maxNbrLinesRead,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogsStack,
                                                        IFilePath archiveFilePath, //null disables archive parsing
                                                        bool parseNonLogs,
                                                        string excelWorkSheetStatusLogCassandra,
                                                        Common.Patterns.Collections.ThreadSafe.Dictionary<string, string> nodeGCInfo,
                                                        List<string> ignoreKeySpaces,
                                                        List<CKeySpaceTableNames> kstblNames,                                                        
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtTPStatsStack,
                                                        int gcPausedFlagThresholdInMS,
                                                        int compactionFllagThresholdInMS,
                                                        int slowLogQueryThresholdInMS)
        {
            DateTime maxLogTimestamp = DateTime.MinValue;
            var dtLog = new System.Data.DataTable(excelWorkSheetLogCassandra + "-" + ipAddress);
            Task statusTask = Task.FromResult<object>(null);
            Task<int> archTask = Task.FromResult(0);

            var logTask = Task.Factory.StartNew(() =>
                                {
                                    Logger.Instance.InfoFormat("Processing File \"{0}\"", logFilePath.Path);
                                    Program.ConsoleLogReadFiles.Increment(string.Format("{0} - {1}", ipAddress, logFilePath.FileName));

                                    dtLogsStack.Push(dtLog);
                                    var linesRead = ReadCassandraLogParseIntoDataTable(logFilePath,
                                                                                        ipAddress,
                                                                                        dcName,
                                                                                        includeLogEntriesAfterThisTimeFrame,
                                                                                        maxNbrLinesRead,
                                                                                        dtLog,
                                                                                        out maxLogTimestamp,
                                                                                        gcPausedFlagThresholdInMS,
                                                                                        compactionFllagThresholdInMS,
                                                                                        slowLogQueryThresholdInMS);

                                    lock (maxminMaxLogDate)
                                    {
                                        maxminMaxLogDate.SetMinMax(maxLogTimestamp);
                                    }

                                    Program.ConsoleLogReadFiles.TaskEnd(string.Format("{0} - {1}", ipAddress, logFilePath.FileName));

                                    return linesRead;
                                },
                                TaskCreationOptions.LongRunning);
            
            statusTask = logTask.ContinueWith(taskResult =>
                            {
                                var dtStatusLog = new System.Data.DataTable(excelWorkSheetStatusLogCassandra + "-" + ipAddress);
                                var dtCFStats = parseNonLogs ? new DataTable("CFStats-Comp" + "-" + ipAddress) : null;
                                var dtTPStats = parseNonLogs ? new DataTable("CFStats-GC" + "-" + ipAddress) : null;

                                dtLogStatusStack.Push(dtStatusLog);
                                dtCFStatsStack.Push(dtCFStats);
                                dtTPStatsStack.Push(dtTPStats);

                                Logger.Instance.InfoFormat("Status Log Processing File \"{0}\"", logFilePath.Path);
                                Program.ConsoleParsingLog.Increment(string.Format("Status {0}", dtLog.TableName));
                                Program.ConsoleParsingNonLog.Increment(string.Format("Status {0}", dtLog.TableName));

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
                                Program.ConsoleParsingNonLog.TaskEnd(string.Format("Status {0}", dtLog.TableName));
                            },
                            TaskContinuationOptions.AttachedToParent
                                | TaskContinuationOptions.LongRunning
                                | TaskContinuationOptions.OnlyOnRanToCompletion);


            if (maxNbrLinesRead <= 0
                        && archiveFilePath != null)
            {
                foreach (IFilePath archiveElement in archiveFilePath.GetWildCardMatches())
                {
                    if (archiveElement.PathResolved != logFilePath.PathResolved)
                    {
                        archTask = ProcessLogFileTasks(archiveElement,
                                                            excelWorkSheetLogCassandra,
                                                            dcName,
                                                            ipAddress,
                                                            includeLogEntriesAfterThisTimeFrame,
                                                            maxminMaxLogDate,
                                                            -1,
                                                            dtLogsStack,
                                                            null,
                                                            parseNonLogs,
                                                            excelWorkSheetStatusLogCassandra,
                                                            nodeGCInfo,
                                                            ignoreKeySpaces,
                                                            kstblNames,                                                            
                                                            dtLogStatusStack,
                                                            dtCFStatsStack,
                                                            dtTPStatsStack,
                                                            gcPausedFlagThresholdInMS,
                                                            compactionFllagThresholdInMS,
                                                            slowLogQueryThresholdInMS);
                    }
                }
            }


            return Task<int>
                    .Factory
                    .ContinueWhenAll(new Task[] { logTask, statusTask, archTask }, tasks => logTask.Result + archTask.Result);
        }

        public static Task<DataTable> ParseCassandraLogIntoSummaryDataTable(Task<DataTable> logTask,
                                                                    string excelWorkSheetLogCassandra,                                                                    
                                                                    DateTimeRange maxminLogDate,
                                                                    Tuple<DateTime, TimeSpan>[] logSummaryPeriods,
                                                                    Tuple<TimeSpan, TimeSpan>[] logSummaryPeriodRanges,
                                                                    string[] logSummaryIndicatorType,
                                                                    string[] logSummaryTaskItems,
                                                                    string[] logSummaryIgnoreTaskExceptions)
        {
            Task<DataTable> summaryTask = Task.FromResult<DataTable>(null);

            if ((logSummaryPeriods != null && logSummaryPeriods.Length > 0)
                            || (logSummaryPeriodRanges != null && logSummaryPeriodRanges.Length > 0))
            {
                summaryTask = logTask.ContinueWith(taskResult =>
                                {
                                    DataTable dtLog = taskResult.Result;
                                    DataTable dtSummaryLog = new DataTable(dtLog.TableName + "Summary");

                                    Tuple <DateTime,TimeSpan>[] summaryPeriods = logSummaryPeriods;

                                    if (logSummaryPeriods == null || logSummaryPeriods.Length == 0)
                                    {
                                        var summaryPeriodList = new List<Tuple<DateTime, TimeSpan>>();

                                        var currentRange = maxminLogDate.Max.RoundUp(logSummaryPeriodRanges.First().Item2);

                                        for (int nIndex = 0; nIndex < logSummaryPeriodRanges.Length; ++nIndex)
                                        {
                                            summaryPeriodList.Add(new Tuple<DateTime, TimeSpan>(currentRange,
                                                                                                    logSummaryPeriodRanges[nIndex].Item2));

                                            currentRange = currentRange - logSummaryPeriodRanges[nIndex].Item1;
                                        }

                                        summaryPeriods = summaryPeriodList.ToArray();
                                    }

                                    Program.ConsoleParsingLog.Increment(string.Format("Summary {0}", dtLog.TableName));

                                    ParseCassandraLogIntoSummaryDataTable(dtLog,
                                                                            dtSummaryLog,                                                                            
                                                                            logSummaryIndicatorType,
                                                                            logSummaryTaskItems,
                                                                            logSummaryIgnoreTaskExceptions,
                                                                            summaryPeriods);

                                    Program.ConsoleParsingLog.TaskEnd(string.Format("Summary {0}", dtLog.TableName));

                                    return dtSummaryLog;
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
        static int ReadCassandraLogParseIntoDataTable(IFilePath clogFilePath,
                                                string ipAddress,
                                                string dcName,
                                                DateTime onlyEntriesAfterThisTimeFrame,
                                                int maxRowWrite,
                                                System.Data.DataTable dtCLog,
                                                out DateTime maxTimestamp,
                                                int gcPausedFlagThresholdInMS,
                                                int compactionFllagThresholdInMS,
                                                int slowLogQueryThresholdInMS)
        {
            if (dtCLog.Columns.Count == 0)
            {
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
                dtCLog.Columns.Add("Flagged", typeof(bool)).AllowDBNull = true;
            }

            var fileLines = clogFilePath.ReadAllLines();
            string line;
            List<string> parsedValues;
            DataRow dataRow;
            DataRow lastRow = null;
            DateTime lineDateTime;
            var minmaxDate = new Common.DateTimeRange();
            string lineIPAddress;
            int skipLines = -1;
            string tableItem = null;
            int tableItemPos = -1;
            int nbrRows = 0;
            DateTimeRange ignoredTimeRange = new DateTimeRange();
            bool exceptionOccurred = false;
            //int tableItemValuePos = -1;

            maxTimestamp = DateTime.MinValue;

            for (int nLine = 0; nLine < fileLines.Length; ++nLine)
            {
                line = fileLines[nLine].Trim();

                if (string.IsNullOrEmpty(line)
                        || line.Length < 3)
                {
                    continue;
                }

                if (line.Substring(0, 3).ToLower() == "at "
                        || line.Substring(0, 4).ToLower() == "... ")
                {                    
                    continue;
                }

                parsedValues = Common.StringFunctions.Split(line,
                                                            ' ',
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

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
                //INFO  [CqlSlowLog-Writer-thread-0] 2016-08-16 17:11:16,429  CqlSlowLogWriter.java:151 - Recording statements with duration of 60001 in slow log
                //ERROR [SharedPool-Worker-15] 2016-08-16 17:11:16,831  SolrException.java:150 - org.apache.solr.common.SolrException: No response after timeout: 60000
                //WARN  [CqlSlowLog-Writer-thread-0] 2016-08-17 00:21:05,698  CqlSlowLogWriter.java:245 - Error writing to cql slow log
                //org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level ONE
                //java.lang.AssertionError: id=3114 length=3040 docID=2090 maxDoc=3040
                //  at org.apache.lucene.index.RTSortedDocValues.getOrd(RTSortedDocValues.java:162) ~[solr - uber - with - auth_2.0 - 4.10.3.0.101.jar:na]       
                //Caused by: org.apache.solr.search.SyntaxError: Cannot parse '(((other_id:() AND other_id_type:(PASSPORT)))^1.0 OR phone:(5126148266 OR 5126148266)^1.0 OR ((street:(CHURCH) AND street:(6835)))^1.0)': Encountered " ")" ") "" at line 1, column 13.         
                //ERROR [SharedPool-Worker-1] 2016-09-28 19:18:25,277  CqlSolrQueryExecutor.java:375 - No response after timeout: 60000
                //org.apache.solr.common.SolrException: No response after timeout: 60000
                //java.lang.RuntimeException: org.apache.cassandra.exceptions.UnavailableException: Cannot achieve consistency level LOCAL_ONE
                //ERROR [SharedPool-Worker-3] 2016-10-01 19:20:14,415  Message.java:538 - Unexpected exception during request; channel = [id: 0xc224c650, /10.16.9.33:49634 => /10.12.50.27:9042]
                //ERROR [MessagingService-Incoming-/10.12.49.27] 2016-09-28 18:53:54,898  JVMStabilityInspector.java:106 - JVM state determined to be unstable.  Exiting forcefully due to:
                //java.lang.OutOfMemoryError: Java heap space
                //WARN  [commitScheduler-4-thread-1] 2016-09-28 18:53:32,436  WorkPool.java:413 - Timeout while waiting for workers when flushing pool Index; current timeout is 300000 millis, consider increasing it, or reducing load on the node.
                //Failure to flush may cause excessive growth of Cassandra commit log.

                #region Exception Log Info Parsing
                if (parsedValues[0].Contains("OutOfMemoryError"))
                {
                    #region OutOfMemoryError

                    if (lastRow != null)
                    {
                        lastRow.BeginEdit();

                        lastRow["Exception"] = parsedValues[0][parsedValues[0].Length - 1] == ':'
                                                ? parsedValues[0].Substring(0, parsedValues[0].Length - 1)
                                                : parsedValues[0];
                        lastRow["Exception Description"] = line;
                        lastRow["Flagged"] = true;
                        lastRow["Associated Item"] = line.Substring(parsedValues[0].Length);

                        lastRow.EndEdit();
                        lastRow.AcceptChanges();
                    }
                    exceptionOccurred = true;
                    continue;

                    #endregion
                }
                else if (parsedValues[0].ToLower().Contains("exception")
                            || parsedValues[0].ToLower().Contains("assertionerror"))
                {
                    #region Exception

                    if (lastRow != null)
                    {
                        lastRow.BeginEdit();

                        lastRow["Exception"] = parsedValues[0][parsedValues[0].Length - 1] == ':'
                                                ? parsedValues[0].Substring(0, parsedValues[0].Length - 1)
                                                : parsedValues[0];
                        lastRow["Exception Description"] = line;
                        lastRow["Flagged"] = true;

                        if((string)lastRow["Exception"] == "io.netty.handler.ssl.NotSslRecordException")
                        {
                            var lstDescr = lastRow["Description"] as string;

                            if(!string.IsNullOrEmpty(lstDescr))
                            {
                                //INFO[SharedPool - Worker - 1] 2016 - 09 - 24 16:33:58,099  Message.java:532 - Unexpected exception during request; channel = [id: 0xa6a28fb0, / 10.14.50.24:44796 => / 10.14.50.24:9042]
                                //io.netty.handler.ssl.NotSslRecordException: not an SSL / TLS record: 0300000001000000160001000b43514c5f56455253494f4e0005332e302e30
                                var pos = lstDescr.IndexOf("channel");

                                if(pos > 0)
                                {
                                    var channelValue = lstDescr.Substring(pos + 10).Trim();
                                    var cpos = channelValue.IndexOf('/');

                                    lastRow["Associated Item"] = cpos > 0 ? "[" + channelValue.Substring(cpos) : channelValue;
                                }
                            }

                        }
                        else if (lastRow["Associated Value"] == DBNull.Value)
                        {
                            var clMatch = RegExCLogCL.Match(line);
                            var toMatch = RegExCLogTO.Match(line);

                            if (toMatch.Success)
                            {
                                lastRow["Associated Value"] = int.Parse(toMatch.Groups[1].Value);
                                lastRow["Associated Item"] = "No response|Operation timeout";
                            }
                            else if (clMatch.Success)
                            {
                                lastRow["Associated Value"] = clMatch.Groups[1].Value;
                                lastRow["Associated Item"] = "Cannot achieve consistency level";
                            }
                            else
                            {
                                foreach (var element in parsedValues)
                                {
                                    if (element[0] == '(')
                                    {
                                        if (LookForIPAddress(element.Substring(1, element.Length - 2).Trim(), ipAddress, out lineIPAddress))
                                        {
                                            lastRow["Associated Value"] = lineIPAddress;
                                            break;
                                        }
                                    }
                                    else if (element[0] == '/')
                                    {
                                        if (LookForIPAddress(element, ipAddress, out lineIPAddress))
                                        {
                                            lastRow["Associated Value"] = lineIPAddress;
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        lastRow.EndEdit();
                        lastRow.AcceptChanges();
                    }
                    exceptionOccurred = true;
                    continue;

                    #endregion
                }
                else if (parsedValues[0].ToLower() == "caused")
                {
                    #region caused

                    if (lastRow != null)
                    {
                        lastRow.BeginEdit();

                        lastRow["Exception"] = parsedValues[2][parsedValues[2].Length - 1] == ':'
                                                ? parsedValues[2].Substring(0, parsedValues[2].Length - 1)
                                                : parsedValues[2];
                        lastRow["Exception Description"] = line;
                        lastRow["Flagged"] = true;

                        if (lastRow["Associated Value"] == DBNull.Value)
                        {
                            if (parsedValues[2].EndsWith("SyntaxError:"))
                            {
                                if(parsedValues[3].ToLower() == "cannot" && parsedValues[4].ToLower() == "parse")
                                {
                                    lastRow["Associated Value"] = parsedValues[5];
                                }
                            }
                            else if (parsedValues[2].EndsWith("ParseException:"))
                            {
                                if (parsedValues[3].ToLower() == "Encountered")
                                {
                                    var lastPos = parsedValues.LastIndexOf("line");

                                    lastRow["Associated Value"] = string.Join(" ", parsedValues.GetRange(4, lastPos - 6));                                                                                
                                }
                            }
                            else
                            {
                                foreach (var element in parsedValues)
                                {
                                    if (element[0] == '(')
                                    {
                                        if (LookForIPAddress(element.Substring(1, element.Length - 2).Trim(), ipAddress, out lineIPAddress))
                                        {
                                            lastRow["Associated Value"] = lineIPAddress;
                                            break;
                                        }
                                    }
                                    else if (element[0] == '/')
                                    {
                                        if (LookForIPAddress(element, ipAddress, out lineIPAddress))
                                        {
                                            lastRow["Associated Value"] = lineIPAddress;
                                            break;
                                        }
                                    }
                                }
                            }
                        }

                        lastRow.EndEdit();
                        lastRow.AcceptChanges();
                    }
                    exceptionOccurred = true;
                    continue;

                    #endregion
                }
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
                        continue;
                    }

                    if (maxRowWrite > 0 && maxRowWrite < int.MaxValue)
                    {
                        if (skipLines < 0)
                        {
                            skipLines = fileLines.Length - nLine - maxRowWrite;
                        }

                        if (--skipLines > 0)
                        {
                            continue;
                        }
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

                if(!ignoredTimeRange.IsEmpty())
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

                if (parsedValues[startRange][0] == '(')
                {
                    ++startRange;
                }

                for (int nCell = startRange; nCell < parsedValues.Count; ++nCell)
                {
                    if (parsedValues[4] == "SolrException.java")
                    {
                        #region SolrException.java
                        //ERROR [SharedPool-Worker-15] 2016-08-16 17:11:16,831  SolrException.java:150 - org.apache.solr.common.SolrException: No response after timeout: 60000
                        //ERROR [SharedPool-Worker-2] 2016-09-30 09:07:53,224  SolrException.java:150 - org.apache.solr.common.SolrException: org.apache.solr.search.SyntaxError: Cannot parse 'something': Encountered " ")" ") "" at line 1, column 13.
                        if (nCell > 4)
                        {
                            if (dataRow["Exception"] == DBNull.Value && parsedValues[nCell].ToLower().Contains("exception"))
                            {
                                dataRow["Flagged"] = true;

                                var startExceptionPos = line.IndexOf(parsedValues[nCell]);
                                var exceptionInfo = Common.StringFunctions.Split(line.Substring(startExceptionPos),
                                                                                    ':',
                                                                                    Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                    Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                                if (exceptionInfo.Count == 1)
                                {
                                    dataRow["Exception"] = "SolrException(" + parsedValues[nCell] + ")";
                                }
                                else
                                {
                                    if (exceptionInfo[0] == "org.apache.solr.common.SolrException")
                                    {
                                        dataRow["Exception"] = exceptionInfo[0] + " (" + exceptionInfo[1] + ")";
                                    }
                                    else
                                    {
                                        dataRow["Exception"] = exceptionInfo[0];
                                    }

                                    dataRow["Exception Description"] = string.Join(": ", exceptionInfo.Skip(1));
                                }
                            }
                        }
                        exceptionOccurred = true;
                        #endregion
                    }
                    else if (parsedValues[4] == "Message.java")
                    {
                        #region Message.java
                        //ERROR [SharedPool-Worker-3] 2016-10-01 19:20:14,415  Message.java:538 - Unexpected exception during request; channel = [id: 0xc224c650, /10.16.9.33:49634 => /10.12.50.27:9042]

                        if (nCell == itemValuePos)
                        {
                            if(parsedValues[nCell] == "channel")
                            {
                                var pos = parsedValues[nCell + 2].IndexOf('/');

                                dataRow["Associated Item"] = pos > 0 ? "[" + parsedValues[nCell + 2].Substring(pos) : parsedValues[nCell + 2];
                            }
                        }                        
                        else if (parsedValues[0] == "ERROR" && parsedValues[nCell] == "Unexpected" && parsedValues[nCell + 1] == "exception")
                        {
                            itemValuePos = nCell + 4;
                            dataRow["Exception"] = "Unexpected";
                            dataRow["Exception Description"] = "Unexpected exception";
                            dataRow["Flagged"] = true;
                        }
                        #endregion
                    }
                    else if (parsedValues[nCell].ToLower().Contains("exception"))
                    {
                        #region exception
                        var exceptionLine = fileLines[nLine + 1].Trim();
                        var exceptionEndPos = exceptionLine.IndexOf(' ');
                        var exceptionClass = exceptionEndPos > 0 ? exceptionLine.Substring(0, exceptionEndPos) : null;

                        if (!string.IsNullOrEmpty(exceptionClass) && exceptionClass[exceptionClass.Length - 1] == ':')
                        {
                            dataRow["Exception"] = exceptionClass.Substring(0, exceptionClass.Length - 1);
                            dataRow["Exception Description"] = exceptionLine.Substring(exceptionEndPos + 1).TrimStart();
                            dataRow["Flagged"] = true;
                            ++nLine;
                        }
                        exceptionOccurred = true;
                        #endregion
                    }
                    else if (parsedValues[4] == "CqlSolrQueryExecutor.java")
                    {
                        #region CqlSolrQueryExecutor.java

                        if (itemPos == nCell)
                        {
                            dataRow["Associated Item"] = parsedValues[nCell];
                        }
                        if (nCell >= itemPos && parsedValues[nCell][parsedValues[nCell].Length - 1] == ')')
                        {
                            var firstParan = parsedValues[nCell].IndexOf('(');

                            if (firstParan >= 0)
                            {
                                dataRow["Associated Value"] = ConvertInToMB(parsedValues[nCell].Substring(firstParan + 1, parsedValues[nCell].Length - firstParan - 2));
                            }
                        }

                        if (parsedValues[nCell].EndsWith("SyntaxError:"))
                        {
                            //ERROR [SharedPool-Worker-2] 2016-09-27 12:27:29,012  CqlSolrQueryExecutor.java:375 - org.apache.solr.search.SyntaxError: Cannot parse 'syntax' at line 1, column 13.

                            itemPos = nCell + 3;
                            dataRow["Flagged"] = true;
                            dataRow["Exception"] = parsedValues[nCell][parsedValues[nCell].Length - 1] == ':'
                                                        ? parsedValues[nCell].Substring(0, parsedValues[nCell].Length - 1)
                                                        : parsedValues[nCell];
                            exceptionOccurred = true;
                        }
                        #endregion
                    }
                    else if (parsedValues[4] == "CompactionController.java")
                    {
                        #region CompactionController.java
                        //Compacting large row billing/account_payables:20160726:FMCC (348583137 bytes)

                        if (itemPos == nCell)
                        {
                            var ksTableName = parsedValues[nCell];
                            var keyDelimatorPos = ksTableName.IndexOf(':');

                            if (keyDelimatorPos > 0)
                            {
                                ksTableName = ksTableName.Substring(0, keyDelimatorPos).Replace('/', '.');

                                var splitItems = SplitTableName(ksTableName, null);

                                ksTableName = splitItems.Item1 + '.' + splitItems.Item2;
                            }

                            dataRow["Associated Item"] = ksTableName;

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
                            dataRow["Flagged"] = true;
                            dataRow["Exception"] = "Compacting large row";
                        }
                        #endregion
                    }
                    else if (parsedValues[4] == "SSTableWriter.java")
                    {
                        #region SSTableWriter.java
                        //WARN  [CompactionExecutor:6] 2016-06-07 06:57:44,146  SSTableWriter.java:240 - Compacting large partition kinesis_events/event_messages:49c023da-0bb8-46ce-9845-111514b43a63 (186949948 bytes)

                        if (itemPos == nCell)
                        {
                            var ksTableName = parsedValues[nCell];
                            var keyDelimatorPos = ksTableName.IndexOf(':');

                            if (keyDelimatorPos > 0)
                            {
                                ksTableName = ksTableName.Substring(0, keyDelimatorPos).Replace('/', '.');

                                var splitItems = SplitTableName(ksTableName, null);

                                ksTableName = splitItems.Item1 + '.' + splitItems.Item2;
                            }

                            dataRow["Associated Item"] = ksTableName;

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
                            dataRow["Flagged"] = true;
                            dataRow["Exception"] = "Compacting large partition";
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
                                dataRow["Flagged"] = true;
                                dataRow["Exception"] = "GC Threshold";
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
                            dataRow["Flagged"] = true;
                            dataRow["Exception"] = "Heap Full";
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

                            dataRow["Flagged"] = true;
                            dataRow["Exception"] = "Pause (FailureDetector)";
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
                        if (nCell == itemPos)
                        {
                            var ksTableName = parsedValues[nCell];

                            if (ksTableName[0] == '[')
                            {
                                ksTableName = ksTableName.Substring(1, ksTableName.Length - 2);

                                var splitItems = SplitTableName(ksTableName, null);

                                ksTableName = splitItems.Item1 + '.' + splitItems.Item2;
                            }

                            dataRow["Associated Item"] = ksTableName;
                        }
                        if (nCell == itemValuePos)
                        {
                            int batchSize;

                            if (int.TryParse(parsedValues[nCell], out batchSize))
                            {
                                dataRow["Associated Value"] = batchSize;
                            }
                        }
                        if (parsedValues[nCell] == "Batch")
                        {
                            itemPos = nCell + 5;
                            itemValuePos = nCell + 9;
                            dataRow["Flagged"] = true;
                            dataRow["Exception"] = "Batch Size Exceeded";
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
                            var splitItems = SplitTableName(parsedValues[nCell], null);
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
                            dataRow["Flagged"] = true;
                        }
                        else if (parsedValues[0] == "ERROR" && parsedValues[nCell] == "Scanned" && parsedValues[nCell + 1] == "over")
                        {
                            itemPos = nCell + 5;
                            itemValuePos = 2;
                            dataRow["Flagged"] = true;
                            dataRow["Exception"] = "Query Tombstones Aborted";
                        }
                        #endregion
                    }
                    else if (parsedValues[4] == "HintedHandoffMetrics.java")
                    {
                        #region HintedHandoffMetrics.java
                        //		WARN  [HintedHandoffManager:1] 2016-07-25 04:26:10,445  HintedHandoffMetrics.java:79 - /10.170.110.191 has 1711 dropped hints, because node is down past configured hint window.				
                        if (parsedValues[nCell] == "dropped")
                        {
                            //dataRow["Associated Item"] = "Dropped Hints";
                            dataRow["Exception"] = "Dropped Hints";

                            if (LookForIPAddress(parsedValues[nCell - 3], ipAddress, out lineIPAddress))
                            {
                                dataRow["Associated Value"] = lineIPAddress;
                            }
                        }
                        #endregion
                    }
                    else if (parsedValues[4] == "StorageService.java")
                    {
                        #region StorageService.java
                        //	WARN [ScheduledTasks:1] 2013-04-10 10:18:12,042 StorageService.java (line 2645) Flushing CFS(Keyspace='Company', ColumnFamily='01_Meta') to relieve memory pressure
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

                            dataRow["Associated Value"] = ksName + "." + tblName;
                        }
                        if (parsedValues[0] == "WARN" && parsedValues[nCell] == "Flushing")
                        {
                            //dataRow["Associated Item"] = "Flushing CFS";
                            dataRow["Exception"] = "CFS Flush";
                            itemValuePos = nCell + 1;
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
                        }
                        #endregion
                    }
                    else if (parsedValues[4] == "MessagingService.java")
                    {
                        #region MessagingService.java
                        //MessagingService.java --  MUTATION messages were dropped in last 5000 ms: 43 for internal timeout and 0 for cross node timeout
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
                            itemPos = nCell + 8;
                        }
                        #endregion
                    }
                    else if (parsedValues[4] == "CompactionTask.java")
                    {
                        #region CompactionTask.java
                        //INFO  [CompactionExecutor:4657] 2016-06-12 06:26:25,534  CompactionTask.java:274 - Compacted 4 sstables to [/data/system/size_estimates-618f817b005f3678b8a453f3930b8e86/system-size_estimates-ka-11348,]. 2,270,620 bytes to 566,478 (~24% of original) in 342ms = 1.579636MB/s. 40 total partitions merged to 10. Partition merge counts were {4:10, }

                        if (nCell == itemValuePos)
                        {
                            var time = DetermineTime(parsedValues[nCell]);

                            if (time is int && (int)time >= compactionFllagThresholdInMS)
                            {
                                dataRow["Flagged"] = true;
                                //dataRow["Associated Item"] = "Compaction Pause";
                                dataRow["Exception"] = "Compaction Latency Warning";
                            }
                            dataRow["Associated Value"] = time;
                        }
                        else if (parsedValues[nCell] == "Compacted")
                        {
                            itemValuePos = nCell + 11;
                            dataRow["Associated Item"] = "Compaction";
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
                                dataRow["Flagged"] = true;
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
                                dataRow["Flagged"] = true;
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
                            dataRow["Flagged"] = true;
                        }
                        #endregion
                    }
                    else if (parsedValues[4] == "CqlSolrQueryExecutor.java")
                    {
                        #region CqlSolrQueryExecutor.java
                        //ERROR [SharedPool-Worker-1] 2016-08-29 16:28:03,882  CqlSolrQueryExecutor.java:409 - No response after timeout: 60000
                        if (parsedValues[nCell] == "No" && parsedValues[nCell + 1] == "response" && parsedValues[nCell + 3] == "timeout")
                        {
                            dataRow["Flagged"] = true;
                            dataRow["Associated Item"] = "Solr Timeout";
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
                            dataRow["Flagged"] = true;
                            dataRow["Associated Item"] = string.Join(" ", parsedValues.Skip(nCell));

                            var splitItems = SplitTableName(RemoveQuotes(parsedValues[nCell - 1]), null);
                            var ksTableName = splitItems.Item1 + '.' + splitItems.Item2;

                            dataRow["Associated Value"] = ksTableName;
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
                            dataRow["Flagged"] = true;
                            dataRow["Associated Item"] = string.Join(" ", parsedValues.Skip(nCell + 5));                            
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
                            var nxtLine = fileLines[nLine + 1].Trim();
                            if (nxtLine.StartsWith("Failure") && nxtLine.Contains("commit log"))
                            {
                                dataRow["Flagged"] = true;
                                dataRow["Associated Item"] = nxtLine;
                                dataRow["Exception"] = "CommitLogFlushFailure";
                                ++nLine;
                            }
                        }
                        #endregion
                    }
                    else if (LookForIPAddress(parsedValues[nCell], ipAddress, out lineIPAddress))
                    {
                        dataRow["Associated Value"] = lineIPAddress;
                    }


                    logDesc.Append(' ');
                    logDesc.Append(parsedValues[nCell]);
                }

                dataRow["Description"] = logDesc;

                #endregion

                dtCLog.Rows.Add(dataRow);
                ++nbrRows;
                lastRow = dataRow;

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

        static void ParseCassandraLogIntoSummaryDataTable(DataTable dtroCLog,
                                                            DataTable dtCSummaryLog,                                                            
                                                            string[] logAggregateIndicators,
                                                            string[] logAggregateAdditionalTaskExceptionItems,
                                                            string[] ignoreTaskExceptionItems,
                                                            IEnumerable<Tuple<DateTime, TimeSpan>> bucketFromAggregatePeriods)
        {
            if (dtCSummaryLog.Columns.Count == 0)
            {
                dtCSummaryLog.Columns.Add("Timestamp Period", typeof(DateTime));
                dtCSummaryLog.Columns.Add("Aggregation Period", typeof(TimeSpan));
                dtCSummaryLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCSummaryLog.Columns.Add("Node IPAddress", typeof(string)).AllowDBNull = true;
                dtCSummaryLog.Columns.Add("Type", typeof(string)).AllowDBNull = true;
                dtCSummaryLog.Columns.Add("Value", typeof(string)).AllowDBNull = true;
                dtCSummaryLog.Columns.Add("Associated Item", typeof(string)).AllowDBNull = true;
                dtCSummaryLog.Columns.Add("Occurrences", typeof(int));

                dtCSummaryLog.DefaultView.Sort = "[Timestamp Period] DESC, [Data Center], [Associated Item], [Value]";
            }

            if (dtroCLog.Rows.Count > 0)
            {
                var segments = new List<Tuple<DateTime, DateTime, TimeSpan, List<CLogSummaryInfo>>>();
                DataRow dataSummaryRow;

                for (int nIndex = 0; nIndex < bucketFromAggregatePeriods.Count(); ++nIndex)
                {
                    segments.Add(new Tuple<DateTime, DateTime, TimeSpan, List<CLogSummaryInfo>>(bucketFromAggregatePeriods.ElementAt(nIndex).Item1,
                                                                                                    bucketFromAggregatePeriods.ElementAtOrDefault(nIndex + 1) == null
                                                                                                        ? DateTime.MinValue
                                                                                                        : bucketFromAggregatePeriods.ElementAt(nIndex + 1).Item1,
                                                                                                    bucketFromAggregatePeriods.ElementAt(nIndex).Item2,
                                                                                                    new List<CLogSummaryInfo>()));
                }

                Logger.Instance.InfoFormat("Summary Log Ranges: {{{0}}} Nbr Log Rows: {1:###,###,##0}", string.Join("; ", segments.Select(element => string.Format("#{0}# >= [Timestamp] and #{1}# < [Timestamp], Interval{{{2}}}", element.Item1, element.Item2, element.Item3))), dtroCLog.Rows.Count);

                Parallel.ForEach(segments, element =>
                //foreach (var element in segments)
                {
                    var dataView = new DataView(dtroCLog,
                                                string.Format("#{0}# >= [Timestamp] and #{1}# < [Timestamp]",
                                                                element.Item1,
                                                                element.Item2),
                                                "[Timestamp] DESC",
                                                DataViewRowState.CurrentRows);

                    var startPeriod = element.Item1;
                    var endPeriod = startPeriod - element.Item3;

                    //			dataView.RowFilter.Dump();
                    //			element.Dump();
                    //			if (dataView.ToTable().Rows.Count.Dump() > 0)
                    //			{				
                    //				dataView.ToTable().Rows[0].Dump();
                    //				dataView.ToTable().Rows[dataView.ToTable().Rows.Count - 1].Dump();
                    //			}

                    foreach (DataRowView dataRow in dataView)
                    {
                        if ((DateTime)dataRow["Timestamp"] < endPeriod)
                        {
                            startPeriod = endPeriod;
                            endPeriod = startPeriod - element.Item3;

                            if ((DateTime)dataRow["Timestamp"] < endPeriod)
                            {
                                var newBeginPeriod = ((DateTime)dataRow["Timestamp"]).RoundUp(element.Item3);

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

                        //		dtCLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                        //		dtCLog.Columns.Add("Node IPAddress", typeof(string));
                        //		dtCLog.Columns.Add("Timestamp", typeof(DateTime));
                        //		dtCLog.Columns.Add("Indicator", typeof(string));
                        //		dtCLog.Columns.Add("Task", typeof(string));
                        //		dtCLog.Columns.Add("Item", typeof(string));
                        //		dtCLog.Columns.Add("Associated Item", typeof(string)).AllowDBNull = true;
                        //		dtCLog.Columns.Add("Associated Value", typeof(object)).AllowDBNull = true;
                        //		dtCLog.Columns.Add("Description", typeof(string));
                        //		dtCLog.Columns.Add("Flagged", typeof(bool)).AllowDBNull = true;

                        var indicator = (string)dataRow["Indicator"];
                        var taskItem = (string)dataRow["Task"];
                        var item = (string)dataRow["Item"];
                        var exception = dataRow["Exception"] as string;

                        if (ignoreTaskExceptionItems.Contains(taskItem)
                            || ignoreTaskExceptionItems.Contains(item)
                            || ignoreTaskExceptionItems.Contains(exception))
                        {
                            continue;
                        }

                        if (logAggregateAdditionalTaskExceptionItems.Contains(taskItem))
                        {
                            var strItem = exception == null ? taskItem : taskItem + " (" + exception + ")";
                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Task", strItem, dataRow));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Task", strItem, dataRow);
                                element.Item4.Add(summaryInfo);
                            }

                            ++summaryInfo.AggregationCount;
                        }
                        else if (logAggregateAdditionalTaskExceptionItems.Contains(item))
                        {
                            if (indicator == "INFO")
                            {
                                bool? flagged = dataRow["Flagged"] as bool?;

                                if (!flagged.HasValue || !flagged.Value)
                                {
                                    continue;
                                }
                            }

                            var strItem = exception == null ? item : item + " (" + exception + ")";
                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Item", strItem, dataRow));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Item", strItem, dataRow);
                                element.Item4.Add(summaryInfo);
                            }

                            ++summaryInfo.AggregationCount;
                        }
                        else if (logAggregateAdditionalTaskExceptionItems.Contains(exception))
                        {
                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Exception", exception, dataRow));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Exception", exception, dataRow);
                                element.Item4.Add(summaryInfo);
                            }

                            ++summaryInfo.AggregationCount;
                        }
                        else if (!string.IsNullOrEmpty(exception))
                        {
                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Exception", exception, dataRow));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Exception", exception, dataRow);
                                element.Item4.Add(summaryInfo);
                            }

                            ++summaryInfo.AggregationCount;
                        }
                        else if (logAggregateIndicators.Contains(indicator))
                        {
                            var summaryInfo = element.Item4.Find(x => x.Equals(startPeriod, "Indicator", indicator, dataRow));

                            if (summaryInfo == null)
                            {
                                summaryInfo = new CLogSummaryInfo(startPeriod, element.Item3, "Indicator", indicator, dataRow);
                                element.Item4.Add(summaryInfo);
                            }

                            ++summaryInfo.AggregationCount;
                        }

                    }

                });//foreach

                foreach (var element in segments)
                {
                    if (element.Item4.Count == 0)
                    {
                        dataSummaryRow = dtCSummaryLog.NewRow();

                        dataSummaryRow["Data Center"] = null;
                        dataSummaryRow["Node IPAddress"] = null;

                        dataSummaryRow["Timestamp Period"] = element.Item1;
                        dataSummaryRow["Aggregation Period"] = element.Item3;
                        dataSummaryRow["Type"] = null;
                        dataSummaryRow["Value"] = null;
                        dataSummaryRow["Associated Item"] = null;
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
                            dataSummaryRow["Value"] = item.ItemValue;
                            dataSummaryRow["Associated Item"] = item.AssociatedItem;
                            dataSummaryRow["Occurrences"] = item.AggregationCount;

                            dtCSummaryLog.Rows.Add(dataSummaryRow);
                        }
                    }
                }
            }

            Logger.Instance.InfoFormat("Summary Log Nbr Rows: {0:###,###,##0}", dtCSummaryLog.Rows.Count);
        }

        static Regex RegExG1Line = new Regex(@"\s*G1.+in\s+(\d+)(?:.*Eden Space:\s*(\d+)\s*->\s*(\d+))?(?:.*Old Gen:\s*(\d+)\s*->\s*(\d+))?(?:.*Survivor Space:\s*(\d+)\s*->\s*(\d+).*)?.*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExGCLine = new Regex(@"\s*GC.+ParNew:\s+(\d+)",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExGCMSLine = new Regex(@"\s*ConcurrentMarkSweep.+in\s+(\d+)(?:.*Old Gen:\s*(\d+)\s*->\s*(\d+))?(?:.*Eden Space:\s*(\d+)\s*->\s*(\d+))?.*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExPoolLine = new Regex(@"\s*(\w+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s+(\d+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCacheLine = new Regex(@"\s*(\w+)\s+(\d+)\s+(\d+)\s+(\w+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExTblLine = new Regex(@"\s*(.+)\s+(\d+)\s*,\s*(\d+)\s*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExPool2Line = new Regex(@"\s*(\w+)\s+(\w+/\w+|\d+)\s+(\w+/\w+|\d+).*",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCompactionTaskCompletedLine = new Regex(@"Compacted\s+(\d+)\s+sstables.+\[\s*(.+)\,\s*\]\.\s+(.+)\s+bytes to (.+)\s+\(\s*(.+)\s*\%.+in\s+(.+)\s*ms\s+=\s+(.+)\s*MB/s.\s+(\d+).+merged to\s+(\d+).+were\s+\{\s*(.+)\,\s*\}",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static void ParseCassandraLogIntoStatusLogDataTable(DataTable dtroCLog,
                                                                DataTable dtCStatusLog,
                                                                DataTable dtCFStats,
                                                                DataTable dtTPStats,
                                                                Common.Patterns.Collections.ThreadSafe.Dictionary<string, string> dictGCIno,
                                                                string ipAddress,
                                                                string dcName,
                                                                List<string> ignoreKeySpaces,
                                                                List<CKeySpaceTableNames> kstblExists)
        {
            //GCInspector.java:258 - G1 Young Generation GC in 691ms.  G1 Eden Space: 4,682,940,416 -> 0; G1 Old Gen: 2,211,450,256 -> 2,797,603,280; G1 Survivor Space: 220,200,960 -> 614,465,536; 
            //GCInspector.java:258 - G1 Young Generation GC in 277ms. G1 Eden Space: 4047503360 -> 0; G1 Old Gen: 2855274656 -> 2855274648;
            //GCInspector.java (line 116) GC for ParNew: 394 ms for 1 collections, 13571498424 used; max is 25340346368
            //ConcurrentMarkSweep GC in 363ms. CMS Old Gen: 5688178056 -> 454696416; Par Eden Space: 3754560 -> 208755688;
            //ConcurrentMarkSweep GC in 2083ms. CMS Old Gen: 8524829104 -> 8531031448; CMS Perm Gen: 68555136 -> 68555392; Par Eden Space: 1139508352 -> 47047616; Par Survivor Space: 35139688 -> 45900968
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

            if (dtCStatusLog.Columns.Count == 0)
            {
                dtCStatusLog.Columns.Add("Timestamp", typeof(DateTime));
                dtCStatusLog.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Node IPAddress", typeof(string));
                dtCStatusLog.Columns.Add("Pool/Cache Type", typeof(string)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("KeySpace", typeof(string)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Table", typeof(string)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("GC Time (ms)", typeof(long)).AllowDBNull = true; //g
                dtCStatusLog.Columns.Add("Eden-From (mb)", typeof(decimal)).AllowDBNull = true; //h
                dtCStatusLog.Columns.Add("Eden-To (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Old-From (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Old-To (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Survivor-From (mb)", typeof(decimal)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Survivor-To (mb)", typeof(decimal)).AllowDBNull = true; //m
                dtCStatusLog.Columns.Add("Active", typeof(object)).AllowDBNull = true; //n
                dtCStatusLog.Columns.Add("Pending", typeof(object)).AllowDBNull = true; //o
                dtCStatusLog.Columns.Add("Completed", typeof(long)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("Blocked", typeof(long)).AllowDBNull = true;
                dtCStatusLog.Columns.Add("All Time Blocked", typeof(long)).AllowDBNull = true; //r
                dtCStatusLog.Columns.Add("Size (mb)", typeof(decimal)).AllowDBNull = true;//s
                dtCStatusLog.Columns.Add("Capacity (mb)", typeof(decimal)).AllowDBNull = true; //y
                dtCStatusLog.Columns.Add("KeysToSave", typeof(string)).AllowDBNull = true; //u
                dtCStatusLog.Columns.Add("MemTable OPS", typeof(long)).AllowDBNull = true; //v
                dtCStatusLog.Columns.Add("Data (mb)", typeof(decimal)).AllowDBNull = true; //w

                dtCStatusLog.Columns.Add("SSTables", typeof(int)).AllowDBNull = true; //x
                dtCStatusLog.Columns.Add("From (mb)", typeof(decimal)).AllowDBNull = true; //y
                dtCStatusLog.Columns.Add("To (mb)", typeof(decimal)).AllowDBNull = true;//z
                dtCStatusLog.Columns.Add("Latency (ms)", typeof(int)).AllowDBNull = true; //aa
                dtCStatusLog.Columns.Add("Rate (MB/s)", typeof(decimal)).AllowDBNull = true; //ab
                dtCStatusLog.Columns.Add("Partitions Merged", typeof(string)).AllowDBNull = true; //ac
                dtCStatusLog.Columns.Add("Merge Counts", typeof(string)).AllowDBNull = true; //ad

                dtCStatusLog.DefaultView.Sort = "[Timestamp] DESC, [Data Center], [Pool/Cache Type], [KeySpace], [Table], [Node IPAddress]";
            }

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
                //		dtCLog.Columns.Add("Flagged", typeof(bool)).AllowDBNull = true;

                var statusLogView = new DataView(dtroCLog,
                                                    "[Item] in ('GCInspector.java', 'StatusLogger.java', 'CompactionTask.java')" +
                                                        " or ([Item] in ('CompactionController.java'" +
                                                                            ", 'SSTableWriter.java'" +
                                                                            ", 'SliceQueryFilter.java'" +
                                                                            ", 'CqlSlowLogWriter.java'" +
                                                                            ", 'FailureDetector.java'" +
                                                                            ", 'BatchStatement.java'" +
                                                                            ", 'JVMStabilityInspector.java'" +
                                                                            ", 'WorkPool.java'" +
                                                                            ") and [Flagged] = true)",
                                                    "[TimeStamp] ASC, [Item] ASC",
                                                    DataViewRowState.CurrentRows);
                var gcLatencies = new List<int>();
                var pauses = new List<long>();
                var compactionLatencies = new List<Tuple<string, string, int>>();
                var compactionRates = new List<Tuple<string, string, decimal>>();
                var partitionLargeSizes = new List<Tuple<string, string, decimal>>();
                var tombstoneCounts = new List<Tuple<string, string, int>>();
                var tpStatusCounts = new List<Tuple<string, long, long, long, long, long>>();
                var statusMemTables = new List<Tuple<string, string, long, decimal>>();
                var tpSlowQueries = new List<int>();
                var batchSizes = new List<Tuple<string, string, int>>();
                var jvmFatalErrors = new List<string>();
                var workPoolErrors = new List<string>();

                string item;

                foreach (DataRowView vwDataRow in statusLogView)
                {
                    item = vwDataRow["Item"] as string;

                    if (string.IsNullOrEmpty(item))
                    {
                        continue;
                    }

                    if (item == "GCInspector.java")
                    {
                        #region GCInspector.java
                       
                        var descr = vwDataRow["Description"] as string;

                        if (string.IsNullOrEmpty(descr))
                        {
                            continue;
                        }

                        if (descr.TrimStart().StartsWith("GC for ParNew"))
                        {
                            var splits = RegExGCLine.Split(descr);
                            var dataRow = dtCStatusLog.NewRow();
                            var time = DetermineTime(splits[1]);

                            dataRow["Timestamp"] = vwDataRow["Timestamp"];
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC-ParNew";
                            dataRow["GC Time (ms)"] = (long) time;

                            dtCStatusLog.Rows.Add(dataRow);
                            gcLatencies.Add((int)time);

                            dictGCIno.TryAdd((dcName == null ? string.Empty : dcName) + "|" + ipAddress, "GC-ParNew");
                        }
                        if (descr.TrimStart().StartsWith("ConcurrentMarkSweep"))
                        {
                            var splits = RegExGCMSLine.Split(descr);
                            var dataRow = dtCStatusLog.NewRow();
                            var time = DetermineTime(splits[1]);

                            dataRow["Timestamp"] = vwDataRow["Timestamp"];
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC-CMS";
                            dataRow["GC Time (ms)"] = (long) ((dynamic) time);

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
                            gcLatencies.Add((int)time);

                            dictGCIno.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress, "GC-CMS", (item1, item2) => "GC-CMS");
                        }
                        else if (descr.TrimStart().StartsWith("G1 Young Generation GC in"))
                        {
                            var splits = RegExG1Line.Split(descr);
                            var dataRow = dtCStatusLog.NewRow();
                            var time = DetermineTime(splits[1]);

                            dataRow["Timestamp"] = vwDataRow["Timestamp"];
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC-G1";
                            dataRow["GC Time (ms)"] = (long) ((dynamic) time);

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
                            gcLatencies.Add((int)time);

                            dictGCIno.AddOrUpdate((dcName == null ? string.Empty : dcName) + "|" + ipAddress, "GC-C1", (item1, item2) => "GC-G1");
                        }

                        continue;
                        #endregion
                    }
                    else if (item == "FailureDetector.java")
                    {
                        #region FailureDetector.java
                        var exception = vwDataRow["Exception"] as string;

                        if (exception.StartsWith("Pause"))
                        {
                            var dataRow = dtCStatusLog.NewRow();
                            var time = vwDataRow["Associated Value"] as long?;

                            dataRow["Timestamp"] = vwDataRow["Timestamp"];
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["Pool/Cache Type"] = "GC Pause";

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
                    else if (item == "StatusLogger.java")
                    {
                        #region StatusLogger.java
                        var descr = vwDataRow["Description"] as string;

                        if (string.IsNullOrEmpty(descr))
                        {
                            continue;
                        }

                        descr = descr.Trim();

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

                                    dataRow["Timestamp"] = vwDataRow["Timestamp"];
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = splits[1];
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

                                    dataRow["Timestamp"] = vwDataRow["Timestamp"];
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = "ColumnFamily";
                                    dataRow["KeySpace"] = ksTable.Item1;
                                    dataRow["Table"] = ksTable.Item2;
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

                                    dataRow["Timestamp"] = vwDataRow["Timestamp"];
                                    dataRow["Data Center"] = dcName;
                                    dataRow["Node IPAddress"] = ipAddress;
                                    dataRow["Pool/Cache Type"] = splits[1];

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
                    else if (item == "CompactionTask.java")
                    {
                        #region CompactionTask
                        var descr = vwDataRow["Description"] as string;
                        var splits = RegExCompactionTaskCompletedLine.Split(descr);

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

                                dataRow["Timestamp"] = vwDataRow["Timestamp"];
                                dataRow["Data Center"] = dcName;
                                dataRow["Node IPAddress"] = ipAddress;
                                dataRow["Pool/Cache Type"] = "Compaction";

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
                            }
                        }
                        #endregion
                    }
                    else if (item == "CompactionController.java" || item == "SSTableWriter.java")
                    {
                        #region CompactionController or SSTableWriter

                        var kstblName = vwDataRow["Associated Item"] as string;
                        var partSize = vwDataRow["Associated Value"] as decimal?;

                        if (kstblName == null || !partSize.HasValue)
                        {
                            continue;
                        }

                        var kstblSplit = SplitTableName(kstblName, null);

                        if (ignoreKeySpaces.Contains(kstblSplit.Item1))
                        {
                            continue;
                        }

                        partitionLargeSizes.Add(new Tuple<string, string, decimal>(kstblSplit.Item1, kstblSplit.Item2, partSize.Value));

                        #endregion
                    }
                    else if (item == "SliceQueryFilter.java")
                    {
                        #region SliceQueryFilter

                        var kstblName = vwDataRow["Associated Item"] as string;
                        var partSize = vwDataRow["Associated Value"] as int?;
                        var warningType = vwDataRow["Exception"] as string;

                        if (kstblName == null || !partSize.HasValue || warningType != "Query Tombstones Warning")
                        {
                            continue;
                        }

                        var kstblSplit = SplitTableName(kstblName, null);

                        if (ignoreKeySpaces.Contains(kstblSplit.Item1))
                        {
                            continue;
                        }

                        tombstoneCounts.Add(new Tuple<string, string, int>(kstblSplit.Item1, kstblSplit.Item2, partSize.Value));

                        #endregion
                    }
                    else if (item == "CqlSlowLogWriter.java")
                    {
                        #region CqlSlowLogWriter
                        var time = vwDataRow["Associated Value"] as int?;

                        if (time.HasValue)
                        {
                            tpSlowQueries.Add(time.Value);
                        }

                        #endregion
                    }
                    else if(item == "BatchStatement.java")
                    {
                        #region BatchSize

                        var kstblName = vwDataRow["Associated Item"] as string;
                        var batchSize = vwDataRow["Associated Value"] as int?;
                        
                        if (kstblName == null || !batchSize.HasValue)
                        {
                            continue;
                        }

                        var kstblSplit = SplitTableName(kstblName, null);

                        if (ignoreKeySpaces.Contains(kstblSplit.Item1))
                        {
                            continue;
                        }

                        batchSizes.Add(new Tuple<string, string, int>(kstblSplit.Item1, kstblSplit.Item2, batchSize.Value));

                        #endregion
                    }                   
                    else if (item == "JVMStabilityInspector.java")
                    {
                        #region JVMStabilityInspector
                        var exception = vwDataRow["Exception"] as string;

                        if (!string.IsNullOrEmpty(exception))
                        {
                            jvmFatalErrors.Add(exception);
                        }

                        #endregion
                    }
                    else if (item == "WorkPool.java")
                    {
                        #region WorkPool
                        var exception = vwDataRow["Exception"] as string;

                        if (!string.IsNullOrEmpty(exception))
                        {
                            workPoolErrors.Add(exception);
                        }

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
                        dataRow["Latency (ms)"] = gcMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC maximum latency";
                        dataRow["Latency (ms)"] = gcMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC mean latency";
                        dataRow["Latency (ms)"] = gcAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "GC occurrences";
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
                        dataRow["Latency (ms)"] = slowMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query maximum latency";
                        dataRow["Latency (ms)"] = slowMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query mean latency";
                        dataRow["Latency (ms)"] = slowAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Slow Query occurrences";
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
                        dataRow["Latency (ms)"] = gcMin;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause maximum latency";
                        dataRow["Latency (ms)"] = gcMax;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause mean latency";
                        dataRow["Latency (ms)"] = gcAvg;

                        dtTPStats.Rows.Add(dataRow);

                        dataRow = dtTPStats.NewRow();

                        dataRow["Source"] = "Cassandra Log";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["Attribute"] = "Pause occurrences";
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
                            dataRow["Occurrences"] = wpGrp.Count;

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
                                dataRow["Value"] = (int)(statItem.Max * BytesToMB);
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
                                dataRow["Value"] = (int)(statItem.Min * BytesToMB);
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
                                dataRow["Value"] = (int)(statItem.Avg * BytesToMB);
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

                        tombstoneCounts.RemoveAll(x => x.Item3 <= 0);

                        if (tombstoneCounts.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding Tombstone Counts ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, tombstoneCounts.Count);

                            var compStats = from cmpItem in tombstoneCounts
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
                                dataRow["Attribute"] = "Tombstones warning maximum";
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
                                dataRow["Attribute"] = "Tombstones warning minimum";
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
                                dataRow["Attribute"] = "Tombstones warning mean";
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
                                dataRow["Attribute"] = "Tombstones warning occurrences";
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

                        batchSizes.RemoveAll(x => x.Item3 <= 0);

                        if (batchSizes.Count > 0)
                        {
                            Logger.Instance.InfoFormat("Adding Batch Sizes ({2}) to CFStats for \"{0}\" \"{1}\"", dcName, ipAddress, batchSizes.Count);

                            var compStats = from cmpItem in batchSizes
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
                                dataRow["Attribute"] = "Batch size maximum";
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
                                dataRow["Attribute"] = "Batch size minimum";
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
                                dataRow["Attribute"] = "Batch size mean";
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
                                dataRow["Attribute"] = "Batch size occurrences";
                                dataRow["Value"] = statItem.Count;
                                dataRow["(Value)"] = statItem.Count;
                                //dataRow["Unit of Measure"] = "bytes";

                                dtCFStats.Rows.Add(dataRow);
                            }
                        }

                        #endregion
                    }
                }

                #endregion
            }
        }


    }
}
