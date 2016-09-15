using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public class ParserSettings
    {
        public static int MaxRowInExcelWorkSheet = Properties.Settings.Default.MaxRowInExcelWorkSheet; //-1 disabled
        public static int MaxRowInExcelWorkBook = Properties.Settings.Default.MaxRowInExcelWorkBook; //-1 disabled
        public static int GCPausedFlagThresholdInMS = Properties.Settings.Default.GCPausedFlagThresholdInMS; //Defines a threshold that will flag a log entry in both the log summary (only if GCInspector.java) and log worksheets
        public static int CompactionFlagThresholdInMS = Properties.Settings.Default.CompactionFlagThresholdInMS; //Defines a threshold that will flag a log entry in both the log summary (only if CompactionTask.java) and log worksheets
        public static int SlowLogQueryThresholdInMS = Properties.Settings.Default.SlowLogQueryThresholdInMS;

        public static TimeSpan LogTimeSpanRange = Properties.Settings.Default.LogTimeSpanRange; //Only import log entries for the past timespan (e.g., the last 5 days) based on LogCurrentDate.
        public static DateTime LogCurrentDate = Properties.Settings.Default.LogCurrentDate; //DateTime.Now.Date; //If DateTime.MinValue all log entries are parsed
        public static int LogMaxRowsPerNode = Properties.Settings.Default.LogMaxRowsPerNode; // -1 disabled //If enabled only the current log file is read (no achieves).
        public static string[] LogSummaryIndicatorType = Properties.Settings.Default.LogSummaryIndicatorType.ToArray();
        public static string[] LogSummaryTaskItems = Properties.Settings.Default.LogSummaryTaskItems.ToArray();
        public static string[] LogSummaryIgnoreTaskExceptions = Properties.Settings.Default.LogSummaryIgnoreTaskExceptions.ToArray(false);
        public static Tuple<DateTime, TimeSpan>[] LogSummaryPeriods = null; //new Tuple<DateTime, TimeSpan>[] { new Tuple<DateTime,TimeSpan>(new DateTime(2016, 08, 02), new TimeSpan(0, 0, 30, 0)), //From By date/time and aggregation period
                                                                            //new Tuple<DateTime,TimeSpan>(new DateTime(2016, 08, 1, 0, 0, 0), new TimeSpan(0, 1, 0, 0)),
                                                                            //new Tuple<DateTime,TimeSpan>(new DateTime(2016, 07, 29, 0, 0, 0), new TimeSpan(1, 0, 0, 0))}; //null disable Summaries.
        public static Tuple<TimeSpan, TimeSpan>[] LogSummaryPeriodRanges = new Tuple<TimeSpan, TimeSpan>[] { new Tuple<TimeSpan,TimeSpan>(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 0, 15, 0)), //Timespan from Log's Max Date or prevous rang/Period time and aggregation period
																								new Tuple<TimeSpan,TimeSpan>(new TimeSpan(1, 0, 0, 0), new TimeSpan(1, 0, 0, 0)),
                                                                                                new Tuple<TimeSpan,TimeSpan>(new TimeSpan(4, 0, 0, 0), new TimeSpan(7, 0, 0, 0))}; //null disable Summaries.

        //Creates a filter that is used for loading the Cassandra Log Worksheets
        // Data Columns are:
        //	[Data Center], string, AllowDBNull
        //	[Node IPAddress], string
        //	[Timestamp], DateTime
        //	[Indicator], string (e.g., INFO, WARN, ERROR)
        //	[Task], string (e.g., ReadStage, CompactionExecutor)
        //	[Item], string (e.g., HintedHandoffMetrics.java, BatchStatement.java)
        //	[Exception], string, AllowDBNull (e.g., java.io.IOException)
        //	[Exception Description], string, AllowDBNull (e.g., "Caused by: java.io.IOException: Cannot proceed on repair because a neighbor (/10.27.34.54) is dead: session failed")
        //	[Associated Item], string, AllowDBNull (e.g., 10.27.34.54, <keyspace.tablename>)  
        //	[Associated Value], object, AllowDBNull (e.g., <size in MB>, <time in ms>)
        //	[Description], string -- log's description
        //	[Flagged], bool, AllowDBNull -- if true this log entry was flagged because it matched some criteria (e.g., GC Pauses -- GCInspector.java exceeds GCPausedFlagThresholdInMS)
        public static string LogExcelWorkbookFilter = Properties.Settings.Default.LogExcelWorkbookFilter; //"[Timestamp] >= #2016-08-01#"; //if null no filter is used. Only used for loading data into Excel
        public static bool LoadLogsIntoExcel = Properties.Settings.Default.LoadLogsIntoExcel;

        public static string ExcelTemplateFilePath = Properties.Settings.Default.ExcelTemplateFilePath;

        //Location where this application will write or update the Excel file.
        public static string ExcelFilePath = Properties.Settings.Default.ExcelFilePath;

        //If diagnosticNoSubFolders is false:
        //Directory where files are located to parse DSE diagnostics files produced by DataStax OpsCenter diagnostics or a special directory structure where DSE diagnostics information is placed.
        //If the "special" directory is used it must follow the following structure:
        // <MySpecialFolder> -- this is the location used for the diagnosticPath variable
        //    |- <DSENodeIPAddress> (the IPAddress must be located at the beginning or the end of the folder name) e.g., 10.0.0.1, 10.0.0.1-DC1, Diag-10.0.0.1
        //	  |       | - nodetool -- static folder name
        //	  |  	  |	     | - cfstats 	-- This must be the output file from nodetool cfstats (static name)
        //	  |  	  |		 | - ring		-- This must be the output file from nodetool ring (static name)
        //	  |		  |		 | - tpstats
        //	  |		  |		 | - info
        //	  |		  |		 | - compactionhistory
        //	  |  	  | - logs -- static folder name
        //	  |       | 	| - Cassandra -- static folder name
        //	  |  				    | - system.log -- This must be the Cassandra log file from the node
        //    | - <NextDSENodeIPAddress> -- e.g., 10.0.0.2, 10.0.0.2-DC1, Diag-10.0.0.2
        //
        //If diagnosticNoSubFolders is true:
        //	All diagnostic files are located directly under diagnosticPath folder. Each file should have the IP address either in the beginning or end of the file name.
        //		e.g., cfstats_10.192.40.7, system-10.192.40.7.log, 10.192.40.7_system.log, etc.
        public static string DiagnosticPath = Properties.Settings.Default.DiagnosticPath;

        public static bool DiagnosticNoSubFolders = Properties.Settings.Default.DiagnosticNoSubFolders;

        public static bool ParseLogs = Properties.Settings.Default.ParseLogs;
        public static bool ParseNonLogs = Properties.Settings.Default.ParseNonLogs;
        public static bool ParseArchivedLogs = Properties.Settings.Default.ParseArchivedLogs; //Only valid when diagnosticNoSubFolders is false and LogMaxRowsPerNode <= 0 (disabled)
        public static string AlternativeLogFilePath = Properties.Settings.Default.AlternativeLogFilePath; //Additional file path that is used to parse log files where the IP address must be in the beginning or end of the file name. This wild cards can be included. 
        public static string AlternativeDDLFilePath = Properties.Settings.Default.AlternativeDDLFilePath; //A file path which supports wild card patterns to parse CQL/DDL

        //Excel Worksheet names
        public static string ExcelWorkSheetRingInfo = Properties.Settings.Default.ExcelWorkSheetRingInfo;
        public static string ExcelWorkSheetRingTokenRanges = Properties.Settings.Default.ExcelWorkSheetRingTokenRanges;
        public static string ExcelWorkSheetCFStats = Properties.Settings.Default.ExcelWorkSheetCFStats;
        public static string ExcelWorkSheetNodeStats = Properties.Settings.Default.ExcelWorkSheetNodeStats;
        public static string ExcelWorkSheetLogCassandra = Properties.Settings.Default.ExcelWorkSheetLogCassandra;
        public static string ExcelWorkSheetDDLKeyspaces = Properties.Settings.Default.ExcelWorkSheetDDLKeyspaces;
        public static string ExcelWorkSheetDDLTables = Properties.Settings.Default.ExcelWorkSheetDDLTables;
        public static string ExcelWorkSheetCompactionHist = Properties.Settings.Default.ExcelWorkSheetCompactionHist;
        public static string ExcelWorkSheetYaml = Properties.Settings.Default.ExcelWorkSheetYaml;
        public static string ExcelWorkSheetOSMachineInfo = Properties.Settings.Default.ExcelWorkSheetOSMachineInfo;
        public static string ExcelWorkSheetSummaryLogCassandra = Properties.Settings.Default.ExcelWorkSheetSummaryLogCassandra;
        public static string ExcelWorkSheetStatusLogCassandra = Properties.Settings.Default.ExcelWorkSheetSummaryLogCassandra;
        //var excelPivotWorkSheets = new string[] {"Read-Write Counts", "Partitions", "Latency", "Storage-Size"};

        public static List<string> IgnoreKeySpaces = Properties.Settings.Default.IgnoreKeySpaces.ToList(false, true); //MUST BE IN LOWER CASe
        public static List<string> CFStatsCreateMBColumns = Properties.Settings.Default.CFStatsCreateMBColumns.ToList(false, true); //MUST BE IN LOWER CASE -- CFStats attributes that contains these phrases/words will convert their values from bytes to MB in a separate Excel Column

        //Static Directory/File names
        public static string DiagNodeDir = Properties.Settings.Default.DiagNodeDir;
        public static string NodetoolDir = Properties.Settings.Default.NodetoolDir;
        public static string DSEToolDir = Properties.Settings.Default.DSEToolDir;
        public static string LogsDir = Properties.Settings.Default.LogsDir;
        public static string NodetoolRingFile = Properties.Settings.Default.NodetoolRingFile;
        public static string DSEtoolRingFile = Properties.Settings.Default.DSEtoolRingFile;
        public static string NodetoolCFStatsFile = Properties.Settings.Default.NodetoolCFStatsFile;
        public static string NodetoolTPStatsFile = Properties.Settings.Default.NodetoolTPStatsFile;
        public static string NodetoolInfoFile = Properties.Settings.Default.NodetoolInfoFile;
        public static string NodetoolCompactionHistFile = Properties.Settings.Default.NodetoolCompactionHistFile;
        public static string LogCassandraDirSystemLog = Properties.Settings.Default.LogCassandraDirSystemLog;
        public static string LogCassandraSystemLogFile = Properties.Settings.Default.LogCassandraSystemLogFile;
        public static string LogCassandraSystemLogFileArchive = Properties.Settings.Default.LogCassandraSystemLogFileArchive; //system-*.log
        public static string ConfCassandraDir = Properties.Settings.Default.ConfCassandraDir;
        public static string ConfCassandraFile = Properties.Settings.Default.ConfCassandraFile;
        public static string ConfCassandraType = Properties.Settings.Default.ConfCassandraType;
        public static string ConfDSEDir = Properties.Settings.Default.ConfDSEDir;
        public static string ConfDSEYamlFile = Properties.Settings.Default.ConfDSEYamlFile;
        public static string ConfDSEYamlType = Properties.Settings.Default.ConfDSEYamlType;
        public static string ConfDSEType = Properties.Settings.Default.ConfDSEYamlType;
        public static string ConfDSEFile = Properties.Settings.Default.ConfDSEFile;
        public static string CQLDDLDirFile = Properties.Settings.Default.CQLDDLDirFile;
        public static string CQLDDLDirFileExt = Properties.Settings.Default.CQLDDLDirFileExt;
        //var nodetoolCFHistogramsFile = "cfhistograms"; //this is based on keyspace and table and not sure of the format. HC doc has it as cfhistograms_keyspace_table.txt
        public static string[] OSMachineFiles = Properties.Settings.Default.OSMachineFiles.ToArray(false); //Referenced from the node directory
        public static string OPSCenterDir = Properties.Settings.Default.OPSCenterDir;
        public static string[] OPSCenterFiles = Properties.Settings.Default.OPSCenterFiles.ToArray(false);

        static string[] ToArray(this System.Collections.Specialized.StringCollection stringCollection, bool returnNull = true)
        {
            if(stringCollection == null)
            {
                return returnNull ? null : new string[0];
            }

            var collectionItem = new string[stringCollection.Count];

            stringCollection.CopyTo(collectionItem, 0);

            return collectionItem;
        }

        static List<string> ToList(this System.Collections.Specialized.StringCollection stringCollection, bool returnNull = true, bool toLowerCase = false)
        {
            if (stringCollection == null)
            {
                return returnNull ? null : new List<string>(0);
            }

           return toLowerCase ? stringCollection.Cast<string>().Select(item => item.ToLower()).ToList() : stringCollection.Cast<string>().ToList();
        }
    }
}
