using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public class ParserSettings
    {
        [Flags]
        public enum LogParsingExcelOptions
        {
            /// <summary>
            /// If enabled, the log settings will be determined based on ParsingExcelOptions settings.
            /// </summary>
            Detect = 0,
            /// <summary>
            /// If enabled, current log files will be included for parsing
            /// </summary>
            Parse = 0x0001,
            /// <summary>
            /// If enabled, archived log files will be included for parsing
            /// </summary>
            ParseArchivedLogs = 0x0002,
            /// <summary>
            /// If enabled, the parsed log files will be captured into separate Excel workbooks.
            /// </summary>
            CreateWorkbook = 0x0004,
            /// <summary>
            /// Enables both current and archived log parsing.
            /// </summary>
            ParseLogs = Parse | ParseArchivedLogs,
            /// <summary>
            /// Enables both current and archived log parsing plus captures this output into Excel workbook(s).
            /// </summary>
            ParseCreateAll = Parse | CreateWorkbook | ParseArchivedLogs,
            /// <summary>
            /// Enables only current log parsing plus captures this output into Excel workbook(s).
            /// </summary>
            ParseCreateOnlyCurrentLogs = Parse | CreateWorkbook
        }

        [Flags]
        public enum ParsingExcelOptions
        {
            Disable = 0,

            /// <summary>
            /// Enables nodetool CFStats file parsing which is used by the analysis worksheets
            /// </summary>
            ParseCFStatsFiles = 0x0001,
            /// <summary>
            /// Enables nodetool TPStats file parsing which is used by the analysis worksheets
            /// </summary>
            ParseTPStatsFiles = 0x0002,
            /// <summary>
            /// Enables nodetool compacation history file parsing
            /// </summary>
            ParseCompacationHistFiles = 0x0004,
            /// <summary>
            /// Enables DDL (cqlsh describe) file processing
            /// Note: This option is important for proper analysis
            /// </summary>
            ParseDDLFiles = 0x0008,
            /// <summary>
            /// Enables nodetool Table/CF Histogram file processing which is used by the analysis worksheets
            /// Note: This option is important for proper analysis
            /// </summary>
            ParseTblHistogramFiles = 0x0010,
            /// <summary>
            /// Enables nodetool/dsetool ring file processing.
            /// Note: This option is important for proper data center related information and is recommended to be enabled.
            /// </summary>
            ParseRingInfoFiles = 0x0020,
            //ParseNodeInfoFiles = 0x0040,
            /// <summary>
            /// Enables machine/OS (created by OpsCenter) file parsing
            /// </summary>
            ParseMachineInfoFiles = 0x1000,
            /// <summary>
            /// Enables yaml/configuration file parsing
            /// </summary>
            ParseYamlFiles = 0x0080,
            /// <summary>
            /// Performs an analysis of the log files and produces a summary error/exception event worksheet.
            /// LogParsingExcelOptions.Parse, ParseArchivedLogs, and/or Detect is required to be enabled.
            /// </summary>
            ParseSummaryLogs = 0x0100,
            /// <summary>
            /// Logs are only summarized so that only overlapping time ranges are used based on the timestamp ranges found in every node's logs.
            /// This is only valid if ParseSummaryLogs is enabled.
            /// Note: This option is important for proper analysis
            /// </summary>
            OnlyOverlappingDateRanges = 0x0200,
            /// <summary>
            /// Enables ParseSummaryLogs and OnlyOverlappingDateRanges
            /// </summary>
            ParseSummaryLogsOnlyOverlappingDateRanges = ParseSummaryLogs | OnlyOverlappingDateRanges,
            /// <summary>
            /// Enables parsing of OpsCenter diagnostic files associated with node related information
            /// </summary>
            ParseOpsCenterFiles = 0x2000,
            /// <summary>
            /// Performs an analysis of the log files and produces a worksheet related to keyspace/table events.
            /// LogParsingExcelOptions.Parse, ParseArchivedLogs, and/or Detect is required to be enabled.
            /// Note: This option is important for proper analysis
            /// </summary>
            ParseCFStatsLogs = 0x4000,
            /// <summary>
            /// Performs an analysis of the log files and produces a worksheet related to node events.
            /// LogParsingExcelOptions.Parse, ParseArchivedLogs, and/or Detect is required to be enabled.
            /// Note: This option is important for proper analysis
            /// </summary>
            ParseNodeStatsLogs = 0x8000,

            /// <summary>
            /// Enables the creation of analysis worksheets (e.g., ParseNodeStatsLogs, ParseCFStatsLogs, ParseYamlFiles, etc.).
            /// </summary>
            LoadWorkSheets = 0x0400,
            /// <summary>
            /// Enables the creation of the log summary worksheet (i.e., ParseSummaryLogs)
            /// Note: This can be disabled and ProduceSummaryWorkbook is enabled to create a detail log exception/error workbook
            /// </summary>
            LoadSummaryWorkSheets = 0x0800,

            /// <summary>
            /// Enables the creation of the log summary exception/error Excel workbook. This workbook can be used to reconcile the summary worksheet.
            /// </summary>
            ProduceSummaryWorkbook = 0x10000,
            /// <summary>
            /// Enables the creation of the node/table stats/information Excel workbook based on the analysis of the log files. This workbook can be used to reconcile the analysis worksheet.
            /// </summary>
            ProduceStatsWorkbook = 0x20000,

            /// <summary>
            /// If enabled, this will determine the required ParsingExcelOptions, LogParsingExcelOptions, and application settings based on the &quot;Parse&quot; and &quote;Produce&quot; settings.
            /// </summary>
            Detect = 0x40000,

			/// <summary>
			/// Parses the logs for Read-Repair events including associated events like GC and compaction.
			/// </summary>
			ParseReadRepairs = 0x80000,
			/// <summary>
			/// Enables the creation of the Read-Repair worksheet (i.e., ParseReadRepairs)
			/// </summary>
			LoadReadRepairWorkSheets = 0x100000,

			/// <summary>
			/// Enables the loading of all worksheets into the main Excel workbook.
			/// </summary>
			LoadAllWorkSheets = LoadWorkSheets | LoadSummaryWorkSheets | LoadReadRepairWorkSheets,
            /// <summary>
            /// Enables all settings.
            /// </summary>
            ParseLoadWorksheets = ParseCFStatsFiles
                                    | ParseTPStatsFiles
                                    | ParseCompacationHistFiles
                                    | ParseDDLFiles
                                    | ParseTblHistogramFiles
                                    | ParseRingInfoFiles
                                    | ParseMachineInfoFiles
                                    | ParseYamlFiles
                                    | ParseOpsCenterFiles
                                    | ParseSummaryLogsOnlyOverlappingDateRanges
                                    | ParseCFStatsLogs
                                    | ParseNodeStatsLogs
                                    | LoadAllWorkSheets
                                    | ProduceSummaryWorkbook
                                    | ProduceStatsWorkbook
									| ParseReadRepairs,
            /// <summary>
            /// Enables only settings related to the log summary analysis
            /// </summary>
            ParseLoadOnlySummaryLogs = ParseSummaryLogsOnlyOverlappingDateRanges | LoadSummaryWorkSheets | ProduceSummaryWorkbook
        }

		public enum FileParsingOptions
		{
			/// <summary>
			/// OpsCenter Diagnostic Tar-Ball structure
			/// </summary>
			OpsCtrDiagStruct = 0,
			/// <summary>
			/// Each file has the Node&apos;s IP Adress (prefixed or suffix) with the Nodetool/DSETool command.
			/// Example: 10.0.0.1-cfstats, 10.0.0.1-system.log, 10.0.0.1-cassandra.yaml, etc.
			/// </summary>
			IndivFiles = 1,
			/// <summary>
			/// Each file is within a folder where the Node&apos;s IP Adress (prefixed or suffix) is within the folder name. All files within this folder
			/// are prefixed by the command (e.g., dsetool, nodetool, etc.) followed by the command&apos;s subcommand/action. Logs and configuration files are just normal.
			/// Example: 10.0.0.1Folder\nodetool.ring, 10.0.0.1Folder\nodetool.cfstats, 10.0.0.1Folder\dsetool.ring, 10.0.0.1Folder\cqlsh.describe.cql, 10.0.0.1Folder\system.log, 10.0.0.1Folder\cassandra.yaml
			/// </summary>
			NodeSubFldStruct = 2
		}

		public class CLogLineFormat
		{
			//ERROR [SharedPool-Worker-3] 2016-10-01 19:20:14,415  Message.java:538 - Unexpected exception during request;
			public int IndicatorPos = 0; //ERROR
			public int TaskPos = 1; //[SharedPool-Worker-3]
			public int ItemPos = 4; //Message.java:53
			public int TimeStampPos = 2; //2016-10-01 19:20:14,415
			public int DescribePos = 5; //- Unexpected exception during request;
		}

		public static List<string> IgnoreKeySpaces = Properties.Settings.Default.IgnoreKeySpaces.ToList(false);

        public static int GCFlagThresholdInMS = Properties.Settings.Default.GCFlagThresholdInMS; //Defines a threshold that will flag a log entry in both the log summary (only if GCInspector.java) and log worksheets
        public static int CompactionFlagThresholdInMS = Properties.Settings.Default.CompactionFlagThresholdInMS; //Defines a threshold that will flag a log entry in both the log summary (only if CompactionTask.java) and log worksheets
        public static decimal CompactionFlagThresholdAsIORate = Properties.Settings.Default.CompactionFlagThresholdAsIORate;
        public static int SlowLogQueryThresholdInMS = Properties.Settings.Default.SlowLogQueryThresholdInMS;
        public static int ToleranceContinuousGCInMS = Properties.Settings.Default.ToleranceContinuousGCInMS;
        public static int ContinuousGCNbrInSeries = Properties.Settings.Default.ContinuousGCNbrInSeries;
        public static TimeSpan GCTimeFrameDetection = Properties.Settings.Default.GCTimeFrameDetection;
        public static decimal GCTimeFrameDetectionPercentage = Properties.Settings.Default.GCTimeFrameDetectionPercentage;
        public static System.Text.RegularExpressions.Regex ExcludePathNamesRegEx = string.IsNullOrEmpty(Properties.Settings.Default.ExcludePathNamesRegEx) ? null : new System.Text.RegularExpressions.Regex(Properties.Settings.Default.ExcludePathNamesRegEx, System.Text.RegularExpressions.RegexOptions.Compiled | System.Text.RegularExpressions.RegexOptions.IgnoreCase);
		public static int ReadRepairThresholdInMS = Properties.Settings.Default.ReadRepairThresholdInMS;

		public static DateTime LogStartDate = Properties.Settings.Default.LogCurrentDate; //DateTime.Now.Date; //If DateTime.MinValue all log entries are parsed
        public static string[] LogSummaryIgnoreTaskExceptions = Properties.Settings.Default.LogSummaryIgnoreTaskExceptions.ToArray(false);
        public static string[] LogSummaryTaskItems = Properties.Settings.Default.LogSummaryTaskItems.ToArray(false);
        public static Tuple<DateTime, TimeSpan>[] LogSummaryPeriods = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<DateTime, TimeSpan>[]>(Properties.Settings.Default.LogSummaryPeriods);
        //new Tuple<DateTime, TimeSpan>[] { new Tuple<DateTime,TimeSpan>(new DateTime(2016, 08, 02), new TimeSpan(0, 0, 30, 0)), //From By date/time and aggregation period
        //                                  new Tuple<DateTime,TimeSpan>(new DateTime(2016, 08, 1, 0, 0, 0), new TimeSpan(0, 1, 0, 0)),
        //                                  new Tuple<DateTime,TimeSpan>(new DateTime(2016, 07, 29, 0, 0, 0), new TimeSpan(1, 0, 0, 0))}; //null disable Summaries.
        //[{"Item1":"2016-08-02T00:00:00","Item2":"00:30:00"},{"Item1":"2016-08-01T00:00:00","Item2":"01:00:00"},{"Item1":"2016-07-29T00:00:00","Item2":"1.00:00:00"}]
        public static Tuple<TimeSpan, TimeSpan>[] LogSummaryPeriodRanges = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<TimeSpan, TimeSpan>[]>(Properties.Settings.Default.LogSummaryPeriodRanges);
        //new Tuple<TimeSpan, TimeSpan>[] { new Tuple<TimeSpan, TimeSpan>(new TimeSpan(1, 0, 0, 0), new TimeSpan(0, 0, 15, 0)), //Timespan from Log's Max Date or previous rang/Period time and aggregation period
        //                                  new Tuple<TimeSpan, TimeSpan>(new TimeSpan(1, 0, 0, 0), new TimeSpan(1, 0, 0, 0)),
        //                                  new Tuple<TimeSpan, TimeSpan>(new TimeSpan(4, 0, 0, 0), new TimeSpan(7, 0, 0, 0))};
        //[{"Item1":"1.00:00:00","Item2":"00:15:00"},{"Item1":"1.00:00:00","Item2":"1.00:00:00"},{"Item1":"4.00:00:00","Item2":"7.00:00:00"}]


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
		public static CLogLineFormat CLogLineFormats = Newtonsoft.Json.JsonConvert.DeserializeObject<CLogLineFormat>(Properties.Settings.Default.CLogLineFormatPositions);

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
		public static FileParsingOptions FileParsingOption = (FileParsingOptions) Enum.Parse(typeof(FileParsingOptions), Properties.Settings.Default.FileParsingOptions);
		public static string[] IgnoreLogFileExtensions = Properties.Settings.Default.IgnoreLogFileExtensions.ToArray(false);
		public static string[] ExtractFilesWithExtensions = Properties.Settings.Default.ExtractFilesWithExtensions.ToArray(false);
		public static int MaxNbrAchievedLogFiles = Properties.Settings.Default.MaxNbrAchievedLogFiles;

		public static LogParsingExcelOptions LogParsingExcelOption = ParseEnumString<LogParsingExcelOptions>(Properties.Settings.Default.LogParsingExcelOptions);
        public static ParsingExcelOptions ParsingExcelOption = ParseEnumString<ParsingExcelOptions>(Properties.Settings.Default.ParsingExcelOptions);
        public static List<string> CFStatsCreateMBColumns = Properties.Settings.Default.CFStatsCreateMBColumns.ToList(false, true); //MUST BE IN LOWER CASE -- CFStats attributes that contains these phrases/words will convert their values from bytes to MB in a separate Excel Column
        public static int MaxRowInExcelWorkSheet = Properties.Settings.Default.MaxRowInExcelWorkSheet; //-1 disabled
        public static int MaxRowInExcelWorkBook = Properties.Settings.Default.MaxRowInExcelWorkBook; //-1 disabled

        public static string AlternativeLogFilePath = Properties.Settings.Default.AlternativeLogFilePath; //Additional file path that is used to parse log files where the IP address must be in the beginning or end of the file name. This wild cards can be included.
        public static string AlternativeDDLFilePath = Properties.Settings.Default.AlternativeDDLFilePath; //A file path which supports wild card patterns to parse CQL/DDL
        public static string TableHistogramDirPath = Properties.Settings.Default.TableHistogramDirPath;

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
        public static string ExcelWorkSheetStatusLogCassandra = Properties.Settings.Default.ExcelWorkSheetStatusLogCassandra;
        public static string ExcelWorkSheetExceptionSummaryLogCassandra = Properties.Settings.Default.ExcelWorkSheetExceptionSummaryLogCassandra;
		public static string ReadRepairWorkSheetName = Properties.Settings.Default.ReadRepairWorkSheetName;

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
        public static string[] LogCassandraDirSystemLogs = Properties.Settings.Default.LogCassandraDirSystemLogs.ToArray(false);
        public static string LogCassandraSystemLogFile = Properties.Settings.Default.LogCassandraSystemLogFile;
		//{0} -- Log File Name and Extension, {1} -- Log File Name Only
        public static string LogCassandraSystemLogFileArchive = Properties.Settings.Default.LogCassandraSystemLogFileArchive; //{0}* //system.log*
		public static string ConfCassandraDir = Properties.Settings.Default.ConfCassandraDir;
        public static string ConfCassandraFile = Properties.Settings.Default.ConfCassandraFile;
        public static string ConfDSEDir = Properties.Settings.Default.ConfDSEDir;
        public static string ConfDSEYamlFile = Properties.Settings.Default.ConfDSEYamlFile;
        public static string ConfDSEFile = Properties.Settings.Default.ConfDSEFile;
        public static string CQLDDLDirFile = Properties.Settings.Default.CQLDDLDirFile;
        public static string CQLDDLDirFileExt = Properties.Settings.Default.CQLDDLDirFileExt;
        public static string[] OSMachineFiles = Properties.Settings.Default.OSMachineFiles.ToArray(false); //Referenced from the node directory
        public static string OPSCenterDir = Properties.Settings.Default.OPSCenterDir;
        public static string[] OPSCenterFiles = Properties.Settings.Default.OPSCenterFiles.ToArray(false);
        public static string TableHistogramFileName = Properties.Settings.Default.TableHistogramFileName;
        public static string CFHistogramFileName = Properties.Settings.Default.CFHistogramFileName;
        public static string ExcelCFHistogramWorkSheet = Properties.Settings.Default.ExcelCFHistogramWorkSheet;
        public static string ExcelWorkBookFileExtension = Properties.Settings.Default.ExcelWorkBookFileExtension;
        public static Dictionary<string, string> SnitchFiles = CreateSnitchDictionary(Properties.Settings.Default.SnitcheFiles);

        public static string[] CFStatsAttribs = Properties.Settings.Default.CFStatsAttribs.ToArray(false);
        public static string[] NodeStatsAttribs = Properties.Settings.Default.NodeAttribs.ToArray(false);
        public static string[] TablehistogramAttribs = Properties.Settings.Default.TableHistogramAttribs.ToArray(false);
        public static string[] PerformanceKeyspaces = Properties.Settings.Default.PerformanceKeyspaces.ToArray(false);
        public static string[] SummaryIgnoreExceptions = Properties.Settings.Default.SummaryIgnoreExceptions.ToArray(false);

        public static Tuple<string,string,System.Data.DataViewRowState> DDLTableWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.DDLTableWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> TokenRangeWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.TokenRangeWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> OSMachineInfoWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.OSMachineInfoWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> YamlSettingsInfoWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.YamlSettingsInfoWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> NodeStatsWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.NodeStatsWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> DDLKeyspaceWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.DDLKeyspaceWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> RingInfoWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.RingInfoWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> SummaryLogWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.SummaryLogWorksheetFilterSort);
        public static Tuple<string, string, System.Data.DataViewRowState> CompactionHistWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.CompactionHistWorksheetFilterSort);
		public static Tuple<string, string, System.Data.DataViewRowState> StatsWorkBookFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.StatsWorkBookFilterSort);
		public static Tuple<string, string, System.Data.DataViewRowState> ReadRepairWorksheetFilterSort = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string, System.Data.DataViewRowState>>(Properties.Settings.Default.ReadRepairWorksheetFilterSort);

        public static bool DivideWorksheetIfExceedMaxRows = Properties.Settings.Default.DivideWorksheetIfExccedMaxRows;

        public static Dictionary<string, string> CreateSnitchDictionary(string configString)
        {
            var configObj = Newtonsoft.Json.JsonConvert.DeserializeObject<Tuple<string, string>[]>(configString);
            var dict = new Dictionary<string, string>();

            if(configObj != null)
            {
                foreach (var item in configObj)
                {
                    dict.Add(item.Item1.ToLower(), item.Item2);
                }
            }

            return dict;
        }

        public static bool ExcludePathName(string name)
        {
            return !string.IsNullOrEmpty(name)
                        && ExcludePathNamesRegEx != null
                        && ExcludePathNamesRegEx.IsMatch(name);
        }

        public static T ParseEnumString<T>(string enumString)
        {
            enumString = enumString.Replace("|", ",").Replace(" + ", ",").Replace(" - ", ", !").Replace(" ~", ", !");

			var enumValues = enumString.Split(',');
			T enumValue = (T) Enum.Parse(typeof(T), string.Join(",", enumValues.Where(i => !i.TrimStart().StartsWith("!"))), true);
			string removeValues = string.Join(",", enumValues.Where(i => i.TrimStart().StartsWith("!")).Select(i => i.TrimStart().Substring(1).TrimStart()));

			if (!string.IsNullOrEmpty(removeValues))
			{
				enumValue &= ~(dynamic)(T)Enum.Parse(typeof(T), removeValues, true);
			}

			return enumValue;
        }

		public static T ParseEnumString<T>(string enumString, T appendValue)
		{
			enumString = enumString?.Trim();

			if (string.IsNullOrEmpty(enumString))
			{
				return default(T);
			}

			if (enumString[0] == '+'
					|| enumString[0] == '-'
					|| enumString[0] == '|'
					|| enumString[0] == ','
					|| enumString[0] == '~')
			{
				return ParseEnumString<T>(appendValue.ToString() + " " + enumString);
			}

			if (enumString[0] == '!')
			{
				return ParseEnumString<T>(appendValue.ToString() + ", " + enumString);
			}

			return ParserSettings.ParseEnumString<T>(enumString);
		}

		public static bool IsEnabled(this LogParsingExcelOptions option)
        {
            return LogParsingExcelOption == option
                        ? true
                        :  (LogParsingExcelOption == LogParsingExcelOptions.Detect ? false : (LogParsingExcelOption & option) == option);
        }

        public static bool IsEnabled(this ParsingExcelOptions option)
        {
            return ParsingExcelOption == option
                        ? true
                        : (ParsingExcelOption == ParsingExcelOptions.Disable ? false : (ParsingExcelOption & option) == option);
        }

        public static bool CheckEnabled(this LogParsingExcelOptions options, LogParsingExcelOptions option)
        {
            return (options & option) == option;
        }

        public static bool CheckEnabled(this ParsingExcelOptions options, ParsingExcelOptions option)
        {
            return (options & option) == option;
        }
    }
}
