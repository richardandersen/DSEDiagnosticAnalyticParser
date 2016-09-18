using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using CommandLine;
using CommandLine.Text;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    class ConsoleArguments
    {
        public ConsoleArguments()
        { }

        /// <summary>
        /// Maximum number of excel rows within a workbook. -1 to disable.
        /// </summary>
        [Option('S', "MaxRowInExcelWorkSheet", HelpText = "Maximum number of excel rows within a workbook. -1 to disable.",
                    Required = false)]
        public int MaxRowInExcelWorkSheet
        {
            get { return ParserSettings.MaxRowInExcelWorkSheet; }
            set { ParserSettings.MaxRowInExcelWorkSheet = value; }
        }

        /// <summary>
        /// Maximum number of excel rows within a workbook. -1 to disable.
        /// </summary>
        [Option('B', "MaxRowInExcelWorkBook", HelpText = "Maximum number of excel rows within a workbook. -1 to disable.",
                    Required = false)]
        public int MaxRowInExcelWorkBook
        {
            get { return ParserSettings.MaxRowInExcelWorkBook; }
            set { ParserSettings.MaxRowInExcelWorkBook = value; }
        }

        /// <summary>
        /// Defines a threshold that will flag a log entry in both the log summary (only if GCInspector.java is defined) and log status for GC latencies. 
        /// </summary>
        [Option('G', "GCFlagThresholdInMS", HelpText = "Defines a threshold that will flag a log entry in both the log summary (only if GCInspector.java is defined) and log status for GC latencies.",
                    Required = false)]
        public int GCFlagThresholdInMS
        {
            get { return ParserSettings.GCFlagThresholdInMS; }
            set { ParserSettings.GCFlagThresholdInMS = value; }
        }

        /// <summary>
        /// Defines a threshold that will flag a log entry in both the log summary (only if CompactionTask.java is defined) and log status for compaction latencies.
        /// </summary>
        [Option('C', "CompactionFlagThresholdInMS", HelpText = "Defines a threshold that will flag a log entry in both the log summary (only if CompactionTask.java is defined) and log status for compaction latencies.",
                    Required = false)]
        public int CompactionFlagThresholdInMS
        {
            get { return ParserSettings.CompactionFlagThresholdInMS; }
            set { ParserSettings.CompactionFlagThresholdInMS = value; }
        }

        /// <summary>
        /// Defines a threshold that will flag a log entry in both the log summary and log status for Slow queries.
        /// </summary>
        [Option('Q', "SlowLogQueryThresholdInMS", HelpText = "Defines a threshold that will flag a log entry in both the log summary and log status for Slow queries.",
                    Required = false)]
        public int SlowLogQueryThresholdInMS
        {
            get { return ParserSettings.SlowLogQueryThresholdInMS; }
            set { ParserSettings.SlowLogQueryThresholdInMS = value; }
        }

        /// <summary>
        /// Only import log entries for the past time period (e.g., the last 5 days) from the LogCurrentDate
        /// </summary>
        [Option('R', "LogTimeSpanRange", HelpText = "Only import log entries for the past time period (e.g., the last 5 days) from the LogCurrentDate",
                    Required = false)]
        public TimeSpan LogTimeSpanRange
        {
            get { return ParserSettings.LogTimeSpanRange; }
            set { ParserSettings.LogTimeSpanRange = value; }
        }

        /// <summary>
        /// Import Log entries from this date based on the LogTimeSpanRange. MinDate will parse all log entries.
        /// </summary>
        [Option('Z', "LogCurrentDate", HelpText = "Only import log entries for the past time period (e.g., the last 5 days) from the LogCurrentDate",
                    Required = false)]
        public DateTime LogCurrentDate
        {
            get { return ParserSettings.LogCurrentDate; }
            set { ParserSettings.LogCurrentDate = value; }
        }

        /// <summary>
        /// This filter is only used when loading into Excel. Null disables filtering. 
        /// </summary>
        /// <example>
        /// [Timestamp] &gt;= #2016-08-01#
        /// </example>
        /// <remarks>
        ///Creates a filter that is used for loading the Cassandra Log Worksheets
        ///     Data Columns are:
 		///         [Data Center], string, AllowDBNull
        ///         [Node IPAddress], string
        ///         [Timestamp], DateTime      
        /// </remarks>
        [Option('F', "LogExcelWorkbookFilter", HelpText = "This filter is only used when loading into Excel. Null disables filtering.",
                    Required = false)]
        public string LogExcelWorkbookFilter
        {
            get { return ParserSettings.LogExcelWorkbookFilter; }
            set { ParserSettings.LogExcelWorkbookFilter = value; }
        }

        /// <summary>
        /// True to allow all the log entries to be imported into Excel. This is independent of parsing. If log parsing is disabled, loading into Excel is also disabled. 
        /// </summary>
        [Option('E', "LoadLogsIntoExcel", HelpText = "True to allow all the log entries to be imported into Excel. This is independent of parsing. If log parsing is disabled, loading into Excel is also disabled.",
                    Required = false)]
        public bool LoadLogsIntoExcel
        {
            get { return ParserSettings.LoadLogsIntoExcel; }
            set { ParserSettings.LoadLogsIntoExcel = value; }
        }

        [Option('e', "DisableLoadLogsIntoExcel", HelpText = "Disabled the loading of logs into Excel",
                   Required = false)]
        public bool DisableLoadLogsIntoExcel
        {
            get { return ParserSettings.LoadLogsIntoExcel; }
            set { ParserSettings.LoadLogsIntoExcel = !value; }
        }

        /// <summary>
        /// True to allow parsing of log files.
        /// </summary>
        [Option('L', "ParseLogs", HelpText = "True to allow parsing of log files.",
                    Required = false)]
        public bool ParseLogs
        {
            get { return ParserSettings.ParseLogs; }
            set { ParserSettings.ParseLogs = value; }
        }

        [Option('l', "DisableParseLogs", HelpText = "Disable the parsing of log files.",
                    Required = false)]
        public bool DisableParseLogs
        {
            get { return ParserSettings.ParseLogs; }
            set { ParserSettings.ParseLogs = !value; }
        }

        /// <summary>
        /// True to parse non-log files (e.g., cfstats, ring, json, etc.). 
        /// </summary>
        [Option('N', "ParseNonLogs", HelpText = "True to parse non-log files (e.g., cfstats, ring, json, etc.).",
                    Required = false)]
        public bool ParseNonLogs
        {
            get { return ParserSettings.ParseNonLogs; }
            set { ParserSettings.ParseNonLogs = value; }
        }

        [Option('n', "DisableParseNonLogs", HelpText = "Disables the parsing of non-log files (e.g., cfstats, ring, json, etc.).",
                    Required = false)]
        public bool DisableParseNonLogs
        {
            get { return ParserSettings.ParseNonLogs; }
            set { ParserSettings.ParseNonLogs = !value; }
        }

        /// <summary>
        /// True to parse archived log files. 
        /// </summary>
        /// <remarks>
        /// Parsing of archive log files is also dependent on DiagnosticNoSubFolders being false.
        /// </remarks>
        [Option('A', "ParseArchivedLogs", HelpText = "True to parse archived log files.",
                    Required = false)]
        public bool ParseArchivedLogs
        {
            get { return ParserSettings.ParseArchivedLogs; }
            set { ParserSettings.ParseArchivedLogs = value; }
        }

        [Option('a', "DisableParseArchivedLogs", HelpText = "Disables the parsing of the archived log files.",
                    Required = false)]
        public bool DisableParseArchivedLogs
        {
            get { return ParserSettings.ParseArchivedLogs; }
            set { ParserSettings.ParseArchivedLogs = !value; }
        }

        /// <summary>
        /// Location of the Excel Template File that is copied and updated with the DSE data.
        /// </summary>
        [Option('T', "ExcelTemplateFilePath", HelpText = "Location of the Excel Template File that is copied and updated with the DSE data. If Relative Path, this path is merged with current value. If null, no template is used.",
                    Required = false)]
        public string ExcelTemplateFilePath
        {
            get
            {
                return Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelTemplateFilePath)?.PathResolved;
            }
            set
            {
                if (value == null || ParserSettings.ExcelTemplateFilePath == null)
                {
                    ParserSettings.ExcelTemplateFilePath = value;
                }
                else
                {
                    var currentPath = Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelTemplateFilePath);
                    var newPath = Common.Path.PathUtils.BuildPath(value,
                                                                    currentPath.ParentDirectoryPath.PathResolved,
                                                                    currentPath.FileExtension,
                                                                    true,
                                                                    true,
                                                                    true);
                    ParserSettings.ExcelTemplateFilePath = newPath.Path;
                }
            }
        }

        /// <summary>
        /// Excel target file.
        /// </summary>
        [Option('P', "ExcelFilePath", HelpText = "Excel target file. If Relative Path, this path is merged with current value.",
                    Required = false)]
        public string ExcelFilePath
        {
            get
            {
                return Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelFilePath)?.PathResolved;
            }
            set
            {
                var currentPath = Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelFilePath);
                var newPath = Common.Path.PathUtils.BuildPath(value,
                                                                currentPath.ParentDirectoryPath.PathResolved,
                                                                currentPath.FileExtension,
                                                                true,
                                                                true,
                                                                true);
                ParserSettings.ExcelFilePath = newPath.Path;
            }
        }

        /// <summary>
        /// If true the diagnostic files are not in the standard OpsCenter folder format. Instead each file name has the IP address embedded at the beginning or end of the file name.
        /// </summary>
        /// <remarks>
        ///If diagnosticNoSubFolders is false:
        ///     Directory where files are located to parse DSE diagnostics files produced by DataStax OpsCenter diagnostics or a special directory structure where DSE diagnostics information is placed.
        ///     If the &quot;special&quot; directory is used it must follow the following structure:
	    ///     &lt;MySpecialFolder&gt; -- this is the location used for the diagnosticPath variable
	    ///             |- &lt;DSENodeIPAddress&gt; (the IPAddress must be located at the beginning or the end of the folder name) e.g., 10.0.0.1, 10.0.0.1-DC1, Diag-10.0.0.1
        ///             |       | - nodetool -- static folder name
	    ///             |       |	    | - cfstats 	-- This must be the output file from nodetool cfstats(static name)
	    ///             |       |		| - ring		-- This must be the output file from nodetool ring(static name)
	    ///             |		|       | - tpstats
		///             |       |		| - info
		///             |       |		| - compactionhistory
		///             |       | - logs -- static folder name
		///             |       |   | - Cassandra -- static folder name
		///             |       |   | - system.log -- This must be the Cassandra log file from the node
	    ///             | - &lt;NextDSENodeIPAddress&gt; -- e.g., 10.0.0.2, 10.0.0.2-DC1, Diag-10.0.0.2
        ///
        ///If diagnosticNoSubFolders is true:
        ///     All diagnostic files are located directly under diagnosticPath folder.Each file should have the IP address either in the beginning or end of the file name.
        ///     e.g., cfstats_10.192.40.7, system-10.192.40.7.log, 10.192.40.7_system.log, etc.
        /// </remarks>
        [Option('O', "DiagnosticNoSubFolders", HelpText = "If true the diagnostic files are not in the standard OpsCenter folder format. Instead each file name has the IP address embedded at the beginning or end of the file name.",
                    Required = false)]
        public bool DiagnosticNoSubFolders
        {
            get { return ParserSettings.DiagnosticNoSubFolders; }
            set { ParserSettings.DiagnosticNoSubFolders = value; }
        }

        [Option('o', "DiagnosticSubFolders", HelpText = "The folder follows the OpsCenter Diagnostic Tar Ball structure.",
                    Required = false)]
        public bool DiagnosticSubFolders
        {
            get { return ParserSettings.DiagnosticNoSubFolders; }
            set { ParserSettings.DiagnosticNoSubFolders = !value; }
        }

        /// <summary>
        /// The directory location of the diagnostic files. The structure of these folders and files is depending on the value of DiagnosticNoSubFolders.
        /// </summary>
        [Option('D', "DiagnosticPath", HelpText = "The directory location of the diagnostic files. The structure of these folders and files is depending on the value of DiagnosticNoSubFolders. If Relative Path, this path is merged with current value.",
                    Required = false)]
        public string DiagnosticPath
        {
            get { return Common.Path.PathUtils.BuildDirectoryPath(ParserSettings.DiagnosticPath)?.PathResolved; }
            set
            {
                var currentPath = Common.Path.PathUtils.BuildDirectoryPath(ParserSettings.DiagnosticPath);
                var newPath = Common.Path.PathUtils.BuildPath(value,
                                                                currentPath.PathResolved,
                                                                null,
                                                                false,
                                                                true,
                                                                true,
                                                                true,
                                                                false);
                ParserSettings.DiagnosticPath = newPath.Path;
            }
        }

        /// <summary>
        /// Additional file path that is used to parse log files where the IP address must be in the beginning or end of the file name. Wild cards can be included. 
        /// </summary>
        [Option('1', "AlternativeLogFilePath", HelpText = "Additional file path that is used to parse log files where the IP address must be in the beginning or end of the file name. Wild cards can be included.",
                    Required = false)]
        public string AlternativeLogFilePath
        {
            get { return string.IsNullOrEmpty(ParserSettings.AlternativeLogFilePath)
                            ? ParserSettings.AlternativeLogFilePath
                            : Common.Path.PathUtils.BuildFilePath(ParserSettings.AlternativeLogFilePath)?.PathResolved; }
            set { ParserSettings.AlternativeLogFilePath = value; }
        }

        /// <summary>
        /// Additional file path that is used to parse CQL/DDL files. Wild cards can be included.
        /// </summary>
        [Option('2', "AlternativeDDLFilePath", HelpText = "Additional file path that is used to parse CQL/DDL files. Wild cards can be included.",
                    Required = false)]
        public string AlternativeDDLFilePath
        {
            get { return string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath)
                            ? ParserSettings.AlternativeDDLFilePath
                            : Common.Path.PathUtils.BuildFilePath(ParserSettings.AlternativeDDLFilePath)?.PathResolved; }
            set { ParserSettings.AlternativeDDLFilePath = value; }
        }

        /// <summary>
        /// A list of keyspaces to ignore during parsing.
        /// </summary>
        [Option("IgnoreKeySpaces", HelpText = "A list of keyspaces to ignore during parsing.",
                    Required = false)]
        public string IgnoreKeySpaces
        {
            get { return string.Join(", ", ParserSettings.IgnoreKeySpaces); }
            set
            {
                var keySpaces = StringFunctions.Split(value, ',',
                                                        StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                        StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries
                                                            | StringFunctions.SplitBehaviorOptions.StringTrimEachElement);

                ParserSettings.IgnoreKeySpaces = keySpaces?.Select(item => ProcessFileTasks.RemoveQuotes(item).ToLower()).ToList();
            }
        }

        [Option('?', "DisplayDefaults", HelpText = "Displays Arguments and Default Values",
                    Required = false)]
        public bool DisplayDefaults
        {
            get;
            set;
        }

        public bool CheckArguments()
        {
            bool bResult = true;
            var diagnosticPath = Common.Path.PathUtils.BuildDirectoryPath(ParserSettings.DiagnosticPath);
            var excelFilePathrentPath = Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelFilePath);
            var excelTemplateFilePath = ParserSettings.ExcelTemplateFilePath == null ? null : Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelTemplateFilePath);

            if(!diagnosticPath.Exist())
            {
                var msg = string.Format("Diagnostic Directory doesn't exists. Directory is \"{0}\".", diagnosticPath.PathResolved);

                Console.WriteLine(msg);
                Logger.Instance.Error(msg);
                bResult = false;
            }

            if (!excelFilePathrentPath.ParentDirectoryPath.Exist())
            {
                var msg = string.Format("Excel Target Directory doesn't exists. Directory is \"{0}\".", excelFilePathrentPath.ParentDirectoryPath.PathResolved);

                Console.WriteLine(msg);
                Logger.Instance.Error(msg);
                bResult = false;
            }

            if (excelTemplateFilePath != null && !excelTemplateFilePath.Exist())
            {
                var msg = string.Format("Excel Template file doesn't exists. File is \"{0}\".", excelTemplateFilePath.PathResolved);

                Console.WriteLine(msg);
                Logger.Instance.Error(msg);
                bResult = false;
            }            

            return bResult;
        }
        public override string ToString()
        {
            return string.Format("Values: " +
                                    "--MaxRowInExcelWork[S]heet {0} " +
                                    "--MaxRowInExcelWork[B]ook {1} " +
                                    "--[G]CFlagThresholdInMS {2} " +
                                    "--[C]ompactionFlagThresholdInMS {3} " +
                                    "--SlowLog[Q]ueryThresholdInMS {4} " +
                                    "--LogTimeSpan[R]ange \"{5}\" " +
                                    "--LogCurrentDate|-Z \"{6}\" " +
                                    "--LogExcelWorkbook[F]ilter \"{7}\" " +
                                    "--LoadLogsInto[E]xcel {8} " +
                                    "--Parse[L]ogs {9} " +
                                    "--Parse[N]onLogs {10} " +
                                    "--Parse[A]rchivedLogs {11} " +
                                    "--Excel[T]emplateFilePath \"{12}\" " +
                                    "--ExcelFile[P]ath \"{13}\" " +
                                    "--DiagnosticNoSubFolders|-O {14} " +
                                    "--[D]iagnosticPath \"{15}\" " +
                                    "--AlternativeLogFilePath|-1 \"{16}\" " +
                                    "--AlternativeDDLFilePath|-2 \"{17}\" " +
                                    "--IgnoreKeySpaces {{{18}}}",
                                    this.MaxRowInExcelWorkSheet,
                                    this.MaxRowInExcelWorkBook,
                                    this.GCFlagThresholdInMS,
                                    this.CompactionFlagThresholdInMS,
                                    this.SlowLogQueryThresholdInMS,
                                    this.LogTimeSpanRange,
                                    this.LogCurrentDate,
                                    this.LogExcelWorkbookFilter,
                                    this.LoadLogsIntoExcel,
                                    this.ParseLogs,
                                    this.ParseNonLogs,
                                    this.ParseArchivedLogs,
                                    this.ExcelTemplateFilePath,
                                    this.ExcelFilePath,
                                    this.DiagnosticNoSubFolders,
                                    this.DiagnosticPath,
                                    this.AlternativeLogFilePath,
                                    this.AlternativeDDLFilePath,
                                    this.IgnoreKeySpaces);
        }
    }
}
