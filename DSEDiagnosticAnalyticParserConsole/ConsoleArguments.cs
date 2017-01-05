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
        /// Defines a threshold that will flag a log entry in both the log summary and log status for GC latencies.
        /// </summary>
        [Option('G', "GCFlagThresholdInMS", HelpText = "Defines a threshold that will flag a log entry in both the log summary and log status for GC latencies.",
                    Required = false)]
        public int GCFlagThresholdInMS
        {
            get { return ParserSettings.GCFlagThresholdInMS; }
            set { ParserSettings.GCFlagThresholdInMS = value; }
        }

        /// <summary>
        /// Defines a threshold that will flag a log entry in both the log summary and log status for compaction latencies.
        /// </summary>
        [Option('C', "CompactionFlagThresholdInMS", HelpText = "Defines a threshold that will flag a log entry in both the log summary and log status for compaction latencies. -1 disables this feature.",
                    Required = false)]
        public int CompactionFlagThresholdInMS
        {
            get { return ParserSettings.CompactionFlagThresholdInMS; }
            set { ParserSettings.CompactionFlagThresholdInMS = value; }
        }

        [Option('R', "CompactionFlagThresholdAsIORate", HelpText = "Defines a threshold that if the IO rate below this threshold will flag a log entry in both the log summary and log status for compaction IO rate (MB/Sec). -1 disables this feature.",
                    Required = false)]
        public decimal CompactionFlagThresholdAsIORate
        {
            get { return ParserSettings.CompactionFlagThresholdAsIORate; }
            set { ParserSettings.CompactionFlagThresholdAsIORate = value; }
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
        /// Import Log entries from this date. MinDate will parse all log entries.
        /// </summary>
        [Option('Z', "LogStartDate", HelpText = "Only import log entries from this date/time. MinDate will parse all entries.",
                    Required = false)]
        public DateTime LogStartDate
        {
            get { return ParserSettings.LogStartDate; }
            set { ParserSettings.LogStartDate = value; }
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

        [Option('L', "LogParsingExcelOption", HelpText = "A list of Log parsing and Excel workbook creation options. Multiple options should be separated by a comma (,).",
                    Required = false)]
        public string LogParsingExcelOption
        {
            get { return ParserSettings.LogParsingExcelOption.ToString(); }
            set { ParserSettings.LogParsingExcelOption = ParserSettings.ParseEnumString<ParserSettings.LogParsingExcelOptions>(value, ParserSettings.LogParsingExcelOption); }
        }

        [Option('E', "ParsingExcelOptions", HelpText = "A list of parsing and Excel workbook and worksheet creation options. Multiple options should be separated by a comma (,).",
                    Required = false)]
        public string ParsingExcelOption
        {
            get { return ParserSettings.ParsingExcelOption.ToString(); }
            set { ParserSettings.ParsingExcelOption = ParserSettings.ParseEnumString<ParserSettings.ParsingExcelOptions>(value, ParserSettings.ParsingExcelOption); }
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

		[Option('O', "FileParsingOption", HelpText = "Structure of the folders and file names used to determine the context of each file.",
				   Required = false)]
		public ParserSettings.FileParsingOptions FileParsingOption
		{
			get { return ParserSettings.FileParsingOption; }
			set { ParserSettings.FileParsingOption = value; }
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
        [Option('d', "AlternativeDDLFilePath", HelpText = "Additional file path that is used to parse CQL/DDL files. Wild cards can be included.",
                    Required = false)]
        public string AlternativeDDLFilePath
        {
            get { return string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath)
                            ? ParserSettings.AlternativeDDLFilePath
                            : Common.Path.PathUtils.BuildFilePath(ParserSettings.AlternativeDDLFilePath)?.PathResolved; }
            set { ParserSettings.AlternativeDDLFilePath = value; }
        }

        [Option('t', "TableHistogramDirPath", HelpText = "Directory of files that contain the results of a nodetool TableHistogram. The file name must have the node's IP address in the beginning or end of the name. If not provide the DiagnosticPath is searched looking for files with the string \"TableHistogram\" embedded in the name.",
                    Required = false)]
        public string TableHistogramDirPath
        {
            get
            {
                return string.IsNullOrEmpty(ParserSettings.TableHistogramDirPath)
                            ? null
                            : Common.Path.PathUtils.BuildFilePath(ParserSettings.TableHistogramDirPath)?.PathResolved;
            }
            set
            {
                ParserSettings.TableHistogramDirPath = value;
            }
        }

        /// <summary>
        /// A list of Keyspaces to ignore during parsing.
        /// </summary>
        [Option('I', "IgnoreKeySpaces", HelpText = "A list of Keyspaces to ignore during parsing. If an empty string, the list is cleared. If the name is prefixed with a '+' or '-', that name is added or removed from the existing list. Names without a prefix will be used to replace the complete list (names with prefixes are ignored). Case-Sensitive",
                    Required = false)]
        public string IgnoreKeySpaces
        {
            get { return string.Join(", ", ParserSettings.IgnoreKeySpaces); }
            set
            {
                if (string.IsNullOrEmpty(value))
                {
                    ParserSettings.IgnoreKeySpaces.Clear();
                }
                else
                {
                    var keySpaces = StringFunctions.Split(value, ',',
                                                        StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                        StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries
                                                            | StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                    var newList = new List<string>();

                    foreach(var name in keySpaces.Select(item => ProcessFileTasks.RemoveQuotes(item)))
                    {
                        if(name[0] == '+')
                        {
                            var newItem = name.Substring(1);

                            if(!ParserSettings.IgnoreKeySpaces.Contains(newItem))
                            {
                                ParserSettings.IgnoreKeySpaces.Add(newItem);
                            }
                        }
                        else if(name[0] == '-')
                        {
                            var newItem = name.Substring(1);
                            ParserSettings.IgnoreKeySpaces.Remove(newItem);
                        }
                        else
                        {
                            newList.Add(name);
                        }
                    }

                    if (newList.Count > 0)
                    {
                        ParserSettings.IgnoreKeySpaces = newList;
                    }
                }
            }
        }

        private bool allowPerformanceKeyspaces = false;

        [Option('i', "IncludePerformanceKeyspaces", HelpText = "Performance Keyspaces are included during parsing. The default based on IgnoreKeySpaces are to ignore these Keyspaces.",
                    Required = false)]
        public bool IncludePerformanceKeyspaces
        {
            get { return allowPerformanceKeyspaces; }
            set
            {
                if(allowPerformanceKeyspaces != value)
                {
                    allowPerformanceKeyspaces = value;
                    ParserSettings.IgnoreKeySpaces.RemoveAll(ks => ParserSettings.PerformanceKeyspaces.Contains(ks));

                    if (!allowPerformanceKeyspaces)
                    {
                        ParserSettings.IgnoreKeySpaces.AddRange(ParserSettings.PerformanceKeyspaces);
                    }
                }
            }
        }

        private bool allowOpscenterKeyspaces = false;

        [Option('k', "IncludeOpsCenterKeyspace", HelpText = "Included the OpsCenter Keyspace.",
                    Required = false)]
        public bool IncludeOpsCenterKeyspace
        {
            get { return allowOpscenterKeyspaces; }
            set
            {
                if (allowOpscenterKeyspaces != value)
                {
                    allowOpscenterKeyspaces = value;
                    ParserSettings.IgnoreKeySpaces.RemoveAll(ks => ks == "OpsCenter");

                    if (!allowOpscenterKeyspaces)
                    {
                        ParserSettings.IgnoreKeySpaces.Add("OpsCenter");
                    }
                }
            }
        }

        [Option('g', "ToleranceContinuousGCInMS", HelpText = "The amount of time, in milliseconds, between GCs that will determine if the GCs are continuous (back-to-back). If negative, this feature is disabled.",
                    Required = false)]
        public int ToleranceContinuousGCInMS
        {
            get { return ParserSettings.ToleranceContinuousGCInMS; }
            set
            {
                ParserSettings.ToleranceContinuousGCInMS = value;
            }
        }

        [Option('v', "NbrGCInSeriesToConsiderContinuous", HelpText = "The number of GC in a series (row) that will be considered as continuous (back-to-back). This works in conjunction with ToleranceContinuousGCInMS to determine the series.",
                    Required = false)]
        public int NbrGCInSeriesToConsiderContinuous
        {
            get { return ParserSettings.ContinuousGCNbrInSeries; }
            set { ParserSettings.ContinuousGCNbrInSeries = value; }
        }

        [Option('f', "GCTimeFrameDetection", HelpText = "A time frame (00:00:00) used to determine the percent of GC activity based on GCTimeFrameDetectionPercentage. If zero, this feature is disabled.",
                    Required = false)]
        public TimeSpan GCTimeFrameDetection
        {
            get { return ParserSettings.GCTimeFrameDetection; }
            set { ParserSettings.GCTimeFrameDetection = value; }
        }

        [Option('e', "GCTimeFrameDetectionPercentage", HelpText = "A percentage of time used within the time frame (GCTimeFrameDetection) to determine excess GC activity. If -1, this feature is disabled.",
                    Required = false)]
        public decimal GCTimeFrameDetectionPercentage
        {
            get { return ParserSettings.GCTimeFrameDetectionPercentage; }
            set { ParserSettings.GCTimeFrameDetectionPercentage = value; }
        }

		[Option('A', "MaxNbrAchievedLogFiles", HelpText = "The maximum number of archived log files that are read per node. If the value is -1, all file are read (disabled).",
				   Required = false)]
		public int MaxNbrAchievedLogFiles
		{
			get { return ParserSettings.MaxNbrAchievedLogFiles; }
			set { ParserSettings.MaxNbrAchievedLogFiles = value; }
		}

		[Option('r', "ReadRepairThresholdInMS", HelpText = "The number of milliseconds after a ReadRepair session is completed to still detect a GC or Compaction event for that session.",
				   Required = false)]
		public int ReadRepairThresholdInMS
		{
			get { return ParserSettings.ReadRepairThresholdInMS; }
			set { ParserSettings.ReadRepairThresholdInMS = value; }
		}

		[Option('o', "CLogLineFormatPosition", HelpText = "C* Log line format layout. The default is \"{IndicatorPos:0,TaskPos:1,ItemPos:4,TimeStampPos:2,DescribePos:5}\"",
				   Required = false)]
		public string CLogLineFormatPosition
		{
			get { return Properties.Settings.Default.CLogLineFormatPositions; }
			set
			{
				if (string.IsNullOrEmpty(value) || value == "null" || value == "\"\"")
				{
					ParserSettings.CLogLineFormats = new ParserSettings.CLogLineFormat();
				}
				else
				{
					ParserSettings.CLogLineFormats = Newtonsoft.Json.JsonConvert.DeserializeObject<ParserSettings.CLogLineFormat>(value);
				}
			}
		}

		[Option('?', "DisplayDefaults", HelpText = "Displays Arguments and Default Values",
                    Required = false)]
        public bool DisplayDefaults
        {
            get;
            set;
        }

        [Option("Debug", HelpText = "Debug",
                    Required = false)]
        public bool Debug
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
            var tableHistogramDirPath = string.IsNullOrEmpty(ParserSettings.TableHistogramDirPath) ? null : Common.Path.PathUtils.BuildDirectoryPath(ParserSettings.TableHistogramDirPath);
            var alternativeDDLFilePath = string.IsNullOrEmpty(ParserSettings.AlternativeDDLFilePath) ? null : Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeDDLFilePath);
            var alternativeLogFilePath = string.IsNullOrEmpty(ParserSettings.AlternativeLogFilePath) ? null : Common.Path.PathUtils.BuildPath(ParserSettings.AlternativeLogFilePath);

            if (!diagnosticPath.Exist())
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

            if (tableHistogramDirPath != null)
            {
                if (tableHistogramDirPath.IsRelativePath)
                {
                    IAbsolutePath absPath;

                    if (diagnosticPath.MakePathFrom((IRelativePath)tableHistogramDirPath, out absPath))
                    {
                        tableHistogramDirPath = (IDirectoryPath)absPath;
                    }
                }

                if (!tableHistogramDirPath.Exist())
                {
                    var msg = string.Format("TableHistogram Folder doesn't exists. Path is \"{0}\".", tableHistogramDirPath.PathResolved);

                    Console.WriteLine(msg);
                    Logger.Instance.Error(msg);
                    bResult = false;
                }
            }

            if (alternativeDDLFilePath != null)
            {
                if (!alternativeDDLFilePath.HasWildCardPattern())
                {
                    if (alternativeDDLFilePath.IsRelativePath)
                    {
                        IAbsolutePath absPath;

                        if (diagnosticPath.MakePathFrom((IRelativePath)alternativeDDLFilePath, out absPath))
                        {
                            alternativeDDLFilePath = absPath;
                        }
                    }

                    if (!alternativeDDLFilePath.Exist())
                    {
                        var msg = string.Format("Alternative DDL path doesn't exists. Path is \"{0}\".", alternativeDDLFilePath.PathResolved);

                        Console.WriteLine(msg);
                        Logger.Instance.Error(msg);
                        bResult = false;
                    }
                }
            }

            if (alternativeLogFilePath != null)
            {
                if (!alternativeLogFilePath.HasWildCardPattern())
                {
                    if (alternativeLogFilePath.IsRelativePath)
                    {
                        IAbsolutePath absPath;

                        if (diagnosticPath.MakePathFrom((IRelativePath)alternativeLogFilePath, out absPath))
                        {
                            alternativeLogFilePath = absPath;
                        }
                    }

                    if (!alternativeLogFilePath.Exist())
                    {
                        var msg = string.Format("Alternative Log Path doesn't exists. Path is \"{0}\".", alternativeLogFilePath.PathResolved);

                        Console.WriteLine(msg);
                        Logger.Instance.Error(msg);
                        bResult = false;
                    }
                }
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
                                    "--CompactionFlagThresholdAsIORate|-R {25}" +
                                    "--SlowLog[Q]ueryThresholdInMS {4} " +
									"--MaxNbrAchievedLogFiles|-A {5}" +
                                    "--LogStartDate|-Z \"{6}\" " +
                                    "--LogExcelWorkbook[F]ilter \"{7}\" " +
                                    "--Parsing[E]xcelOptions {{{8}}} " +
                                    "--[L]ogParsingExcelOption {{{9}}} " +
                                    "" +
                                    "--Excel[T]emplateFilePath \"{12}\" " +
                                    "--ExcelFile[P]ath \"{13}\" " +
									"--FileParsingOption|-O {14} " +
                                    "--[D]iagnosticPath \"{15}\" " +
                                    "--AlternativeLogFilePath|-l \"{16}\" " +
                                    "--AlternativeDDLFilePath|-d \"{17}\" " +
                                    "--[I]gnoreKeySpaces {{{18}}} " +
                                    "--IncludePerformanceKeyspaces|-i {19} " +
                                    "--IncludeOpsCenterKeyspace|-k {20} " +
                                    "--TableHistogramDirPath|-t \"{21}\" " +
                                    "--ToleranceContinuousGCInMS|-g {22} " +
                                    "--NbrGCInSeriesToConsiderContinuous|-v {26} " +
                                    "--GCTimeFrameDetection|-f {23} " +
                                    "--GCTimeFrameDetectionPercentage|-e {24} " +
									"--ReadRepairThresholdInMS|-r {27} " +
									"--CLogLineFormatPosition|-o {28}",
                                    this.MaxRowInExcelWorkSheet,
                                    this.MaxRowInExcelWorkBook,
                                    this.GCFlagThresholdInMS,
                                    this.CompactionFlagThresholdInMS,
                                    this.SlowLogQueryThresholdInMS,
                                    this.MaxNbrAchievedLogFiles,
                                    this.LogStartDate,
                                    this.LogExcelWorkbookFilter,
                                    this.ParsingExcelOption,
                                    this.LogParsingExcelOption,
                                    null, //10
                                    null,
                                    this.ExcelTemplateFilePath,
                                    this.ExcelFilePath,
                                    this.FileParsingOption,
                                    this.DiagnosticPath,
                                    this.AlternativeLogFilePath,
                                    this.AlternativeDDLFilePath,
                                    this.IgnoreKeySpaces,
                                    this.IncludePerformanceKeyspaces,
                                    this.IncludeOpsCenterKeyspace, //20
                                    this.TableHistogramDirPath,
                                    this.ToleranceContinuousGCInMS,
                                    this.GCTimeFrameDetection,
                                    this.GCTimeFrameDetectionPercentage,
                                    this.NbrGCInSeriesToConsiderContinuous,
                                    this.CompactionFlagThresholdAsIORate,
									this.ReadRepairThresholdInMS, //27
									this.CLogLineFormatPosition);
        }
    }
}
