# DSEDiagnosticAnalyticParser
DSE Diagnostic Tar Ball Analytic Parser

This application will parse and perform analytics on a DataStax OpsCenter diagnostic tar ball. It will place this information into Excel workbooks for further analysis. The main workbook will contain a set of pivot tables that utilized the aggregated data. This workbook also contains a master filter worksheet and a "refresh" button that will refresh all the pivot tables which is required upon initial opening of the main workbook. There can be optional workbooks that provide detail information that can be used to reconcile the main workbook's worhsheets via the "reconciliation id". Each workbook will contain an "application" worksheet that contains information about the running of the application. **These workbooks are tested and targeted for Microsoft Excel 2013.** It is recommended that Excel x64 be used (not required). Other spreadsheet applications can be used but they may have limited functionality. 

There are two zip files that contain the required assemblies to run the application. These zip files are:

- DSEDiagnosticAnalyticParser_Win64_V#.zip -- This file contains the assemblies that should be ran under **MS-Windows x64** with at least 8GB of physical memory. The .Net frameworks of 4.0 and 4.5.2 are required. 

- DSEDiagnosticAnalyticParser_Mono_V#.zip -- This file contains the assembiies that should be ran under Mono 4.6 (Linux, Mac OS X, Windows). Mono can be downloaded from the Mono Project websit (http://www.mono-project.com/download/). After installing Mono you can run the application using the following command line: `mono DSEDiagnosticAnalyticParserConsole.exe -?` 
Warning: The terminal (console) windows must be resized before running the application to at least '117X30'. If the windows is smaller than these specification, an exception will be thrown. 

Where '#' is the package version.

To install, just unzip the file into a newly created folder. To execute run a console and change the directory to the folder where the assemblies have been placed and execute the 'exe' files. This is a console application (no GUI). 

`DSEDiagnosticAnalyticParserConsole.exe` can optionally take a set of arguments. These arguments override the default settings defined in the application configuration settings files (DSEDiagnosticAnalyticParserConsole.exe.config). The application configuration setting file, DSEDiagnosticAnalyticParserConsole.exe.config, defines the default settings for the command line arguments, excel options, sorting options, and for the logger (log4net). Currently log4net is configured to overwrite the log file (DSEDiagnosticAnalyticParserConsole.log) on each run. This log file will contain all warning and error messages and should be reviewed after each run. 

Below is a description of the application command line arguments:

**Help** -- Displays all arguments including documentation and shortcuts.

**DisplayDefaults|-?** -- Displays all arguments with the default values.

**GCFlagThresholdInMS** -- Defines a threshold, in milliseconds, that will flag GC related latencies in the C* log. Default 5000 

**CompactionFlagThresholdInMS** -- Defines a threshold, in milliseconds, that will flag compaction latencies in the C* log. Default 10000

**CompactionFlagThresholdAsIORate** -- Defines a threshold that if the IO rate below this threshold will flag a log entry in both the log summary and log status for compaction IO rate (MB/Sec). -1 disables this feature.

**SlowLogQueryThresholdInMS** -- Defines a threshold, in milliseconds, that will flag query latencies in the C* log. Default 2000

**ToleranceContinuousGCInMS** -- The amount of time, in milliseconds, between GCs that will determine if the GCs are continuous (back-to-back). If negative, this feature is disabled. The default is 500ms

**NbrGCInSeriesToConsiderContinuous** -- The number of GC in a series (row) that will be considered as continuous (back-to-back). This works in conjunction with ToleranceContinuousGCInMS to determine the series.

**GCTimeFrameDetectionPercentage** -- A percentage (as a decimal value) of time used within the time frame (GCTimeFrameDetection) to determine excess GC activity. If -1, this feature is disabled. Default is 0.25 (25%).

**GCTimeFrameDetection** -- A time frame (format of 00:00:00) used to determine the percent of GC activity based on 'GCTimeFrameDetectionPercentage'. If zero, this feature is disabled. The default is 00:05:00 (5 minutes).
Note: Using the default values, if GC(s) take up 25% of 5 minutes (i.e., 1.25 minutes), these GC(s) will be reported in the GC worksheet.

**ReadRepairThresholdInMS** -- Number of milliseconds after a read repair session ends that we consider a GC, compaction, solr rebuild index events that will be assocated with that read repair session. The default is 150ms.

**QueueDroppedBlockedWarningPeriodInMins** -- The time frame, in minutes, that is used to aggregate the total number of drops and blocks detected in the logs. If less than or equal to zero, will allow any single instance to trigger this warning. Default 30 mins

**QueueDroppedBlockedWarningThreshold** -- The number of drops and blocks detected in the log that will trigger this warning within QueueDroppedBlockedWarningPeriodInMins. If less than or equal to zero, will disable this option. Default 100 drops/blocks

**EnableLogReadThrottle** -- If Yes (default), all log file reads will be throttled based on the LogReadThrottleTaskCount and LogReadThrottleWaitPeriodMS application config settings.

**LogStartDate|Z** -- The date/time used to start collecting C* log entries. Log entries greater than and equal this date/time will be collected. If no value (null), all entries will be collected. Default is no value (all entries).

**LogMaxRowsPerNode** -- The maximum number of C* log entries that are read per log file node. -1 will allow all entries to be read. Default is -1. Note: If this is enabled (> 0), reading C* achieve logs are disabled (only the most LogMaxRowsPerNode current lines in each Cassandra/system.log are read).

**IgnoreKeySpaces|-I** -- A collection of keyspaces that are ignored during parsing. Default is: dse_system, system_auth, system_traces, system, dse_perf

**IncludePerformanceKeyspaces|-i** -- If true, performance keyspaces are included during parsing. The default is to ignore these keyspaces.

**IncludeOpsCenterKeyspace|-k** -- if true, the OpsCenter Keyspace is included. The default is false.

**DiagnosticPath|-D** -- The path of the folder that contains the diagnostic files. This can be an absolute or a relative path. This is a required field. The default is "[MyDocuments]\DataStax\TestData\OpsCenter-diagnostics-2016_08_30_19_08_03_UTC". Note the structure of the content of this folder is dependent on the value of DiagnosticNoSubFolders. 

**FileParsingOption|-O** -- Structure of the folders and file names used to determine the diagnostic content. The default is OpsCtrDiagStrut. Values are:
```
	OpsCtrDiagStruct -- OpsCenter Diagnostic Tar-Ball structure
      		<MySpecialFolder> -- this is the location used for the diagnosticPath variable
			|- <DSENodeIPAddress> (the IPAddress must be located at the beginning or the end of the folder name) e.g., 10.0.0.1, 10.0.0.1-DC1, Diag-10.0.0.1
			|	| - nodetool -- static folder name
			|	|	| - cfstats 	-- This must be the output file from nodetool cfstats (static name)
			|	|	| - ring		-- This must be the output file from nodetool ring (static name)
			|	|	| - tpstats
			|	|	| - info
			|	|	| - compactionhistory
			|	| - logs -- static folder name
			|	|	| - Cassandra -- static folder name
			|	|	|	| - system.log -- This must be the Cassandra log file from the node
			| - <NextDSENodeIPAddress> -- e.g., 10.0.0.2, 10.0.0.2-DC1, Diag-10.0.0.2	
	IndivFiles -- All diagnostic files are located directly under diagnosticPath folder. Each file should have the IP address either in the beginning or end of the file name.
    			e.g., cfstats_10.192.40.7, system-10.192.40.7.log, 10.192.40.7_system.log, etc.
	NodeSubFldStruct -- Each file is within a folder where the Node's IP Adress (prefixed or suffix) is within the folder name.
				All files within this folder are prefixed by the command (e.g., dsetool, nodetool, etc.) followed by the command's subcommand/action. Logs and configuration files are just their associated file name (e.g., system.log).
				Example: 
				<MySpecialFolder> -- this is the location used for the diagnosticPath variable
					| - 10.0.0.1 -- IPAdress is the folder name (IPAdress can be prefixed in the name (e.g., 10.0.0.1-MyFolderName)
					|	| - nodetool.ring
					|	| - nodetool.cfstats
					|	| - dsetool.ring
					|	| - cqlsh.describe.cql
					|	| - system.log
					|	| - cassandra.yaml		
```

Below settings are related to how aggregation is performed on the "Summary Log" worksheet. Below settings determine the aggregation period or buckets:

**LogStartDate** -- Only import log entries from this date/time. MinDate ('1/1/0001 00:00:00') will parse all entries which is the default.

**MaxNbrAchievedLogFiles** -- The maximum number of archived log files that are read per node. If the value is -1 (default), all file are read (disabled).

**CLogLineFormatPosition|-o** -- Defines how the C* Log line format layout is parsed where (zero based index) IndicatorPos is the position of the log indicator (e.g., INFO, WARN, ERROR), TaskPos is task value (e.g., [SharedPool-Worker-3]), ItemPos position (e.g., Message.java:53), TimeStampPos is the date/time position (e.g., 2016-10-01 19:20:14,415), and DescribePos is the beginning of the describe (e.g., - Unexpected exception during request;).  The default is {IndicatorPos:0, TaskPos:1 , ItemPos:4, TimeStampPos:2, DescribePos:5}"


Below settings are used for parsing of diagnostic fles and creation of the Excel worksheets/workbooks:

**ParsingExcelOptions|-E** -- A list of parsing and Excel workbook and worksheet creation options (flags). Multiple options should be separated by a comma (,) or can be proceed by an plus/minus sign to add or remove options from the default. The options are split into "Parse" and "Produce" actions. "Parse" actions are used to parse certain segements of the diagnostic files. "Produce" actions are used to create/generate the corresponding Excel workbooks/worksheets. Typically, you specify the “Produce” actions and the corresponding “Parse” actions are selected by the application. The default is "ParseLoadWorksheets" (unless changed in the aplication config file). Below are the options:
```
  ParseCFStatsFiles         -- Enables nodetool CFStats file parsing which is used by the analysis worksheets
  ParseTPStatsFiles         -- Enables nodetool TPStats file parsing which is used by the analysis worksheets
  ParseCompacationHistFiles -- Enables nodetool compacation history file parsing
  ParseDDLFiles             -- Enables DDL (cqlsh describe) file processing
  ParseTblHistogramFiles    -- Enables nodetool Table/CF Histogram file processing which is used by the analysis worksheets.
                                  Note: This option is important for proper analysis
  ParseRingInfoFiles        -- Enables nodetool/dsetool ring file processing.
                                  Note: This option is important for proper data center related information and is recommended to be enabled.
  ParseMachineInfoFiles     -- Enables machine/OS (created by OpsCenter) file parsing
  ParseYamlFiles            -- Enables yaml/configuration file parsing
  ParseSummaryLogs          -- Performs an analysis of the log files and produces a summary error/exception event worksheet.
                                  Note: LogParsingExcelOptions of Parse, ParseArchivedLogs, and/or Detect are required to be enabled.
  OnlyOverlappingDateRanges -- Logs are only summarized so that only overlapping time ranges are used based on the timestamp ranges found in every node's logs. This is only valid if ParseSummaryLogs is enabled.
                                  Note: This option is important for proper analysis
  ParseSummaryLogsOnlyOverlappingDateRanges** = ParseSummaryLogs | OnlyOverlappingDateRanges
  ParseOpsCenterFiles       -- Enables parsing of OpsCenter diagnostic files associated with node related information
  ParseCFStatsLogs          -- Performs an analysis of the log files and produces a worksheet related to keyspace/table events.
                                  Note: LogParsingExcelOptions of Parse, ParseArchivedLogs, and/or Detect is required to be enabled. This option is important for proper analysis
  ParseNodeStatsLogs        -- Performs an analysis of the log files and produces a worksheet related to node events.
                                  Note: LogParsingExcelOptions of Parse, ParseArchivedLogs, and/or Detect is required to be enabled. This option is important for proper analysis
  LoadWorkSheets            -- Enables the creation of analysis worksheets produced by the parsing options (e.g., ParseNodeStatsLogs, ParseCFStatsLogs, ParseYamlFiles, etc.).
  LoadSummaryWorkSheets     -- Enables the creation of the log summary worksheet produced by the ParseSummaryLogs option
                                  Note: This can be disabled and ProduceSummaryWorkbook is enabled to create a detail log exception/error workbook
  ProduceSummaryWorkbook    -- Enables the creation of the log summary exception/error Excel workbook. This workbook can be used to reconcile the summary worksheet.
  ProduceStatsWorkbook      -- Enables the creation of the node/table stats/information Excel workbook based on the analysis of the log files. This workbook can be used to reconcile the analysis worksheet.
  Detect                    -- If enabled, this will determine the required ParsingExcelOptions, LogParsingExcelOptions, and application settings based on the "Parse" and "Produce" settings.
  LoadAllWorkSheets = LoadWorkSheets | LoadSummaryWorkSheets
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
  ParseLoadOnlySummaryLogs = ParseSummaryLogsOnlyOverlappingDateRanges | LoadSummaryWorkSheets | ProduceSummaryWorkbook
```

**LogParsingExcelOption|-L** -- A list of options around how logs are parsed and if seperate Ecel workbooks are created. Multiple options should be separated by a comma (,) or can be proceed by an plus/minus sign to add or remove options from the default. The default is "Detect" (unless changed in the aplication config file). Below are the options:
```
  Detect            -- If enabled, the log settings will be determined based on ParsingExcelOptions settings (above).
  Parse             -- If enabled, current log files will be included for parsing
  ParseArchivedLogs -- If enabled, archived log files will be included for parsing
  CreateWorkbook    -- If enabled, the parsed log files will be captured into separate Excel workbooks.
  ParseLogs = Parse | ParseArchivedLogs,
  ParseCreateAll = Parse | CreateWorkbook | ParseArchivedLogs,
  ParseCreateOnlyCurrentLogs = Parse | CreateWorkbook
```
Below settings are used for processing of Excel worksheets/workbooks:

**MaxRowInExcelWorkSheet** -- The maximum number of Excel rows in an Log or stats individual worksheet (not the main workbook). If this limit is reached a new worksheet is created. Default 500,000

**MaxRowInExcelWorkBook** -- The maximum number of Excel rows in the whole log or stats workbook(s) (does not apply to the main workbook). If this limit is reached a new workbook is created. Default 1,000,000

**DivideWorksheetIfExceedMaxRows** -- 1 (Enables)/0 (Disables) this behavior. If enabled and the internal data table's row count exceeds the EPPlus's maximum number of rows for a worksheet, the rows are divided into multiple worksheets. The default is  1 (Enable)
	**Warning:** This can split the base worksheets in the main workbook that will result in the pivot tables containing incomplete information. 
			If MS-Excel 64-bit is being used, the divided worksheets can be merged into one base worksheet and refresh all pivot tables should fit this issue.
                   
**LogExcelWorkbookFilter|-F** -- This is a filter that is applied to only log entries when loading into Excel. The default is no value (null). See http://www.csharp-examples.net/dataview-rowfilter/ for more information. Below is an example of a filter:
    "[Timestamp] >= #2016-08-01 15:30:00#"
```    
    Filter Columns are:
  	  [Data Center], string, AllowDBNull
  	  [Node IPAddress], string
  	  [Timestamp], DateTime    	
```

**ExcelTemplateFilePath|-T** -- The location of the Diagnostic Analytic Excel template workbook file that is used to create the "main" workbook. This can be no value (null), no template will be used. Default is ".\dseTemplate.xlsx" (looks in the current directory for the file).

**ExcelFilePath|-P** -- The folder and file name of the "main" Excel workbook. Any additionally created workbooks (i.e., MaxRowInExcelWorkBook) are also placed into this folder. The default is "[DeskTop]\Test.xlsx"

Alternative Folder Locations. These are additional locations to find additional log or CQL/DDL files.

**AlternativeLogFilePath|-l** -- Additional file path that is used to parse log files where the IP address must be in the beginning or end of the file name. Wild cards in the path are supported. Default is no value (null). 

**AlternativeDDLFilePath|-d** -- Additional file path that is used to parse CQL/DDL files. Wild cards in the path are supported. Default is no value (null). 

**TableHistogramDirPath|-t** -- Directory of files that contain the results of a nodetool TableHistogram. The file names must have the node's IP address in the beginning or end of the name. If this argument is not provide, the 'DiagnosticPath' is searched looking for files with the string "TableHistogram" embedded in the name.


Other Commands:

**CreateDirStructForNodes|-x** -- This will create the OpsCenter directory structure using a list of IP4 node addresses separated by comma. If this is specified all other commands/arguments are ignored except for argument **DiagnosticPath** which is the location where the folders are created.
**Validate|-V** -- If defined the analysis will parse required options for validation purposes only

**Note** that any of the C# "Special Folder" values can be used in any of the path settings (just surround the name of the enumeration with square brackets, e.g., [DeskTop]\Test.xlsx). See https://msdn.microsoft.com/en-us/library/system.environment.specialfolder%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396 or https://ibboard.co.uk/Programming/mono-special-folders.html

Also, all command arguments that take a path string (e.g., --ExcelFilePath) will merge the argument against the default value. For example, "[DeskTop]\Test.xlsx" is the default (defined in the application config file) and "myDSEReview" is the argument to --ExcelFilePath, the resulting path used by the application would be "[DeskTop]\myDSEReview.xlsx".
