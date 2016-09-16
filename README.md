# DSEDiagnosticAnalyticParser
DSE Diagnostic Tar Ball Analytic Parser

This application will parse and perform analytics on a DataStax OpsCenter diagnostic tar ball. It will place this information into Excel workbooks for further analysis.
It optionally can use an Excel template to generate the "main" workbook. One is provided that includes some common analytics. 

The "Binaries" folder contains zip files of the complete set of required assemblies to run this application. This requires .Net 4.5.2 framework.


`DSEDiagnosticAnalyticParserConsole.exe` can optionally take a set of arguments. These arguments override the default settings defined in the application configuration settings files (DSEDiagnosticAnalyticParserConsole.exe.config). To list these arguments and associated documentation execute DSEDiagnosticAnalyticParserConsole with the `--help` flag (i.e., `DSEDiagnosticAnalyticParserConsole --help`). To display the arguments with the associated default values, use the `-?` flag. 

The application configuration setting file, DSEDiagnosticAnalyticParserConsole.exe.config, defines the default settings for the application and for the logger (log4net). Currently log4net is configured to overwrite the log file (DSEDiagnosticAnalyticParserConsole.log) on each run. This log file will contain all warning and error messages and should be reviewed after each run. Below is a description of the application settings:

**GCPausedFlagThresholdInMS** -- Defines a threshold, in milliseconds, that will flag GC related latencies in the C* log. Default 5000 

**CompactionFlagThresholdInMS** -- Defines a threshold, in milliseconds, that will flag compaction latencies in the C* log. Default 10000

**SlowLogQueryThresholdInMS** -- Defines a threshold, in milliseconds, that will flag query latencies in the C* log. Default 2000

**LogCurrentDate** -- The date/time used to start collecting C* log entries. Log entries greater than and equal this date/time will be collected. If no value (null), all entries will be collected. Default is no value (all entries).

**LogTimeSpanRange** -- Only valid if LogCurrentDate is defined. The time span from LogCurrentDate to collect the C* entries (e.g., the last 5 days from LogCurrentDate of 2016-09-15; From 2016-09-15 23:59:59 to 2016-09-10 00:00:00). Default 02:00:00 (2 days)

**LogMaxRowsPerNode** -- The maximum number of C* log entries that are read per log file node. -1 will allow all entries to be read. Default is -1. Note: If this is enabled (> 0), reading C* achieve logs are disabled.

**IgnoreKeySpaces** -- A collection of keyspaces that are ignored during parsing. Default is: dse_system, system_auth, system_traces, system, dse_perf

**ParseArchivedLogs** -- If enabled (true), any archieve C* logs are read. Default is true. Note: This option is only valid when LogMaxRowsPerNode is enabled (> 0) and DiagnosticNoSubFolders is false.

**ParseLogs** -- If true, log files are parsed. If false, no log files are parsed and any associated analytics will not be performed. Default is true. If false, archive log files are not parsed either.

**ParseNonLogs** -- If true, non-log files (e.g., tpstat, cfstat, machine-os, etc.) are parsed. If false, no non-log files are parsed and any associated analytics will not be performed. Default is true.

**DiagnosticPath** -- The path of the folder that contains the diagnostic files. This can be an absolute or a relative path. This is a required field. The default is "[MyDocuments]\DataStax\TestData\OpsCenter-diagnostics-2016_08_30_19_08_03_UTC". Note the structure of the content of this folder is dependent on the value of DiagnosticNoSubFolders. 

**DiagnosticNoSubFolders** -- This setting determines the structure of the diagnostic folder. Default is false. Below explains this setting:
  If diagnosticNoSubFolders is false:
    Directory where files are located to parse DSE diagnostics files produced by DataStax OpsCenter diagnostics or a special directory structure where DSE diagnostics information is placed.
    If the "special" directory is used it must follow the following structure:
```
      <MySpecialFolder> -- this is the location used for the diagnosticPath variable
        |- <DSENodeIPAddress> (the IPAddress must be located at the beginning or the end of the folder name) e.g., 10.0.0.1, 10.0.0.1-DC1, Diag-10.0.0.1
        |       | - nodetool -- static folder name
        |       |     | - cfstats 	-- This must be the output file from nodetool cfstats (static name)
        |       |     | - ring		-- This must be the output file from nodetool ring (static name)
        |       |     | - tpstats
        |       |     | - info
        |       |     | - compactionhistory
        |  	    | - logs -- static folder name
        |       | 	  | - Cassandra -- static folder name
        |  				    | - system.log -- This must be the Cassandra log file from the node
        | - <NextDSENodeIPAddress> -- e.g., 10.0.0.2, 10.0.0.2-DC1, Diag-10.0.0.2
  ```
  If diagnosticNoSubFolders is true:
    All diagnostic files are located directly under diagnosticPath folder. Each file should have the IP address either in the beginning or end of the file name.
    e.g., cfstats_10.192.40.7, system-10.192.40.7.log, 10.192.40.7_system.log, etc.

Below settings are related to how aggregation is performed on the "Summary Log" worksheet. Below settings determine the aggregation period or bucket:

**LogSummaryPeriods** -- Defines the period as a series of dates and time entries. Each entry is defined by two fields. First field (Item1) is the beginning date/time bucket and the second (Item2) is the aggregation of that time gor that bucket. The time ranges starts at this entry and ends at the next entry. Here is an example:
    [{"Item1":"2016-08-02T00:00:00","Item2":"00:30:00"},{"Item1":"2016-08-01T00:00:00","Item2":"1.00:00:00"},{"Item1":"2016-07-20T00:00:00","Item2":"7.00:00:00"}]
  This defines the following date/time ranges:
    Range 1 -- 2016-08-02 midnight to 2016-08-01 midnight (exclusive) where all log entries are aggregated every 30 mins.
    Range 2 -- 2016-08-01 midnight to 2016-07-20 midnight (exclusive) where all log entries are aggregated every 1 day.
    Range 3 -- 2016-07-20 midnight to remaining entries where all log entries are aggregated every 7 days.
    
  Default is no value (disabled). Note that either LogSummaryPeriods or LogSummaryPeriodRanges are set. 

**LogSummaryPeriodRanges** -- Defines the period as a series of time spans and time range entries. Each entry is defined by two fields. First field (Item1) is the time span from the most recent log entry or the last entry, and the second (Item2) is the aggregation of that time range. Here is an example:
    [{"Item1":"1.00:00:00","Item2":"00:15:00"},{"Item1":"2.00:00:00","Item2":"01:00:00"},{"Item1":"10.00:00:00","Item2":"7.00:00:00"}]
  This defines the following date/time ranges assuming the most recent log entry is 2016-09-12 10:45:
    Range 1 -- using most recent log for 1 day ==> 2016-09-12 10:45 to 2016-09-11 10:45 (exclusive) where all log entries are aggregated every 15 mins.
    Range 2 -- Using the ending date/time from Range 1 ==> 2016-09-11 10:45 to 2016-09-08 10:45 (exclusive) where all log entries are aggregated every 1 hour.
    Range 3 -- Using the ending date/time from Range 2 ==> 2016-09-08 10:45 to remaining entries where all log entries are aggregated every 7 days.
    
    Default is [{"Item1":"1.00:00:00","Item2":"00:15:00"},{"Item1":"1.00:00:00","Item2":"1.00:00:00"},{"Item1":"4.00:00:00","Item2":"7.00:00:00"}]. Note that either LogSummaryPeriods or LogSummaryPeriodRanges are set. 

Below settings are used for processing of the Excel worksheets/workbooks:

**MaxRowInExcelWorkSheet** -- The maximum number of Excel rows in an individual worksheet. If this limit is reached a new worksheet is created. Default 500,000

**MaxRowInExcelWorkBook** -- The maximum number of Excel rows in the whole workbook. If this limit is reached a new workbook is created. Default 1,000,000

**LogExcelWorkbookFilter** -- This is a filter that is applied to only log entries when loading into Excel. The default it no value (null). Below is an example of a filter:
    "[Timestamp] >= #2016-08-01#"
    
    Filter Columns are:
  	  [Data Center], string, AllowDBNull
  	  [Node IPAddress], string
  	  [Timestamp], DateTime
    	[Indicator], string (e.g., INFO, WARN, ERROR)
    	[Task], string (e.g., ReadStage, CompactionExecutor)
    	[Item], string (e.g., HintedHandoffMetrics.java, BatchStatement.java)
    	[Exception], string, AllowDBNull (e.g., java.io.IOException)
    	[Exception Description], string, AllowDBNull (e.g., "Caused by: java.io.IOException: Cannot proceed on repair because a neighbor (/10.27.34.54) is dead: session failed")
    	[Associated Item], string, AllowDBNull (e.g., 10.27.34.54, <keyspace.tablename>)  
    	[Associated Value], object, AllowDBNull (e.g., <size in MB>, <time in ms>)
    	[Description], string -- log's description
    	[Flagged], bool, AllowDBNull -- if true this log entry was flagged because it matched some criteria (e.g., GC Pauses -- GCInspector.java exceeds GCPausedFlagThresholdInMS)
  	
**LoadLogsIntoExcel** -- If true log entries are loaded into their own separate workbooks. Default is true. Note that if this is disabled (false), logs are still process if ParseLogs is true. If ParseLogs is false this option is ignored and no logs are loaded into Excel. 

**ExcelTemplateFilePath** -- The location of the Diagnostic Analytic Excel template workbook file that is used to create the "main" workbook. This can be no value (null), no template will be used. Default is ".\dseTemplate.xlsx" (looks in the current directory for the file).

**ExcelFilePath** -- The folder and file name of the "main" Excel workbook. Any additionally created workbooks (i.e., MaxRowInExcelWorkBook) are also placed into this folder. The default is "[DeskTop]\Test.xlsx"

Alternative Folder Locations. These are additional locations to find additional log or CQL/DDL files.

**AlternativeLogFilePath** -- Additional file path that is used to parse log files where the IP address must be in the beginning or end of the file name. Wild cards in the path are supported. Default is no value (null). 

**AlternativeDDLFilePath** -- Additional file path that is used to parse CQL/DDL files. Wild cards in the path are supported. Default is no value (null). 

**Note** that any of the C# "Special Folder" values can be used in any of the path settings (just surround the name of the emulator with square brackets, e.g., [DeskTop]). See https://msdn.microsoft.com/en-us/library/system.environment.specialfolder%28v=vs.110%29.aspx?f=255&MSPPError=-2147217396
