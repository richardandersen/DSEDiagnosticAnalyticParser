using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using OfficeOpenXml;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class DTLoadIntoExcel
    {
        public static void LoadSummaryLogCFNodeStats(Task<DataTable> runLogParsingTask,
                                                        Task<DataTable> runSummaryLogTask,
                                                        ExcelPackage excelPkg,                                                        
                                                        string excelWorkSheetSummaryLogCassandra,
                                                        DateTimeRange logCassandraMaxMinTimestamp,
                                                        DateTimeRange maxminMaxLogDate,
                                                        TimeSpan logTimeSpanRange,
                                                        string logExcelWorkbookFilter,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
                                                        string excelWorkSheetCFStats,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtNodeStatsStack,
                                                        string excelWorkSheetNodeStats)
        {
            runLogParsingTask?.Wait();
            
            LoadCFStats(excelPkg, dtCFStatsStack, excelWorkSheetCFStats);
            LoadNodeStats(excelPkg, dtNodeStatsStack, excelWorkSheetNodeStats);

            runSummaryLogTask?.Wait();

            LoadSummaryLog(excelPkg,
                            runSummaryLogTask.Result,
                            excelWorkSheetSummaryLogCassandra,
                            logCassandraMaxMinTimestamp,                            
                            logTimeSpanRange,
                            logExcelWorkbookFilter);            
        }

    }
}
