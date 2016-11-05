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
                                                        Task<Tuple<DataTable,DataTable>> runSummaryLogTask,
                                                        ExcelPackage excelPkg,                                                        
                                                        string excelWorkSheetSummaryLogCassandra,
                                                        DateTimeRange minmaxSummaryLogDate,
                                                        DateTimeRange minmaxLogDate,
                                                        DateTimeRange maxminMaxLogDate,                                                        
                                                        string logExcelWorkbookFilter,
                                                        Task<DataTable> cfMergeTableTask,
                                                        string excelWorkSheetCFStats,
                                                        Common.Patterns.Collections.LockFree.Stack<DataTable> dtNodeStatsStack,
                                                        string excelWorkSheetNodeStats,
                                                        DataTable dtTable,
                                                        string excelWorkSheetDDL)
        {
            runLogParsingTask?.Wait();
            cfMergeTableTask?.Wait();

            LoadCFStats(excelPkg, cfMergeTableTask.Result, excelWorkSheetCFStats);
            LoadNodeStats(excelPkg, dtNodeStatsStack, excelWorkSheetNodeStats);
            LoadTableDDL(excelPkg, dtTable, excelWorkSheetDDL);

            runSummaryLogTask?.Wait();

            if(runSummaryLogTask?.Result != null)
            {
                LoadSummaryLog(excelPkg,
                                runSummaryLogTask.Result.Item1,
                                excelWorkSheetSummaryLogCassandra,
                                minmaxLogDate,
                                minmaxSummaryLogDate,                                
                                logExcelWorkbookFilter);
            }        
        }

    }
}
