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
        private static void LoadSummaryLog(ExcelPackage excelPkg,
                                            DataTable dtLogSummary,
                                            string excelWorkSheetSummaryLogCassandra,
                                            DateTimeRange logCassandraMaxMinTimestamp,                                            
                                            TimeSpan logTimeSpanRange,
                                            string logExcelWorkbookFilter)
        {
            Program.ConsoleExcelNonLog.Increment(excelWorkSheetSummaryLogCassandra);

            DTLoadIntoExcel.WorkBook(excelPkg,
                                        excelWorkSheetSummaryLogCassandra,
                                        dtLogSummary,
                                        workSheet =>
                                        {                                           
                                            workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                            workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                            //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);

                                            workSheet.Cells["A1:H1"].Style.WrapText = true;
                                            workSheet.Cells["A1:H1"].Merge = true;
                                            workSheet.Cells["A1:H1"].Value = string.IsNullOrEmpty(logExcelWorkbookFilter)
                                                                                ? string.Format("Log Timestamp range is from \"{0}\" ({3}) to \"{1}\" ({4}) ({2:d\\ hh\\:mm}).",
                                                                                                    logCassandraMaxMinTimestamp.Min,
                                                                                                    logCassandraMaxMinTimestamp.Max,
                                                                                                    logCassandraMaxMinTimestamp.Max - logCassandraMaxMinTimestamp.Min,
                                                                                                    logCassandraMaxMinTimestamp.Min.DayOfWeek,
                                                                                                    logCassandraMaxMinTimestamp.Max.DayOfWeek)
                                                                                    : logExcelWorkbookFilter;
                                            workSheet.Cells["A1:H1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Left;

                                            workSheet.Cells["A:A"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm";
                                            workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0";
                                            workSheet.Cells["B:B"].Style.Numberformat.Format = "d hh:mm";

                                            workSheet.View.FreezePanes(3, 1);
                                            workSheet.Cells["A2:H2"].AutoFilter = true;
                                            workSheet.Cells.AutoFitColumns();
                                        },                                        
                                        new Tuple<string, string, DataViewRowState>(null,
                                                                                       "[Timestamp Period] DESC, [Data Center], [Associated Item], [Value]",
                                                                                    DataViewRowState.CurrentRows),
                                        "A2");
            Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetSummaryLogCassandra);
        }

    }
}
