using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class DTLoadIntoExcel
    {
        public static Task LoadCassandraLog(Task<DataTable> runLogParsingTask,
                                                string excelFilePath,
                                                string excelWorkSheetLogCassandra,                                                
                                                DateTimeRange logCassandraMaxMinTimestamp,
                                                int maxRowInExcelWorkBook,
                                                int maxRowInExcelWorkSheet,
                                                string logExcelWorkbookFilter)
        {
            var runLogToExcel = runLogParsingTask.ContinueWith(logTask =>
            {
                #region Load Actual Logs into Excel

                DifferentExcelWorkBook(excelFilePath,
                                                excelWorkSheetLogCassandra,
                                                logTask.Result,
                                                (stage, filePath, workSheetName, excelPackage, rowCount) =>
                                                {
                                                    switch (stage)
                                                    {
                                                        case WorkBookProcessingStage.PreProcess:
                                                            Program.ConsoleExcelLog.Increment(filePath);
                                                            break;
                                                        case WorkBookProcessingStage.PreLoad:
                                                            Program.ConsoleExcelLog.Increment(string.Format("{0} - {1}", workSheetName, filePath.FileName));
                                                            break;
                                                        case WorkBookProcessingStage.PreSave:
                                                            break;
                                                        case WorkBookProcessingStage.Saved:
                                                            Program.ConsoleExcelLog.TaskEnd(string.Format("{0} - {1}", workSheetName, filePath.FileName));
                                                            Program.ConsoleExcelWorkbook.Increment(filePath);
                                                            break;
                                                        case WorkBookProcessingStage.PostProcess:
                                                            Program.ConsoleExcelLog.Decrement(filePath);
                                                            break;
                                                        default:
                                                            break;
                                                    }                                                    
                                                },
                                                workSheet =>
                                                {
                                                    workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                    workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;

                                                    workSheet.Cells["A1:M1"].Style.WrapText = true;
                                                    workSheet.Cells["A1:M1"].Merge = true;
                                                    workSheet.Cells["A1:M1"].Value = string.IsNullOrEmpty(logExcelWorkbookFilter)
                                                                            ? string.Format("Log Timestamp range is from \"{0}\" ({3}) to \"{1}\" ({4}) ({2:d\\ hh\\:mm}).",
                                                                                                logCassandraMaxMinTimestamp.Min,
                                                                                                logCassandraMaxMinTimestamp.Max,
                                                                                                logCassandraMaxMinTimestamp.Max - logCassandraMaxMinTimestamp.Min,
                                                                                                logCassandraMaxMinTimestamp.Min.DayOfWeek,
                                                                                                logCassandraMaxMinTimestamp.Max.DayOfWeek)
                                                                                : logExcelWorkbookFilter;
                                                    workSheet.Cells["A1:M1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Left;


                                                    //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                    workSheet.View.FreezePanes(3, 1);

                                                    workSheet.Cells["C:C"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";

                                                    workSheet.Cells["A2:J2"].AutoFilter = true;
                                                    workSheet.Cells["A:F"].AutoFitColumns();
                                                    workSheet.Cells["I:J"].AutoFitColumns();
                                                },
                                                    maxRowInExcelWorkBook,
                                                    maxRowInExcelWorkSheet,
                                                    null,
                                                    "A2");
               
                #endregion
            },
            TaskContinuationOptions.AttachedToParent
                | TaskContinuationOptions.LongRunning
                | TaskContinuationOptions.OnlyOnRanToCompletion);

            return runLogToExcel;
        }

    }
}
