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
                                                (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount) =>
                                                {
                                                    switch (stage)
                                                    {
                                                        case WorkBookProcessingStage.PreProcess:
                                                            Program.ConsoleExcelLog.Increment(string.Format("{0} - {1}", workSheetName, orgFilePath.FileName));
                                                            break;
                                                        case WorkBookProcessingStage.PrepareFileName:
                                                            targetFilePath.FileNameFormat = null;
                                                            targetFilePath.ReplaceFileName(string.Format("{0}-{1} {2:yyyy-MM-dd-HH-mm-ss} To {3:yyyy-MM-dd-HH-mm-ss}",
                                                                                                            orgFilePath.FileNameWithoutExtension,
                                                                                                            workSheetName,
                                                                                                            excelDataTable.Rows[0]["Timestamp"],
                                                                                                            excelDataTable.Rows[excelDataTable.Rows.Count - 1]["Timestamp"]),
                                                                                                            targetFilePath.FileExtension);
                                                            break;
                                                        case WorkBookProcessingStage.PreLoad:
                                                            Program.ConsoleExcelLog.Increment(targetFilePath);
                                                            break;
                                                        case WorkBookProcessingStage.PreSave:
                                                            break;
                                                        case WorkBookProcessingStage.Saved:
                                                            Program.ConsoleExcelLog.TaskEnd(targetFilePath);
                                                            Program.ConsoleExcelWorkbook.Increment(targetFilePath);
                                                            break;
                                                        case WorkBookProcessingStage.PostProcess:
                                                            Program.ConsoleExcelLog.Increment(string.Format("{0} - {1}", workSheetName, orgFilePath.FileName));
                                                            break;
                                                        default:
                                                            break;
                                                    }
                                                },
                                                workSheet =>
                                                {
                                                    workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                    workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;

                                                    workSheet.Cells["A1:N1"].Style.WrapText = true;
                                                    workSheet.Cells["A1:N1"].Merge = true;
                                                    workSheet.Cells["A1:N1"].Value = string.IsNullOrEmpty(logExcelWorkbookFilter)
                                                                                                            ? string.Format("Log Timestamp range is from \"{0}\" ({3}) to \"{1}\" ({4}) ({2:d\\ hh\\:mm}).",
                                                                                                                                logCassandraMaxMinTimestamp.Min,
                                                                                                                                logCassandraMaxMinTimestamp.Max,
                                                                                                                                logCassandraMaxMinTimestamp.Max - logCassandraMaxMinTimestamp.Min,
                                                                                                                                logCassandraMaxMinTimestamp.Min.DayOfWeek,
                                                                                                                                logCassandraMaxMinTimestamp.Max.DayOfWeek)
                                                                                                                : logExcelWorkbookFilter;
                                                    workSheet.Cells["A1:N1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Left;


                                                    //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                    workSheet.View.FreezePanes(3, 1);

                                                    workSheet.Cells["C:C"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";

                                                    workSheet.Cells["A2:K2"].AutoFilter = true;
                                                    workSheet.Cells["A:G"].AutoFitColumns();
                                                    workSheet.Cells["J:K"].AutoFitColumns();
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
