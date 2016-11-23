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
        public static Task LoadCassandraExceptionSummaryLog(Task<Tuple<DataTable,DataTable,DateTimeRange>> runLogSummaryParsingTask,
                                                                string excelFilePath,
                                                                string excelWorkSheetLogCassandra)
        {
            var runLogToExcel = runLogSummaryParsingTask.ContinueWith(logTask =>
            {
                #region Load Actual Logs into Excel
                if (logTask.Result != null)
                {
                    DifferentExcelWorkBook(excelFilePath,
                                                excelWorkSheetLogCassandra,
                                                logTask.Result.Item2,
                                                (stage, orgFilePath, targetFilePath, workSheetName, excelPackage, excelDataTable, rowCount) =>
                                                {
                                                    switch (stage)
                                                    {
                                                        case WorkBookProcessingStage.PreProcess:
                                                            Program.ConsoleExcelLogStatus.Increment(string.Format("{0} - {1}", workSheetName, orgFilePath.FileName));
                                                            break;
                                                        case WorkBookProcessingStage.PrepareFileName:
                                                            targetFilePath.FileNameFormat = null;
                                                            targetFilePath.ReplaceFileName(string.Format("{0}-{1}",
                                                                                                            orgFilePath.FileNameWithoutExtension,
                                                                                                            workSheetName),
                                                                                                            targetFilePath.FileExtension);
                                                            break;
                                                        case WorkBookProcessingStage.PreLoad:
                                                            Program.ConsoleExcelLogStatus.Increment(targetFilePath);
                                                            break;
                                                        case WorkBookProcessingStage.PreSave:
                                                            break;
                                                        case WorkBookProcessingStage.Saved:
                                                            Program.ConsoleExcelLogStatus.TaskEnd(targetFilePath);
                                                            Program.ConsoleExcelWorkbook.Increment(targetFilePath);
                                                            break;
                                                        case WorkBookProcessingStage.PostProcess:
                                                            Program.ConsoleExcelLogStatus.Increment(string.Format("{0} - {1}", workSheetName, orgFilePath.FileName));
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
                                                    workSheet.Cells["A1:M1"].Value = //string.IsNullOrEmpty(logExcelWorkbookFilter)
                                                                                                            string.Format("Exception Summary Log Timestamp range is from \"{0}\" ({3}) to \"{1}\" ({4}) ({2:d\\ hh\\:mm}).",
                                                                                                                                runLogSummaryParsingTask.Result.Item3.Min,
                                                                                                                                runLogSummaryParsingTask.Result.Item3.Max,
                                                                                                                                runLogSummaryParsingTask.Result.Item3.Max - runLogSummaryParsingTask.Result.Item3.Min,
                                                                                                                                runLogSummaryParsingTask.Result.Item3.Min.DayOfWeek,
                                                                                                                                runLogSummaryParsingTask.Result.Item3.Max.DayOfWeek);
                                                    // : logExcelWorkbookFilter;
                                                    workSheet.Cells["A1:M1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Left;


                                                    //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                    workSheet.View.FreezePanes(3, 1);

                                                    workSheet.Cells["D:D"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";

                                                    workSheet.Cells["A2:K2"].AutoFilter = true;
                                                    workSheet.Cells["A:G"].AutoFitColumns();
                                                    workSheet.Cells["J:K"].AutoFitColumns();
                                                },
                                                    -1,
                                                    -1,
                                                    null,
                                                    "A2");
                }
               
                #endregion
            },
            TaskContinuationOptions.AttachedToParent
                | TaskContinuationOptions.LongRunning
                | TaskContinuationOptions.OnlyOnRanToCompletion);

            return runLogToExcel;
        }

    }
}
