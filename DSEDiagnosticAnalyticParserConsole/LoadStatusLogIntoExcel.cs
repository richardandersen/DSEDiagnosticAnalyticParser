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
        public static Task LoadStatusLog(Task<DataTable> runLogParsingTask,
                                            Common.Patterns.Collections.LockFree.Stack<DataTable> dtLogStatusStack,
                                            string excelFilePath,
                                            string excelWorkSheetStatusLogCassandra,                                            
                                            DateTimeRange logCassandraMaxMinTimestamp,
                                            int maxRowInExcelWorkBook,
                                            int maxRowInExcelWorkSheet,
                                            string logExcelWorkbookFilter)
        {
            var runStatusLogToExcel = runLogParsingTask.ContinueWith(logTask =>
            {
                #region Load Status Logs into Excel

                DifferentExcelWorkBook(excelFilePath,
                                                excelWorkSheetStatusLogCassandra,
                                                dtLogStatusStack,
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
                                                            Program.ConsoleExcelLogStatus.Increment(targetFilePath);
                                                            break;
                                                        case WorkBookProcessingStage.PreSave:
                                                            break;
                                                        case WorkBookProcessingStage.Saved:
                                                            Program.ConsoleExcelLogStatus.TaskEnd(targetFilePath);
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
                                                    //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                    workSheet.View.FreezePanes(3, 1);

                                                    workSheet.Cells["A1:F1"].Style.WrapText = true;
                                                    workSheet.Cells["A1:F1"].Merge = true;
                                                    workSheet.Cells["A1:F1"].Value = string.IsNullOrEmpty(logExcelWorkbookFilter)
                                                                                        ? string.Format("Log Timestamp range is from \"{0}\" ({3}) to \"{1}\" ({4}) ({2:d\\ hh\\:mm}).",
                                                                                                        logCassandraMaxMinTimestamp.Min,
                                                                                                        logCassandraMaxMinTimestamp.Max,
                                                                                                        logCassandraMaxMinTimestamp.Max - logCassandraMaxMinTimestamp.Min,
                                                                                                        logCassandraMaxMinTimestamp.Min.DayOfWeek,
                                                                                                        logCassandraMaxMinTimestamp.Max.DayOfWeek)
                                                                                        : logExcelWorkbookFilter;
                                                    workSheet.Cells["A1:F1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Left;

                                                    workSheet.Cells["G1:M1"].Style.WrapText = true;
                                                    workSheet.Cells["G1:M1"].Merge = true;
                                                    workSheet.Cells["G1:M1"].Value = "GC";
                                                    workSheet.Cells["G1:G2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                    workSheet.Cells["M1:M2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                    workSheet.Cells["N1:R1"].Style.WrapText = true;
                                                    workSheet.Cells["N1:R1"].Merge = true;
                                                    workSheet.Cells["N1:R1"].Value = "Pool";
                                                    workSheet.Cells["N1:N2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                    workSheet.Cells["R1:R2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                    workSheet.Cells["S1:U1"].Style.WrapText = true;
                                                    workSheet.Cells["S1:U1"].Merge = true;
                                                    workSheet.Cells["S1:U1"].Value = "Cache";
                                                    workSheet.Cells["S1:S2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                    workSheet.Cells["U1:U2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                    workSheet.Cells["V1:W1"].Style.WrapText = true;
                                                    workSheet.Cells["V1:W1"].Merge = true;
                                                    workSheet.Cells["V1:W1"].Value = "Column Family";
                                                    workSheet.Cells["V1:V2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                    workSheet.Cells["W1:W2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                    workSheet.Cells["X1:AD"].Style.WrapText = true;
                                                    workSheet.Cells["X1:AD1"].Merge = true;
                                                    workSheet.Cells["X1:AD1"].Value = "Compaction";
                                                    workSheet.Cells["X1:X2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                    workSheet.Cells["AD1:AD2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                    workSheet.Cells["A:A"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
                                                    workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0";
                                                    workSheet.Cells["N:R"].Style.Numberformat.Format = "#,###,###,##0";
                                                    workSheet.Cells["V:V"].Style.Numberformat.Format = "#,###,###,##0";
                                                    workSheet.Cells["H:M"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                    workSheet.Cells["S:T"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                    workSheet.Cells["Y:Y"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                    workSheet.Cells["W:W"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                    workSheet.Cells["X:X"].Style.Numberformat.Format = "#,###,###,##0";
                                                    workSheet.Cells["AA:AA"].Style.Numberformat.Format = "#,###,###,##0";
                                                    workSheet.Cells["Y:Y"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                    workSheet.Cells["Z:Z"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                    workSheet.Cells["AB:AB"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                    workSheet.Cells["AD1"].AddComment("The notation means {sstables:rows}. For example {1:3, 3:1} means 3 rows were taken from one sstable (1:3) and 1 row taken from 3 (3:1) sstables, all to make the one sstable in that compaction operation.", "Rich Andersen");
                                                    workSheet.Cells["X1"].AddComment("Number of SSTables Compacted", "Rich Andersen");
                                                    workSheet.Cells["AC1"].AddComment("Number of Partitions Merged to", "Rich Andersen");

                                                    workSheet.Cells["A2:AD2"].AutoFilter = true;
                                                    workSheet.Cells.AutoFitColumns();
                                                },
                                            maxRowInExcelWorkBook,
                                            maxRowInExcelWorkSheet,
                                            new Tuple<string, string, DataViewRowState>(logExcelWorkbookFilter,
                                                                                            "[Data Center], [Timestamp] DESC",
                                                                                        DataViewRowState.CurrentRows),
                                            "A2");
                #endregion
            },
            TaskContinuationOptions.AttachedToParent
                | TaskContinuationOptions.LongRunning
                | TaskContinuationOptions.OnlyOnRanToCompletion);

            return runStatusLogToExcel;
        }

    }
}
