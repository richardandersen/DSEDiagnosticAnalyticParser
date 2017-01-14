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
        public static Task LoadStatusLog(Task<DataTable> runStatsLogMerged,
                                            string excelFilePath,
                                            string excelWorkSheetStatusLogCassandra,
                                            DateTimeRange logCassandraMaxMinTimestamp,
                                            int maxRowInExcelWorkBook,
                                            int maxRowInExcelWorkSheet,
											string logExcelWorkbookFilter)
        {
            var runStatusLogToExcel = runStatsLogMerged.ContinueWith(logTask =>
            {
				#region Load Status Logs into Excel
				runStatsLogMerged.Wait();

				if (runStatsLogMerged.Result != null)
				{
					DifferentExcelWorkBook(excelFilePath,
												excelWorkSheetStatusLogCassandra,
												runStatsLogMerged.Result,
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

													workSheet.Cells["H1:N1"].Style.WrapText = true;
													workSheet.Cells["H1:N1"].Merge = true;
													workSheet.Cells["H1:N1"].Value = "GC";
													workSheet.Cells["H1:H2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
													workSheet.Cells["N1:N2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

													workSheet.Cells["O1:S1"].Style.WrapText = true;
													workSheet.Cells["O1:S1"].Merge = true;
													workSheet.Cells["O1:S1"].Value = "Pool";
													workSheet.Cells["O1:O2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
													workSheet.Cells["S1:S2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

													workSheet.Cells["T1:V1"].Style.WrapText = true;
													workSheet.Cells["T1:V1"].Merge = true;
													workSheet.Cells["T1:V1"].Value = "Cache";
													workSheet.Cells["T1:T2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
													workSheet.Cells["V1:V2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

													workSheet.Cells["W1:X1"].Style.WrapText = true;
													workSheet.Cells["W1:X1"].Merge = true;
													workSheet.Cells["W1:X1"].Value = "Column Family";
													workSheet.Cells["W1:W2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
													workSheet.Cells["X1:X2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

													workSheet.Cells["Y1:AE1"].Style.WrapText = true;
													workSheet.Cells["Y1:AE1"].Merge = true;
													workSheet.Cells["Y1:AE1"].Value = "Compaction";
													workSheet.Cells["Y1:Y2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
													workSheet.Cells["AE1:AE2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

													workSheet.Cells["AF1:AQ1"].Style.WrapText = true;
													workSheet.Cells["AF1:AQ1"].Merge = true;
													workSheet.Cells["AF1:AQ1"].Value = "Read Repair";
													workSheet.Cells["AF1:AF2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
													workSheet.Cells["AQ1:AQ2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

													workSheet.Cells["B:B"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
													workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["O:S"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["W:W"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["I:N"].Style.Numberformat.Format = "#,###,###,##0.00";
													workSheet.Cells["T:T"].Style.Numberformat.Format = "#,###,###,##0.0000";
													workSheet.Cells["U:U"].Style.Numberformat.Format = "#,###,###,##0.00";
													workSheet.Cells["Z:Z"].Style.Numberformat.Format = "#,###,###,##0.00";
													workSheet.Cells["X:X"].Style.Numberformat.Format = "#,###,###,##0.00";
													workSheet.Cells["Y:Y"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["AB:AB"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["Z:Z"].Style.Numberformat.Format = "#,###,###,##0.00";
													workSheet.Cells["AA:AA"].Style.Numberformat.Format = "#,###,###,##0.00";
													workSheet.Cells["AC:AC"].Style.Numberformat.Format = "#,###,###,##0.00";
													workSheet.Cells["AJ:AJ"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["AK:AK"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["AL:AL"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["AM:AM"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["AN:AN"].Style.Numberformat.Format = "#,###,###,##0";
													workSheet.Cells["AP:AP"].Style.Numberformat.Format = "#,###,###,##0";

													workSheet.Cells["AE2"].AddComment("The notation means {sstables:rows}. For example {1:3, 3:1} means 3 rows were taken from one sstable (1:3) and 1 row taken from 3 (3:1) sstables, all to make the one sstable in that compaction operation.", "Rich Andersen");
													workSheet.Cells["Y2"].AddComment("Number of SSTables Compacted", "Rich Andersen");
													workSheet.Cells["AD2"].AddComment("Number of Partitions Merged to", "Rich Andersen");

													workSheet.Cells["A2:AQ2"].AutoFilter = true;
													workSheet.Cells.AutoFitColumns();
												},
											maxRowInExcelWorkBook,
											maxRowInExcelWorkSheet,
											null,
											"A2");
				}
                #endregion
            },
            TaskContinuationOptions.AttachedToParent
                | TaskContinuationOptions.LongRunning
                | TaskContinuationOptions.OnlyOnRanToCompletion);

            return runStatusLogToExcel;
        }

    }
}
