﻿using System;
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
        public static void LoadSummaryLog(ExcelPackage excelPkg,
                                            DataTable dtLogSummary,
                                            string excelWorkSheetSummaryLogCassandra,
                                            DateTimeRange minmaxLogDate,
                                            DateTimeRange minmaxSummaryDateRange,
                                            string logExcelWorkbookFilter)
        {
            Program.ConsoleExcelNonLog.Increment(excelWorkSheetSummaryLogCassandra);

            DTLoadIntoExcel.WorkSheet(excelPkg,
                                        excelWorkSheetSummaryLogCassandra,
                                        dtLogSummary,
                                        workSheet =>
                                        {
                                            workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                            workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                            //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);

                                            workSheet.Cells["A1:G1"].Style.WrapText = true;
                                            workSheet.Cells["A1:G1"].Merge = true;
                                            workSheet.Cells["A1:G1"].Value = string.IsNullOrEmpty(logExcelWorkbookFilter)
                                                                                ? string.Format("Log Timestamp range is from \"{0}\" ({3}) to \"{1}\" ({4}) ({2:d\\ hh\\:mm}).{5}",
                                                                                                    minmaxSummaryDateRange.Min,
                                                                                                    minmaxSummaryDateRange.Max,
                                                                                                    minmaxSummaryDateRange.Max - minmaxSummaryDateRange.Min,
                                                                                                    minmaxSummaryDateRange.Min.DayOfWeek,
                                                                                                    minmaxSummaryDateRange.Max.DayOfWeek,
                                                                                                    minmaxLogDate == minmaxSummaryDateRange ? string.Empty : " Note: Only Overlapping Date Ranges used")
                                                                                    : logExcelWorkbookFilter;
                                            workSheet.Cells["A1:G1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Left;

                                            if (minmaxLogDate != minmaxSummaryDateRange || !string.IsNullOrEmpty(logExcelWorkbookFilter))
                                            {
                                                workSheet.Cells["H1"].AddComment(string.Format("Complete Log range is from \"{0}\" ({3}) to \"{1}\" ({4}) ({2:d\\ hh\\:mm}).",
                                                                                                    minmaxLogDate.Min,
                                                                                                    minmaxLogDate.Max,
                                                                                                    minmaxLogDate.Max - minmaxLogDate.Min,
                                                                                                    minmaxLogDate.Min.DayOfWeek,
                                                                                                    minmaxLogDate.Max.DayOfWeek), "LogRange");
                                            }

                                            workSheet.Cells["A:A"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm";
											workSheet.Cells["J:J"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
											workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0";
                                            workSheet.Cells["B:B"].Style.Numberformat.Format = "d hh:mm";

                                            workSheet.View.FreezePanes(3, 1);
                                            workSheet.Cells["A2:N2"].AutoFilter = true;

                                            DTLoadIntoExcel.AutoFitColumn(workSheet, workSheet.Cells["A:E"], workSheet.Cells["J:K"]);
                                            
                                            workSheet.Column(6).Width = 45; //F
                                            workSheet.Column(7).Width = 38; //G
                                            workSheet.Column(8).Width = 5; //H
                                            workSheet.Column(9).Width = 16; //I
                                        },
                                        ParserSettings.SummaryLogWorksheetFilterSort,
                                        "A2");
            Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetSummaryLogCassandra);
        }

    }
}
