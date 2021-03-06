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
        public static void LoadCompacationHistory(ExcelPackage excelPkg,
                                                    DataTable dtCompHistMerge,
                                                    string excelWorkSheetCompactionHist)
        {
            if (dtCompHistMerge != null)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetCompactionHist);

                DTLoadIntoExcel.WorkSheet(excelPkg,
                                            excelWorkSheetCompactionHist,
                                            dtCompHistMerge,
                                            workSheet =>
                                            {
                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                workSheet.Cells["F:F"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
                                                workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###,###,##0.000";
                                                workSheet.Cells["L1"].AddComment("A positive value indicates an increase in size and negative a decrease in size between \"before size\" and \"after size\".", "Rich Andersen");
                                                workSheet.Cells["N1"].AddComment("The notation means {sstables:rows}. For example {1:3, 3:1} means 3 rows were taken from one sstable (1:3) and 1 row taken from 3 (3:1) sstables, all to make the one sstable in that compaction operation.", "Rich Andersen");

                                                workSheet.View.FreezePanes(2, 1);
                                                workSheet.Cells["A1:M1"].AutoFilter = true;
                                                DTLoadIntoExcel.AutoFitColumn(workSheet, workSheet.Cells["A:N"]);
                                            },
                                            null,
                                            "A1",
                                            false);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetCompactionHist);
            }
        }

    }
}
