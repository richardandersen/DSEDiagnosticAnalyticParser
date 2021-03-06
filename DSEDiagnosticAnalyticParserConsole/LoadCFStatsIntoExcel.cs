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
        public static void LoadCFStats(ExcelPackage excelPkg,
                                            DataTable dtCFStats,
                                            string excelWorkSheetCFStats)
        {
            if (dtCFStats != null && dtCFStats.Rows.Count > 0)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetCFStats);

                DTLoadIntoExcel.WorkSheet(excelPkg,
                                            excelWorkSheetCFStats,
                                            dtCFStats,
                                            workSheet =>
                                            {
                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                workSheet.Cells["I1"].AddComment("Change Numeric Format to Display Decimals", "Rich Andersen");
                                                workSheet.Cells["I1"].Value = workSheet.Cells["I1"].Text + "(Formatted)";
                                                workSheet.View.FreezePanes(2, 1);
                                                workSheet.Cells["A1:L1"].AutoFilter = true;
                                                //workSheet.Column(10).Hidden = true;

                                                WorkSheetLoadColumnDefaults(workSheet, "F", ParserSettings.CFStatsAttribs);

                                                DTLoadIntoExcel.AutoFitColumn(workSheet, workSheet.Cells["A:K"]);                                               
                                            });

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetCFStats);
            }
        }

    }
}
