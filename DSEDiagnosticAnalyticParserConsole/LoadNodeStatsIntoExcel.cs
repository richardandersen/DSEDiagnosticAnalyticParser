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
        public static void LoadNodeStats(ExcelPackage excelPkg,
                                            DataTable dtNodeStats,
                                            string excelWorkSheetNodeStats)
        {
            if (dtNodeStats != null && dtNodeStats.Rows.Count > 0)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetNodeStats);

                DTLoadIntoExcel.WorkSheet(excelPkg,
                                            excelWorkSheetNodeStats,
                                            dtNodeStats,
                                            workSheet =>
                                            {
                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                workSheet.Cells["E:L"].Style.Numberformat.Format = "#,###,###,##0";
												workSheet.Cells["M:Q"].Style.Numberformat.Format = "#,###,###,##0.00";

												WorkSheetLoadColumnDefaults(workSheet, "D", ParserSettings.NodeStatsAttribs);

                                                workSheet.Cells["A1:Q1"].AutoFilter = true;
                                                DTLoadIntoExcel.AutoFitColumn(workSheet, workSheet.Cells["A1:Q1"]);                                                
                                                workSheet.View.FreezePanes(2, 1);
                                            },
                                            ParserSettings.NodeStatsWorksheetFilterSort);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetNodeStats);
            }
        }

    }
}
