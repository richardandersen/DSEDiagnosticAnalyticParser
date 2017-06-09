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
        public static void LoadKeySpaceDDL(ExcelPackage excelPkg,
                                            DataTable dtKeySpace,
                                            string excelWorkSheetDDLKeyspaces)
        {
            if (dtKeySpace.Rows.Count > 0)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetDDLKeyspaces);

                DTLoadIntoExcel.WorkSheet(excelPkg,
                                            excelWorkSheetDDLKeyspaces,
                                            dtKeySpace,
                                            workSheet =>
                                            {
                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
												//workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
												workSheet.Cells["D:D"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["E:E"].Style.Numberformat.Format = "#,###";
                                                workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###";
                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["J:J"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["N:N"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###";
												workSheet.Cells["P:P"].Style.Numberformat.Format = "#,###";

                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 5].FormulaR1C1 = string.Format("sum(E2:E{0})", dtKeySpace.Rows.Count);
                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 6].FormulaR1C1 = string.Format("sum(F2:F{0})", dtKeySpace.Rows.Count);
                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 5].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Thick;
                                                workSheet.Cells[dtKeySpace.Rows.Count + 2, 6].Style.Border.Top.Style = OfficeOpenXml.Style.ExcelBorderStyle.Thick;
                                               
                                                workSheet.View.FreezePanes(2, 1);
                                                workSheet.Cells["A1:P1"].AutoFilter = true;
                                                DTLoadIntoExcel.AutoFitColumn(workSheet, workSheet.Cells["A:P"]);                                                
                                            },
                                            ParserSettings.DDLKeyspaceWorksheetFilterSort);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetDDLKeyspaces);
            }
        }

    }
}
