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

												workSheet.View.FreezePanes(2, 1);
                                                workSheet.Cells["A1:O1"].AutoFilter = true;
                                                workSheet.Cells["A:O"].AutoFitColumns();
                                            },
                                            ParserSettings.DDLKeyspaceWorksheetFilterSort);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetDDLKeyspaces);
            }
        }

    }
}
