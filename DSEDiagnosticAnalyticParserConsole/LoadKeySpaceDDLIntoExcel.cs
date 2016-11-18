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
                                                workSheet.View.FreezePanes(2, 1);
                                                workSheet.Cells["A1:E1"].AutoFilter = true;
                                                workSheet.Cells["A:E"].AutoFitColumns();
                                            },
                                            ParserSettings.DDLKeyspaceWorksheetFilterSort);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetDDLKeyspaces);
            }
        }

    }
}
