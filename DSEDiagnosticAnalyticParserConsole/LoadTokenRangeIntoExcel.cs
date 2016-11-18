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
        public static void LoadTokenRangeInfo(ExcelPackage excelPkg,
                                                DataTable dtTokenRange,
                                                string excelWorkSheetRingTokenRanges)
        {
            if (dtTokenRange.Rows.Count > 0)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetRingTokenRanges);

                WorkSheet(excelPkg,
                            excelWorkSheetRingTokenRanges,
                            dtTokenRange,
                            workSheet =>
                            {
                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                workSheet.View.FreezePanes(2, 1);
                                workSheet.Cells["A1:F1"].AutoFilter = true;
                                //workSheet.Cells["C:D"].Style.Numberformat.Format = "TEXT";
                                //workSheet.Cells["E:E"].Style.Numberformat.Format = "TEXT";
                                workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0.00";
                                workSheet.Cells.AutoFitColumns();
                            },
                            ParserSettings.TokenRangeWorksheetFilterSort);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetRingTokenRanges);
            }            
        }

    }
}
