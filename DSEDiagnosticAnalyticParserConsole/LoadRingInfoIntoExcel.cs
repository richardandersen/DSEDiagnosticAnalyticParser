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
        private static void LoadRingInfo(ExcelPackage excelPkg,
                                            DataTable dtRingInfo,
                                            string excelWorkSheetRingInfo)
        {
            if (dtRingInfo.Rows.Count > 0)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetRingInfo);

                DTLoadIntoExcel.WorkSheet(excelPkg,
                                            excelWorkSheetRingInfo,
                                            dtRingInfo,
                                            workSheet =>
                                            {
                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                workSheet.View.FreezePanes(2, 1);
                                                workSheet.Cells["A1:O1"].AutoFilter = true;
                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["H:H"].Style.Numberformat.Format = "##0.00%";
                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "d hh:mm";

                                                workSheet.Cells.AutoFitColumns();
                                            },
                                            ParserSettings.RingInfoWorksheetFilterSort);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetRingInfo);
            }
        }

    }
}
