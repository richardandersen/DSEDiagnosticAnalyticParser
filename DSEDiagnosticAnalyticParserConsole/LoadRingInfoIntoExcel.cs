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
                                                workSheet.Cells["A1:V1"].AutoFilter = true;
                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["H:H"].Style.Numberformat.Format = "##0.00%";
                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "0.00";
                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "0.00";
                                                //workSheet.Cells["K:K"].Style.Numberformat.Format = "d hh:mm";
                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
                                                workSheet.Cells["M:M"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "d hh:mm";
                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "d hh:mm";
                                                workSheet.Cells["P:P"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
                                                workSheet.Cells["Q:Q"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
                                                workSheet.Cells["R:R"].Style.Numberformat.Format = "d hh:mm";
                                                workSheet.Cells["S:S"].Style.Numberformat.Format = "d hh:mm";

                                                workSheet.Cells["U:U"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["V:V"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["W:W"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["X:X"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                //workSheet.InsertColumn(11, 1);
                                                //workSheet.Column(10).Hidden = true;
                                                //workSheet.Cells["J1"].Value = "Uptime (Days)";
                                                //workSheet.Cells["K1"].Value = "Uptime";
                                                //workSheet.Cells[string.Format("K2:K{0}", dtRingInfo.Rows.Count + 2)].FormulaR1C1 = "CONCATENATE(TEXT(FLOOR(J2,1),\"@\"),\" \",TEXT(J2,\"hh:mm:ss\"))";

                                                DTLoadIntoExcel.AutoFitColumn(workSheet);                                                
                                            },
                                            ParserSettings.RingInfoWorksheetFilterSort);

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetRingInfo);
            }
        }

    }
}
