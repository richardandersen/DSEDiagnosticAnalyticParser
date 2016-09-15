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
        private static void LoadOSMachineInfo(ExcelPackage excelPkg,
                                               DataTable dtOSMachineInfo,
                                               string excelWorkSheetOSMachineInfo)
        {
            if (dtOSMachineInfo.Rows.Count > 0)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetOSMachineInfo);

                DTLoadIntoExcel.WorkBook(excelPkg,
                                            excelWorkSheetOSMachineInfo,
                                            dtOSMachineInfo,
                                            workSheet =>
                                            {
                                                workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                workSheet.View.FreezePanes(3, 1);

                                                workSheet.Cells["J1:M1"].Style.WrapText = true;
                                                workSheet.Cells["J1:M1"].Merge = true;
                                                workSheet.Cells["J1:M1"].Value = "CPU Load (Percent)";
                                                workSheet.Cells["J1:J2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                workSheet.Cells["M1:M2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                workSheet.Cells["N1:S1"].Style.WrapText = true;
                                                workSheet.Cells["N1:S1"].Merge = true;
                                                workSheet.Cells["N1:S1"].Value = "Memory (MB)";
                                                workSheet.Cells["N1:N2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                workSheet.Cells["S1:S2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                workSheet.Cells["T1:X1"].Style.WrapText = true;
                                                workSheet.Cells["T1:X1"].Merge = true;
                                                workSheet.Cells["T1:X1"].Value = "Java";
                                                workSheet.Cells["T1:T2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                workSheet.Cells["X1:X2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;

                                                workSheet.Cells["Y1:AB1"].Style.WrapText = true;
                                                workSheet.Cells["Y1:AB1"].Merge = true;
                                                workSheet.Cells["Y1:AB1"].Value = "Java Non-Heap (MB)";
                                                workSheet.Cells["Y1:Y2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;
                                                workSheet.Cells["AB1:AB2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;

                                                workSheet.Cells["AC1:AF1"].Style.WrapText = true;
                                                workSheet.Cells["AC1:AF1"].Merge = true;
                                                workSheet.Cells["AC1:AF1"].Value = "Java Heap (MB)";
                                                workSheet.Cells["AC1:AC2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Dashed;
                                                workSheet.Cells["AF1:AF2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                workSheet.Cells["AG1:AK1"].Style.WrapText = true;
                                                workSheet.Cells["AG1:AK1"].Merge = true;
                                                workSheet.Cells["AG1:AK1"].Value = "Versions";
                                                workSheet.Cells["AG1:AG2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                workSheet.Cells["AK1:AK2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                workSheet.Cells["AL1:AS1"].Style.WrapText = true;
                                                workSheet.Cells["AL1:AS1"].Merge = true;
                                                workSheet.Cells["AL1:AS1"].Value = "NTP";
                                                workSheet.Cells["AL1:AL2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                workSheet.Cells["AS1:AS2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                workSheet.Cells["E:E"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["N:N"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["O:O"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["P:P"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["Q:Q"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["R:R"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["S:S"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["AL:AL"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["AM:AM"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["AN:AN"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["AO:AO"].Style.Numberformat.Format = "#,###,###,##0";
                                                workSheet.Cells["AP:AP"].Style.Numberformat.Format = "#,###,###,##0";

                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["X:X"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["Y:Y"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AA:AA"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AB:AB"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AC:AC"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AD:AD"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AE:AE"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AF:AF"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AQ:AQ"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AR:AR"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["AS:AS"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                workSheet.Cells["A2:AS2"].AutoFilter = true;
                                                workSheet.Cells.AutoFitColumns();
                                            },
                                            null,
                                            "A2");

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetOSMachineInfo);
            }
        }

    }
}
