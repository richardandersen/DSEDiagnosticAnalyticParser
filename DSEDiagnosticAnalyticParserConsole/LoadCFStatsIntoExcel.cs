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
        private static void LoadCFStats(ExcelPackage excelPkg,
                                            Common.Patterns.Collections.LockFree.Stack<DataTable> dtCFStatsStack,
                                            string excelWorkSheetCFStats)
        {
            Program.ConsoleExcelNonLog.Increment(excelWorkSheetCFStats);

            DTLoadIntoExcel.WorkBook(excelPkg,
                                        excelWorkSheetCFStats,
                                        dtCFStatsStack,
                                        workSheet =>
                                        {
                                            workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                            workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                            //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                            workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0";

                                            workSheet.Cells["I1"].AddComment("Change Numeric Format to Display Decimals", "Rich Andersen");
                                            workSheet.Cells["I1"].Value = workSheet.Cells["I1"].Text + "(Formatted)";
                                            workSheet.View.FreezePanes(2, 1);
                                            workSheet.Cells["A1:I1"].AutoFilter = true;
                                            //workSheet.Column(10).Hidden = true; //J
                                            workSheet.Cells.AutoFitColumns();
                                        },
                                        false,
                                        -1,
                                        new Tuple<string, string, DataViewRowState>(null, "[Data Center], [Node IPAddress], [KeySpace], [Table]", DataViewRowState.CurrentRows));

            Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetCFStats);
        }

    }
}
