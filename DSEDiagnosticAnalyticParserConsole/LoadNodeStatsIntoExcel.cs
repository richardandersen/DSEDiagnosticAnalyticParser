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
        private static void LoadNodeStats(ExcelPackage excelPkg,
                                            Common.Patterns.Collections.LockFree.Stack<DataTable> dtNodeStatsStack,
                                            string excelWorkSheetNodeStats)
        {
            Program.ConsoleExcelNonLog.Increment(excelWorkSheetNodeStats);

            DTLoadIntoExcel.WorkBook(excelPkg,
                                        excelWorkSheetNodeStats,
                                        dtNodeStatsStack,
                                        workSheet =>
                                        {
                                            workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                            workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                            //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                            workSheet.Cells["E:L"].Style.Numberformat.Format = "#,###,###,##0";

                                            WorkSheetLoadColumnDefaults(workSheet, "D", ParserSettings.NodeStatsAttribs);

                                            workSheet.Cells["A1:L1"].AutoFilter = true;
                                            workSheet.Cells.AutoFitColumns();
                                            workSheet.View.FreezePanes(2, 1);
                                        },
                                        false,
                                        -1,
                                        new Tuple<string, string, DataViewRowState>(null, "[Data Center], [Node IPAddress]", DataViewRowState.CurrentRows));

            Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetNodeStats);
        }

    }
}
