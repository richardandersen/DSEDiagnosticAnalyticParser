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
        public static void LoadCompacationHistory(ExcelPackage excelPkg,
                                                    Common.Patterns.Collections.LockFree.Stack<DataTable> dtCompHistStack,
                                                    string excelWorkSheetCompactionHist)
        {
            Program.ConsoleExcelNonLog.Increment(excelWorkSheetCompactionHist);

            DTLoadIntoExcel.WorkSheet(excelPkg,
                                        excelWorkSheetCompactionHist,
                                        dtCompHistStack,
                                        workSheet =>
                                        {
                                            workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                            workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                            //workBook.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                            workSheet.Cells["E:E"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss";
                                            workSheet.Cells["F:F"].Style.Numberformat.Format = "#,###,###,##0";
                                            workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0.00";
                                            workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0";
                                            workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0.00";
                                            workSheet.Cells["K1"].AddComment("The notation means {sstables:rows}. For example {1:3, 3:1} means 3 rows were taken from one sstable (1:3) and 1 row taken from 3 (3:1) sstables, all to make the one sstable in that compaction operation.", "Rich Andersen");

                                            workSheet.View.FreezePanes(2, 1);
                                            workSheet.Cells["A1:J1"].AutoFilter = true;
                                            workSheet.Cells["A:J"].AutoFitColumns();
                                        },
                                        false,
                                        -1,
                                        ParserSettings.CompactionHistWorksheetFilterSort);

            Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetCompactionHist);
        }

    }
}
