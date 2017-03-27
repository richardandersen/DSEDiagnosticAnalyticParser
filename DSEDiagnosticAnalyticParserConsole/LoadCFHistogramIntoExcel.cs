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
        public static void LoadCFHistogram(ExcelPackage excelPkg,
                                                Task<DataTable> tskdtCFHistogram,
                                                string excelWorkSheetCFHistogram)
        {
            tskdtCFHistogram.Wait();

            var dtCFHistogram = tskdtCFHistogram.Result;

            if (dtCFHistogram.Rows.Count > 0)
            {
                Program.ConsoleExcelNonLog.Increment(excelWorkSheetCFHistogram);

                DTLoadIntoExcel.WorkSheet(excelPkg,
                                            excelWorkSheetCFHistogram,
                                            dtCFHistogram,
                                            workSheet =>
                                            {
                                                workSheet.Cells["1:1"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
                                                workSheet.Cells["1:1"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;
                                                //workSheet.Cells["1:1"].Style.Fill.BackgroundColor.SetColor(System.Drawing.Color.LightGray);
                                                workSheet.View.FreezePanes(2, 1);
                                                workSheet.Cells["A1:M1"].AutoFilter = true;
                                                workSheet.Cells["G:G"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["H:H"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["I:I"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["J:J"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["K:K"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["L:L"].Style.Numberformat.Format = "#,###,###,##0.00";
                                                workSheet.Cells["M:M"].Style.Numberformat.Format = "#,###,###,##0.00";

                                                DTLoadIntoExcel.AutoFitColumn(workSheet);                                                
                                            });

                Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetCFHistogram);
            }
            else
            {
                WorkSheetLoadColumnDefaults(excelPkg,
                                            excelWorkSheetCFHistogram,
                                            "F",
                                            2,
                                            ParserSettings.TablehistogramAttribs);
            }

        }

    }
}
