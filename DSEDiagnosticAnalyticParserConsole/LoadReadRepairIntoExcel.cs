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
		public static void LoadReadRepair(Task<DataTable> runReadRepairTask,
											ExcelPackage excelPkg,
											string excelWorkSheetReadRepair)
		{
			if(runReadRepairTask.Result != null)
			{
				Program.ConsoleExcelNonLog.Increment(excelWorkSheetReadRepair);

				DTLoadIntoExcel.WorkSheet(excelPkg,
											excelWorkSheetReadRepair,
											runReadRepairTask.Result,
											workSheet =>
											{
												workSheet.Cells["1:2"].Style.Fill.PatternType = OfficeOpenXml.Style.ExcelFillStyle.LightGray;
												workSheet.Cells["1:2"].Style.HorizontalAlignment = OfficeOpenXml.Style.ExcelHorizontalAlignment.Center;

												workSheet.Cells["I1:U1"].Style.WrapText = true;
												workSheet.Cells["I1:U1"].Merge = true;
												workSheet.Cells["I1:U1"].Value = "Read-Repair";
												workSheet.Cells["I1:I2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["U1:U2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["V1:Y1"].Style.WrapText = true;
												workSheet.Cells["V1:Y1"].Merge = true;
												workSheet.Cells["V1:Y1"].Value = "GC";
												workSheet.Cells["V1:V2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["Y1:Y2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["Z1:AD1"].Style.WrapText = true;
												workSheet.Cells["Z1:AD1"].Merge = true;
												workSheet.Cells["Z1:AD1"].Value = "Compaction";
												workSheet.Cells["Z1:Z2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AD1:AD2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.View.FreezePanes(3, 1);

												workSheet.Cells["A:A"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
												workSheet.Cells["H:H"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
												workSheet.Cells["J:J"].Style.Numberformat.Format = "d hh:mm:ss.000";
												workSheet.Cells["M:M"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["O:O"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["P:P"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["Q:Q"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["R:R"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["V:V"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["W:W"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["X:X"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["Y:Y"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["Z:Z"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AA:AA"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["AB:AB"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AC:AC"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AD:AD"].Style.Numberformat.Format = "###,###,##0.0000";
												workSheet.Cells["AE:AE"].Style.Numberformat.Format = "###,###,##0";

												//WorkSheetLoadColumnDefaults(workSheet, "F", ParserSettings.CFStatsAttribs);

												workSheet.Cells["A2:AF2"].AutoFilter = true;
												workSheet.Cells.AutoFitColumns();
												workSheet.Column(2).Width = 45;
											},
											ParserSettings.ReadRepairWorksheetFilterSort,
											"A2");

				Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetReadRepair);
			}
		}
	}
}
