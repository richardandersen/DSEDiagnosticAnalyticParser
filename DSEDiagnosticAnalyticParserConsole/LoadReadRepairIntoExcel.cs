﻿using System;
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

												workSheet.Cells["I1:W1"].Style.WrapText = true;
												workSheet.Cells["I1:W1"].Merge = true;
												workSheet.Cells["I1:W1"].Value = "Read-Repair";
												workSheet.Cells["I1:I2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["W1:W2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["X1:AA1"].Style.WrapText = true;
												workSheet.Cells["X1:AA1"].Merge = true;
												workSheet.Cells["X1:AA1"].Value = "GC";
												workSheet.Cells["X1:X2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AA1:AA2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["AB1:AF1"].Style.WrapText = true;
												workSheet.Cells["AB1:AF1"].Merge = true;
												workSheet.Cells["AB1:AF1"].Value = "Compaction";
												workSheet.Cells["AB1:AB2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AF1:AF2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["AG1:AK1"].Style.WrapText = true;
												workSheet.Cells["AG1:AK1"].Merge = true;
												workSheet.Cells["AG1:AK1"].Value = "Memtable Flush";
												workSheet.Cells["AK1:AK2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AJ1:AJ2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.View.FreezePanes(3, 1);

												workSheet.Cells["A:A"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
												workSheet.Cells["H:H"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";

												//Read Repair
												workSheet.Cells["J:J"].Style.Numberformat.Format = "d hh:mm:ss.000";
												workSheet.Cells["M:M"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["O:O"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["P:P"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["Q:Q"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["R:R"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["S:S"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["T:T"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["V:V"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["W:W"].Style.Numberformat.Format = "###,###,##0";

												//GC
												workSheet.Cells["X:X"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["Y:Y"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["Z:Z"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["AA:AA"].Style.Numberformat.Format = "###,##0.0000";

												//Compaction
												workSheet.Cells["AB:AB"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AC:AC"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["AD:AD"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AE:AE"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AF:AF"].Style.Numberformat.Format = "###,###,##0.0000";

												//Memtable Flush
												workSheet.Cells["AG:AG"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AH:AH"].Style.Numberformat.Format = "###,###,##0.0000";
												workSheet.Cells["AI:AI"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AJ:AJ"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AK:AK"].Style.Numberformat.Format = "###,###,##0.0000";

												//WorkSheetLoadColumnDefaults(workSheet, "F", ParserSettings.CFStatsAttribs);

												workSheet.Cells["A2:AM2"].AutoFilter = true;
												workSheet.Cells.AutoFitColumns();
												workSheet.Column(2).Width = 45; //b
												workSheet.Column(14).Width = 15; //n
											},
											ParserSettings.ReadRepairWorksheetFilterSort,
											"A2");

				Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetReadRepair);
			}
		}
	}
}
