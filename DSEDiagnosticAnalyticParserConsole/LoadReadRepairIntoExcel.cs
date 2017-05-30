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

												workSheet.Cells["I1:X1"].Style.WrapText = true;
												workSheet.Cells["I1:X1"].Merge = true;
												workSheet.Cells["I1:X1"].Value = "Read-Repair";
												workSheet.Cells["I1:I2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["X1:Y2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["Z1:AC1"].Style.WrapText = true;
												workSheet.Cells["Z1:AC1"].Merge = true;
												workSheet.Cells["Z1:AC1"].Value = "GC";
												workSheet.Cells["Z1:Z2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AC1:AC2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["AD1:AH1"].Style.WrapText = true;
												workSheet.Cells["AD1:AH1"].Merge = true;
												workSheet.Cells["AD1:AH1"].Value = "Compaction";
												workSheet.Cells["AD1:AD2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AH1:AH2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["AI1:AM1"].Style.WrapText = true;
												workSheet.Cells["AI1:AM1"].Merge = true;
												workSheet.Cells["AI1:AM1"].Value = "Memtable Flush";
												workSheet.Cells["AI1:AI2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AM1:AM2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

												workSheet.Cells["AN1:AO1"].Style.WrapText = true;
												workSheet.Cells["AN1:AO1"].Merge = true;
												workSheet.Cells["AN1:AO1"].Value = "Performance Warnings";
												workSheet.Cells["AN1:AN2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
												workSheet.Cells["AO1:AO2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                workSheet.Cells["AP1:AQ1"].Style.WrapText = true;
                                                workSheet.Cells["AP1:AQ1"].Merge = true;
                                                workSheet.Cells["AP1:AQ1"].Value = "Solr Duration (ms)";
                                                workSheet.Cells["AP1:AP2"].Style.Border.Left.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;
                                                workSheet.Cells["AQ1:AQ2"].Style.Border.Right.Style = OfficeOpenXml.Style.ExcelBorderStyle.Medium;

                                                //workSheet.View.FreezePanes(3, 1);

                                                workSheet.Cells["A:A"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";
												workSheet.Cells["H:H"].Style.Numberformat.Format = "mm/dd/yyyy hh:mm:ss.000";

												//Read Repair
												workSheet.Cells["J:J"].Style.Numberformat.Format = "d hh:mm:ss.000";
												workSheet.Cells["M:M"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["O:O"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["Q:Q"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["R:R"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["S:S"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["T:T"].Style.Numberformat.Format = "###,###,##0";
                                                workSheet.Cells["U:U"].Style.Numberformat.Format = "###,###,##0";
                                                workSheet.Cells["V:V"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["X:X"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["Y:Y"].Style.Numberformat.Format = "###,###,##0";

												//GC
												workSheet.Cells["Z:Z"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AA:AA"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["AB:AB"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["AC:AC"].Style.Numberformat.Format = "###,##0.0000";

												//Compaction
												workSheet.Cells["AD:AD"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AE:AE"].Style.Numberformat.Format = "###,##0.0000";
												workSheet.Cells["AF:AF"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AG:AG"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AH:AH"].Style.Numberformat.Format = "###,###,##0.0000";

												//Memtable Flush
												workSheet.Cells["AI:AI"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AJ:AJ"].Style.Numberformat.Format = "###,###,##0.0000";
												workSheet.Cells["AK:AK"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AL:AL"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AM:AM"].Style.Numberformat.Format = "###,###,##0.0000";

												//Performance Warnings
												workSheet.Cells["AN:AN"].Style.Numberformat.Format = "###,###,##0";
												workSheet.Cells["AO:AO"].Style.Numberformat.Format = "###,###,##0";

                                                //Solr
                                                workSheet.Cells["AP:AP"].Style.Numberformat.Format = "###,###,##0";
                                                workSheet.Cells["AO:AO"].Style.Numberformat.Format = "###,###,##0";

                                                //WorkSheetLoadColumnDefaults(workSheet, "F", ParserSettings.CFStatsAttribs);

                                                workSheet.Cells["A2:AO2"].AutoFilter = true;
                                                DTLoadIntoExcel.AutoFitColumn(workSheet, workSheet.Cells["A:A"], workSheet.Cells["C:M"], workSheet.Cells["P:P"], workSheet.Cells["R:AO"]);                                               												
												workSheet.Column(2).Width = 45; //b
                                                workSheet.Column(7).Width = 20; //g
                                                workSheet.Column(14).Width = 15; //n
                                                workSheet.Column(17).Width = 20; //q
                                            },
											ParserSettings.ReadRepairWorksheetFilterSort,
											"A2");

				Program.ConsoleExcelNonLog.TaskEnd(excelWorkSheetReadRepair);
			}
		}
	}
}
