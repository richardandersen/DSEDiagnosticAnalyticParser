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
        static public ExcelRangeBase WorkSheet(ExcelPackage excelPkg,
                                                string workSheetName,
                                                System.Data.DataTable dtExcel,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                string startingWSCell = "A1")
        {
            Program.ConsoleExcel.Increment(string.Format("{0} - {1}", workSheetName, dtExcel.TableName));

            dtExcel.AcceptChanges();

            var dtErrors = dtExcel.GetErrors();
            if (dtErrors.Length > 0)
            {
                dtErrors.Dump(Logger.DumpType.Error, "Table \"{0}\" Has Error", dtExcel.TableName);
                Program.ConsoleErrors.Increment("Data Table has Errors");
            }

            if (dtExcel.Rows.Count == 0) return null;

            if (viewFilterSortRowStateOpts != null)
            {
                dtExcel = (new DataView(dtExcel,
                                        viewFilterSortRowStateOpts.Item1,
                                        viewFilterSortRowStateOpts.Item2,
                                        viewFilterSortRowStateOpts.Item3))
                                .ToTable();
            }

            if (dtExcel.Rows.Count == 0) return null;

            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            if (workSheet == null)
            {
                workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
            }
            else
            {
                workSheet.Cells.Clear();
                foreach (ExcelComment comment in workSheet.Comments.Cast<ExcelComment>().ToArray())
                {
                    workSheet.Comments.Remove(comment);
                }
            }

            if (viewFilterSortRowStateOpts == null || string.IsNullOrEmpty(viewFilterSortRowStateOpts.Item1))
            {
                Logger.Instance.InfoFormat("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\". Rows: {2:###,###,##0}", dtExcel.TableName, workSheet.Name, dtExcel.Rows.Count);
            }
            else
            {
                Logger.Instance.InfoFormat("Loading DataTable \"{0}\" into Excel WorkSheet \"{1}\" with Filter \"{2}\". Rows: {3:###,###,##0}",
                                                dtExcel.TableName, workSheet.Name, viewFilterSortRowStateOpts.Item1, dtExcel.Rows.Count);
            }

            var loadRange = workSheet.Cells[startingWSCell].LoadFromDataTable(dtExcel, true);

            if (loadRange != null && worksheetAction != null)
            {
                worksheetAction(workSheet);
            }

            Program.ConsoleExcel.TaskEnd(string.Format("{0} - {1}", workSheetName, dtExcel.TableName));

            return loadRange;
        }

        static public ExcelRangeBase WorkSheet(ExcelPackage excelPkg,
                                                string workSheetName,
                                                Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable> dtExcels,
                                                Action<ExcelWorksheet> worksheetAction = null,
                                                bool enableMaxRowLimitPerWorkSheet = true,
                                                int maxRowInExcelWorkSheet = -1,
                                                Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                string startingWSCell = "A1")
        {
            DataTable dtComplete = dtExcels.MergeIntoOneDataTable(viewFilterSortRowStateOpts);

            if (dtComplete.Rows.Count == 0) return null;

            if (enableMaxRowLimitPerWorkSheet
                        && maxRowInExcelWorkSheet > 0
                        && dtComplete.Rows.Count > maxRowInExcelWorkSheet)
            {
                var dtSplits = dtComplete.SplitTable(maxRowInExcelWorkSheet);                
                ExcelRangeBase excelRange = null;
                int splitCnt = 0;

                foreach (var dtSplit in dtSplits)
                {
                    excelRange = WorkSheet(excelPkg,
                                            string.Format("{0}-{1:000}", workSheetName, ++splitCnt),
                                            dtSplit,
                                            worksheetAction,
                                            null,
                                            startingWSCell);
                }

                return excelRange;
            }

            return WorkSheet(excelPkg,
                            workSheetName,
                            dtComplete,
                            worksheetAction,
                            null,
                            startingWSCell);
        }

        static public void WorkSheetLoadColumnDefaults(ExcelWorksheet workSheet,
                                                        string column,
                                                        string[] defaultValues)
        {           
            for (int emptyRow = workSheet.Dimension.End.Row + 1, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
            }

        }

        static public void WorkSheetLoadColumnDefaults(ExcelPackage excelPkg,
                                                        string workSheetName,
                                                        string column,
                                                        int startRow,
                                                        string[] defaultValues)
        {
            var workSheet = excelPkg.Workbook.Worksheets[workSheetName];
            if (workSheet == null)
            {
                workSheet = excelPkg.Workbook.Worksheets.Add(workSheetName);
            }
            else
            {
                workSheet.Cells[string.Format("{0}:{1}", startRow, workSheet.Dimension.End.Row)].Clear();                
            }

            for (int emptyRow = startRow, posValue = 0; posValue < defaultValues.Length; ++emptyRow, ++posValue)
            {
                workSheet.Cells[string.Format("{0}{1}", column, emptyRow)].Value = defaultValues[posValue];
            }

        }

    }
}
