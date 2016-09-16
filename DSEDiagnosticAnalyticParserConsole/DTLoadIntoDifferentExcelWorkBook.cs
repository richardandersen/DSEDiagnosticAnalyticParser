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
        public enum WorkBookProcessingStage
        {
            PreProcess,
            PreLoad,
            PreSave,
            Saved,
            PostProcess
        }

        static public int DifferentExcelWorkBook(string excelFilePath,
                                                    string workSheetName,
                                                    DataTable dtExcel,
                                                    Action<WorkBookProcessingStage, IFilePath, string, ExcelPackage, int> workBookActions,
                                                    Action<ExcelWorksheet> worksheetAction = null,
                                                    int maxRowInExcelWorkBook = -1,
                                                    int maxRowInExcelWorkSheet = -1,
                                                    Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                    string startingWSCell = "A1",
                                                    bool bypassPreprocessAction = false)
        {
            var excelTargetFile = Common.Path.PathUtils.BuildFilePath(excelFilePath);

            if (!bypassPreprocessAction)
            {
                workBookActions?.Invoke(WorkBookProcessingStage.PreProcess, excelTargetFile, null, null, -1);
            }

            if (dtExcel.Rows.Count == 0)
            {
                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, null, null, null, 0);
                return 0;
            }

            if (maxRowInExcelWorkBook <= 0 || dtExcel.Rows.Count <= maxRowInExcelWorkBook)
            {
                excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}{1}",
                                                                        excelTargetFile.Name,
                                                                        excelTargetFile.FileExtension);

                var excelFile = excelTargetFile.ApplyFileNameFormat(new object[] { workSheetName });
                
                using (var excelPkg = new ExcelPackage(excelFile.FileInfo()))
                {
                    workBookActions?.Invoke(WorkBookProcessingStage.PreLoad, excelFile, workSheetName, excelPkg, -1);

                    WorkBook(excelPkg,
                                workSheetName,
                                dtExcel,
                                worksheetAction,
                                null,
                                startingWSCell);

                    workBookActions?.Invoke(WorkBookProcessingStage.PreSave, excelFile, workSheetName, excelPkg, dtExcel.Rows.Count);
                    DTLoadIntoExcel.UpdateApplicationWs(excelPkg);
                    excelPkg.Save();
                    workBookActions?.Invoke(WorkBookProcessingStage.Saved, excelFile, workSheetName, excelPkg, dtExcel.Rows.Count);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
                }

                workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, null, null, null, dtExcel.Rows.Count);
                return dtExcel.Rows.Count;
            }

            var dtSplits = dtExcel.SplitTable(maxRowInExcelWorkBook);           
            int nResult = 0;
            long totalRows = 0;

            excelTargetFile.FileNameFormat = string.Format("{0}-{{0}}-{{1:000}}{1}",
                                                                excelTargetFile.Name,
                                                                excelTargetFile.FileExtension);

            Parallel.ForEach(dtSplits, dtSplit =>
            //foreach (var dtSplit in dtSplits)
            {
                var excelFile = ((IFilePath)excelTargetFile.Clone()).ApplyFileNameFormat(new object[] { workSheetName, System.Threading.Interlocked.Increment(ref totalRows) });
                using (var excelPkg = new ExcelPackage(excelFile.FileInfo()))
                {
                    var newStack = new Common.Patterns.Collections.LockFree.Stack<System.Data.DataTable>();

                    workBookActions?.Invoke(WorkBookProcessingStage.PreLoad, excelFile, workSheetName, excelPkg, -1);

                    newStack.Push(dtSplit);

                    WorkBook(excelPkg,
                                workSheetName,
                                newStack,
                                worksheetAction,
                                maxRowInExcelWorkSheet > 0,
                                maxRowInExcelWorkSheet,
                                null,
                                startingWSCell);

                    System.Threading.Interlocked.Add(ref nResult, dtSplit.Rows.Count);

                    workBookActions?.Invoke(WorkBookProcessingStage.PreSave, excelFile, workSheetName, excelPkg, dtSplit.Rows.Count);
                    DTLoadIntoExcel.UpdateApplicationWs(excelPkg);
                    excelPkg.Save();
                    workBookActions?.Invoke(WorkBookProcessingStage.Saved, excelFile, workSheetName, excelPkg, dtSplit.Rows.Count);
                    Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
                }
            });

            workBookActions?.Invoke(WorkBookProcessingStage.PostProcess, null, null, null, nResult);

            return nResult;
        }

        static public int DifferentExcelWorkBook(string excelFilePath,
                                                    string workSheetName,
                                                    Common.Patterns.Collections.LockFree.Stack<DataTable> dtExcelStack,
                                                    Action<WorkBookProcessingStage, IFilePath, string, ExcelPackage, int> workBookActions,
                                                    Action<ExcelWorksheet> worksheetAction = null,
                                                    int maxRowInExcelWorkBook = -1,
                                                    int maxRowInExcelWorkSheet = -1,
                                                    Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null,
                                                    string startingWSCell = "A1")
        {
            workBookActions?.Invoke(WorkBookProcessingStage.PreProcess, Common.Path.PathUtils.BuildFilePath(excelFilePath), null, null, -1);

            var dtComplete = dtExcelStack.MergeIntoOneDataTable(viewFilterSortRowStateOpts);

            return DifferentExcelWorkBook(excelFilePath,
                                            workSheetName,
                                            dtComplete,
                                            workBookActions,
                                            worksheetAction,
                                            maxRowInExcelWorkBook,
                                            maxRowInExcelWorkSheet,
                                            null,
                                            startingWSCell,
                                            true);
        }

        static public void UpdateApplicationWs(ExcelPackage excelPkg)
        {
            var workSheet = excelPkg.Workbook.Worksheets["Application"];
            if (workSheet == null)
            {
                workSheet = excelPkg.Workbook.Worksheets.Add("Application");
            }
           
            workSheet.Cells["A2"].Value = string.Format("Run Timestamp {0}", Program.RunDateTime);
            workSheet.Cells["A3"].Value = string.Format("Program: {0} Version: {1} Directory: {2}",
                                                            Common.Functions.Instance.ApplicationName,
                                                            Common.Functions.Instance.ApplicationVersion,
                                                            Common.Functions.Instance.AssemblyDir);
            workSheet.Cells["A4"].Value = string.Format("Working Directory: {0}", System.Environment.CurrentDirectory);
            workSheet.Cells["A5"].Value = Program.CommandArgsString;
            workSheet.Cells["A6"].Value = string.Format("Warnings: {0} Errors: {1}",
                                                            Program.ConsoleWarnings.Counter > 0,
                                                            Program.ConsoleErrors.Counter > 0);
            workSheet.Cells["A7"].Value = string.Format("Log {0}", ProcessFileTasks.LogCassandraMaxMinTimestamp);
        }
    }
}
