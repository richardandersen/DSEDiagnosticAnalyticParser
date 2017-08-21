using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Threading.Tasks;
using Common;
using OfficeOpenXml;

namespace DSEDiagnosticAnalyticParserConsole
{
	static public partial class DTLoadIntoExcel
	{
		public static Task LoadIntoExcel(Task<DataTable> runStatsLogMerged,
											Task<Tuple<DataTable, DataTable, DateTimeRange>> runSummaryLogTask,
											Task<DataTable> runLogMergedTask,
											Task<DataTable> runCFStatsMergedDDLUpdated,
											Task<DataTable> runNodeStatsMergedTask,
											Task<DataTable> tskdtCFHistogram,
											Task<DataTable> runReadRepairTbl,
											Task<Tuple<DataTable, DataTable, DataTable, Common.Patterns.Collections.ThreadSafe.Dictionary<string, string>>> updateRingWYamlInfoTask,
											DataTable dtTokenRange,
											DataTable dtKeySpace,
											DataTable dtDDLTable,
											Task<DataTable> runCompHistMergeTask,
											Task runReleaseDependentLogTask)
		{

			#region Excel Creation/Formatting

			var excelFile = Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelFilePath);
			//bool excelFileCopied = false;
			bool excelFileExists = (ParserSettings.ParsingExcelOptions.LoadSummaryWorkSheets.IsEnabled()
										|| ParserSettings.ParsingExcelOptions.LoadWorkSheets.IsEnabled())
									&& excelFile.Exist();
			var excelFileFound = excelFileExists;
			var excelFileAttrs = excelFileExists ? excelFile.GetAttributes() : System.IO.FileAttributes.Normal;
			Task runLogToExcel = null;

			try
			{
				if ((ParserSettings.ParsingExcelOptions.LoadSummaryWorkSheets.IsEnabled()
											|| ParserSettings.ParsingExcelOptions.LoadWorkSheets.IsEnabled())
						&& !string.IsNullOrEmpty(ParserSettings.ExcelTemplateFilePath))
				{
					var excelTemplateFile = ParserSettings.ExcelTemplateFilePath == null ? null : Common.Path.PathUtils.BuildFilePath(ParserSettings.ExcelTemplateFilePath);

					if (excelTemplateFile != null
							&& !excelFileExists
							&& excelTemplateFile.Exist())
					{
						try
						{
							excelFile.ReplaceFileExtension(excelTemplateFile.FileExtension);
							if (excelTemplateFile.Copy(excelFile))
							{
								Logger.Instance.InfoFormat("Created Workbook \"{0}\" from Template \"{1}\"", excelFile.Path, excelTemplateFile.Path);
								excelFileExists = true;
								//excelFileCopied = true;
								excelFileAttrs = excelFile.GetAttributes();
								excelFile.SetAttributes(excelFileAttrs | System.IO.FileAttributes.Hidden);
							}
						}
						catch (System.Exception ex)
						{
							Logger.Instance.Error(string.Format("Created Workbook \"{0}\" from Template \"{1}\" Failed", excelFile.Path, excelTemplateFile.Path), ex);
							Program.ConsoleErrors.Increment("Workbook Template Copy Failed");
						}
					}
				}

				if (excelFileFound)
				{
					excelFile.SetAttributes(excelFileAttrs | System.IO.FileAttributes.Hidden);
				}
				else if (!excelFileExists)
				{
					excelFile.ReplaceFileExtension(ParserSettings.ExcelWorkBookFileExtension);
				}

				#region Load Logs into Excel
				{
					Task statusLogToExcel = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();
					Task summaryLogToExcel = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();
					Task logToExcel = Common.Patterns.Tasks.CompletionExtensions.CompletedTask();

					if (ParserSettings.ParsingExcelOptions.ProduceStatsWorkbook.IsEnabled())
					{
						statusLogToExcel = DTLoadIntoExcel.LoadStatusLog(runStatsLogMerged,
																			   excelFile.Path,
																			   ParserSettings.ExcelWorkSheetStatusLogCassandra,
																			   ProcessFileTasks.LogCassandraMaxMinTimestamp,
																			   ParserSettings.MaxRowInExcelWorkBook,
																			   ParserSettings.MaxRowInExcelWorkSheet,
																			   ParserSettings.LogExcelWorkbookFilter);
					}

					if (ParserSettings.ParsingExcelOptions.ProduceSummaryWorkbook.IsEnabled())
					{
						summaryLogToExcel = DTLoadIntoExcel.LoadCassandraExceptionSummaryLog(runSummaryLogTask,
																								excelFile.Path,
																								ParserSettings.ExcelWorkSheetExceptionSummaryLogCassandra);
					}

					Task.Factory
							.ContinueWhenAll(new Task[] { statusLogToExcel, summaryLogToExcel }, tasks => { Program.ConsoleExcelLogStatus.Terminate(); });
					Task.Factory
							.ContinueWhenAll(new Task[] { runSummaryLogTask, summaryLogToExcel }, tasks =>
							{
								//Data Table should be loaded into Excel... Release rows to free up memory...
								runSummaryLogTask.Result?.Item2?.Clear();
							});

					if (ParserSettings.LogParsingExcelOptions.CreateWorkbook.IsEnabled())
					{
						logToExcel = DTLoadIntoExcel.LoadCassandraLog(runLogMergedTask,
																			excelFile.Path,
																			ParserSettings.ExcelWorkSheetLogCassandra,
																			ProcessFileTasks.LogCassandraMaxMinTimestamp,
																			ParserSettings.MaxRowInExcelWorkBook,
																			ParserSettings.MaxRowInExcelWorkSheet,
																			ParserSettings.LogExcelWorkbookFilter);

						logToExcel.ContinueWith(action =>
						{
							Program.ConsoleExcelLog.Terminate();
						});
					}

					Task.Factory
							.ContinueWhenAll(new Task[] { logToExcel, runLogMergedTask, runReleaseDependentLogTask }, tasks =>
							{
								//Data Table should be loaded into Excel... Release rows to free up memory...
								runLogMergedTask.Result?.Clear();
							});

					runLogToExcel = Task.Factory
										.ContinueWhenAll(new Task[] { statusLogToExcel, summaryLogToExcel, logToExcel }, tasks => { });
				}

				#endregion

				#region non-logs into Excel
				//Non-Logs
				if (ParserSettings.ParsingExcelOptions.LoadWorkSheets.IsEnabled()
						|| ParserSettings.ParsingExcelOptions.LoadSummaryWorkSheets.IsEnabled()
						|| ParserSettings.ParsingExcelOptions.LoadReadRepairWorkSheets.IsEnabled())
				{
					using (var excelPkg = new ExcelPackage(excelFile.FileInfo()))
					{
                        System.Console.CancelKeyPress += (sender, eventArgs) =>
                        {
                            try
                            {
                                var newExcelFile = (IFilePath)excelFile.Clone("Aborted-" + excelFile.FileName);
                                Logger.Instance.InfoFormat("ABORTED Excel WorkBook saved to \"{0}\"", newExcelFile.PathResolved);
                                DTLoadIntoExcel.UpdateApplicationWs(excelPkg, true);
                                excelPkg.SaveAs(newExcelFile.FileInfo());
                                Program.ConsoleExcelWorkbook.Increment(newExcelFile);                                
                            }
                            catch
                            {
                                Logger.Instance.Error("ABORTED Excel WorkBook Save failed!");
                            }
                            try
                            {
                                if (excelFileFound)
                                {
                                    excelFile.SetAttributes(excelFileAttrs);
                                }
                                else
                                {
                                    excelFile.Delete();
                                }
                            }
                            catch { }
                        };

                        if (ParserSettings.ParsingExcelOptions.LoadWorkSheets.IsEnabled())
						{
							DTLoadIntoExcel.LoadTokenRangeInfo(excelPkg, dtTokenRange, ParserSettings.ExcelWorkSheetRingTokenRanges);							
                            DTLoadIntoExcel.LoadYamlRingOSInfo(runLogMergedTask,
                                                                updateRingWYamlInfoTask,
																excelPkg,
																ParserSettings.ExcelWorkSheetYaml,
																ParserSettings.ExcelWorkSheetRingInfo,
																ParserSettings.ExcelWorkSheetOSMachineInfo);

                            if (ParserSettings.ParsingExcelOptions.ParseCompacationHistFiles.IsEnabled())
                            {
                                runCompHistMergeTask.Wait();
                                DTLoadIntoExcel.LoadCompacationHistory(excelPkg, runCompHistMergeTask.Result, ParserSettings.ExcelWorkSheetCompactionHist);
                                runCompHistMergeTask.Result?.Clear();
                            }

                            runLogMergedTask?.Wait();
                            runCFStatsMergedDDLUpdated?.Wait();
							runNodeStatsMergedTask?.Wait();

							DTLoadIntoExcel.LoadCFStats(excelPkg, runCFStatsMergedDDLUpdated?.Result, ParserSettings.ExcelWorkSheetCFStats);
							DTLoadIntoExcel.LoadNodeStats(excelPkg, runNodeStatsMergedTask?.Result, ParserSettings.ExcelWorkSheetNodeStats);
							DTLoadIntoExcel.LoadKeySpaceDDL(excelPkg, dtKeySpace, ParserSettings.ExcelWorkSheetDDLKeyspaces);
							DTLoadIntoExcel.LoadTableDDL(excelPkg, dtDDLTable, ParserSettings.ExcelWorkSheetDDLTables);
						}
						runCFStatsMergedDDLUpdated?.Result?.Clear();
						runNodeStatsMergedTask?.Result?.Clear();
						dtDDLTable?.Clear();

						if (ParserSettings.ParsingExcelOptions.LoadSummaryWorkSheets.IsEnabled())
						{
							runSummaryLogTask?.Wait();

							if (runSummaryLogTask?.Result != null)
							{
								DTLoadIntoExcel.LoadSummaryLog(excelPkg,
																runSummaryLogTask.Result.Item1,
																ParserSettings.ExcelWorkSheetSummaryLogCassandra,
																ProcessFileTasks.LogCassandraMaxMinTimestamp,
																runSummaryLogTask.Result.Item3,
																ParserSettings.LogExcelWorkbookFilter);
							}
						}
						runSummaryLogTask?.Result?.Item1?.Clear();

						if (ParserSettings.ParsingExcelOptions.LoadWorkSheets.IsEnabled())
						{
							DTLoadIntoExcel.LoadCFHistogram(excelPkg, tskdtCFHistogram, ParserSettings.ExcelCFHistogramWorkSheet);
						}
						tskdtCFHistogram?.Result?.Clear();

						if (ParserSettings.ParsingExcelOptions.LoadReadRepairWorkSheets.IsEnabled())
						{
							DTLoadIntoExcel.LoadReadRepair(runReadRepairTbl, excelPkg, ParserSettings.ReadRepairWorkSheetName);
						}
						runReadRepairTbl?.Result?.Clear();

						DTLoadIntoExcel.UpdateApplicationWs(excelPkg);

						excelPkg.Save();
						Program.ConsoleExcelWorkbook.Increment(excelFile);
						Program.ConsoleExcelNonLog.Terminate();
						Logger.Instance.InfoFormat("Excel WorkBooks saved to \"{0}\"", excelFile.PathResolved);
					} //Save non-log data
				}
				#endregion
			}
			finally
			{
				if (excelFileExists)
				{
					excelFile.SetAttributes(excelFileAttrs);
				}
			}
			#endregion

			return runLogToExcel;
		}
	}
}
