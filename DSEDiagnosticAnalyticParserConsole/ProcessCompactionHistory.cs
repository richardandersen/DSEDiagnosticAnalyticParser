using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using System.Text.RegularExpressions;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
		static public void initializeCompactionHistDataTable(DataTable dtCmpHist)
		{
			if (dtCmpHist.Columns.Count == 0)
			{
				dtCmpHist.Columns.Add("Source", typeof(string));
				dtCmpHist.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
				dtCmpHist.Columns.Add("Node IPAddress", typeof(string));
				dtCmpHist.Columns.Add("KeySpace", typeof(string));
				dtCmpHist.Columns.Add("Table", typeof(string));
				dtCmpHist.Columns.Add("Compaction Timestamp (UTC)", typeof(DateTime));
				dtCmpHist.Columns.Add("SSTable Size Before", typeof(long));
				dtCmpHist.Columns.Add("Before Size (MB)", typeof(decimal));
				dtCmpHist.Columns.Add("SSTable Size After", typeof(long));
				dtCmpHist.Columns.Add("After Size (MB)", typeof(decimal));
				dtCmpHist.Columns.Add("Compaction Strategy", typeof(string)).AllowDBNull = true;
				dtCmpHist.Columns.Add("Partitions Merged (tables:rows)", typeof(string));
				dtCmpHist.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;

				//dtFSStats.PrimaryKey = new System.Data.DataColumn[] { dtFSStats.Columns[0],  dtFSStats.Columns[1],  dtFSStats.Columns[2],  dtFSStats.Columns[3], dtFSStats.Columns[4] };
			}
		}

		static public void ReadCompactionHistFileParseIntoDataTable(IFilePath cmphistFilePath,
																		string ipAddress,
																		string dcName,
																		DataTable dtCmpHist,
																		DataTable dtTable,
																		IEnumerable<string> ignoreKeySpaces,
																		List<CKeySpaceTableNames> kstblExists)
		{
			initializeCompactionHistDataTable(dtCmpHist);

			string line;
			DataRow dataRow;
			DataRow ksDataRow;
			List<string> parsedLine;
			string currentKeySpace;
			string currentTable;
			int offSet;

			using (var readStream = cmphistFilePath.StreamReader())
			{
				while(!readStream.EndOfStream)
				{
					line = readStream.ReadLine().Trim();

					if (string.IsNullOrEmpty(line)
							|| line.StartsWith("Compaction History", StringComparison.OrdinalIgnoreCase)
							|| line.StartsWith("id ", StringComparison.OrdinalIgnoreCase))
					{
						continue;
					}

					if (line.StartsWith("exception running", StringComparison.OrdinalIgnoreCase))
					{
						line.Dump(Logger.DumpType.Warning, "Invalid Line in \"{0}\".", cmphistFilePath);
						Program.ConsoleWarnings.Increment("Compaction History invalid Line...");
						continue;
					}

					//Compaction History:
					//id 									keyspace_name      	columnfamily_name 	compacted_at		bytes_in 	bytes_out      					rows_merged
					//cfde9db0-3d06-11e6-adbd-0fa082120add 	production_mqh_bi  	bi_newdata			1467101014795		247011505	247011472      					{ 1:354, 2:1}
					//																				timestamp			size SSTtable before and after compaction	the number of partitions merged. The notation means {tables:rows}. For example: {1:3, 3:1} means 3 rows were taken from one SSTable (1:3) and 1 row taken from 3 SSTables (3:1) to make the one SSTable in that compaction operation.
					//	0										1				2					3					4			5								6
					//	 26909550-65e3-11e6-923c-7d02e3681807     dse_perf           partition_size_histograms_summary1471593696037             168003         1114           {1:15}

					parsedLine = Common.StringFunctions.Split(line,
																' ',
																Common.StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace,
																Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

					if (parsedLine.Count <= 4)
					{
						line.Dump(Logger.DumpType.Warning, "Invalid Line in \"{0}\".", cmphistFilePath);
						Program.ConsoleWarnings.Increment("Compaction History invalid Line...");
						continue;
					}

					if (parsedLine.Count > 6)
					{
						currentKeySpace = RemoveQuotes(parsedLine[1]);
						currentTable = RemoveQuotes(parsedLine[2]);
						offSet = 0;
					}
					else
					{
						if (parsedLine[1].Length > 20)
						{
							var ksItem = kstblExists
												.Where(e => parsedLine[1].StartsWith(e.ConcatName))
												.OrderByDescending(e => e.LogName.Length).FirstOrDefault();

							currentKeySpace = ksItem == null ? "?" : ksItem.KeySpaceName;
							currentTable = ksItem == null ? parsedLine[1] : ksItem.TableName;

							if (ignoreKeySpaces.Contains(currentKeySpace))
							{
								continue;
							}

							if (ksItem != null && parsedLine[1].Length > currentKeySpace.Length + currentTable.Length)
							{
								parsedLine[1] = parsedLine[1].Substring(currentKeySpace.Length + currentTable.Length);
								offSet = 2;
							}
							else
							{
								if (ksItem == null)
								{
									line.Dump(Logger.DumpType.Warning, "Line Ignored. Invalid line found/Unknown Keyspace/Table in Compaction History File \"{0}\"", cmphistFilePath.PathResolved);
									Program.ConsoleWarnings.Increment("Invalid Compaction History Line/Unknown KS/Tbl:", line);
									continue;
								}

								offSet = 1;
							}
						}
						else
						{
							currentKeySpace = RemoveQuotes(parsedLine[1]);

							if (ignoreKeySpaces.Contains(currentKeySpace))
							{
								continue;
							}

							if (parsedLine[2].Length > 30)
							{
								var ksItem = kstblExists
												.Where(e => e.KeySpaceName == currentKeySpace
																&& parsedLine[2].StartsWith(e.TableName))
												.OrderByDescending(e => e.LogName.Length).FirstOrDefault();

								currentTable = ksItem == null ? "?" : ksItem.TableName;

								if (ksItem != null && parsedLine[2].Length > currentTable.Length)
								{
									parsedLine[2] = parsedLine[2].Substring(currentTable.Length);
									offSet = 1;
								}
								else
								{
									if (ksItem == null)
									{
										line.Dump(Logger.DumpType.Warning, "Line Ignored. Invalid line found/unknown keyspace/table in Compaction History File \"{0}\"", cmphistFilePath.PathResolved);
										Program.ConsoleWarnings.Increment("Invalid Compaction History Line/Unknown KS/Tbl:", line);
										continue;
									}

									offSet = 1;
								}
							}
							else
							{
								currentTable = RemoveQuotes(parsedLine[2]);
								offSet = 0;
							}
						}
					}

					if (ignoreKeySpaces.Contains(currentKeySpace))
					{
						continue;
					}

					try
					{
						dataRow = dtCmpHist.NewRow();

						dataRow["Source"] = "CompactionHistory";
						dataRow["Data Center"] = dcName;
						dataRow["Node IPAddress"] = ipAddress;
						dataRow["KeySpace"] = currentKeySpace;
						dataRow["Table"] = currentTable;
						dataRow["Compaction Timestamp (UTC)"] = FromUnixTime(parsedLine[3 - offSet]);
						dataRow["SSTable Size Before"] = long.Parse(parsedLine[4 - offSet]);
						dataRow["Before Size (MB)"] = ConvertInToMB(parsedLine[4 - offSet], "MB");
						dataRow["SSTable Size After"] = long.Parse(parsedLine[5 - offSet]);
						dataRow["After Size (MB)"] = ConvertInToMB(parsedLine[5 - offSet], "MB");
						dataRow["Partitions Merged (tables:rows)"] = parsedLine[6 - offSet];

						ksDataRow = dtTable.Rows.Find(new object[] { currentKeySpace, currentTable });

						if (ksDataRow != null)
						{
							dataRow["Compaction Strategy"] = ksDataRow["Compaction Strategy"];
						}

						dtCmpHist.Rows.Add(dataRow);
					}
					catch (System.Exception ex)
					{
						Logger.Instance.Error(string.Format("Parsing for Compacation History for Node {0} failed during parsing of line \"{1}\". Line skipped.",
																ipAddress,
																line),
												ex);
						Program.ConsoleWarnings.Increment("Compaction History Parsing Exception; Line Skipped");
					}
				}
			}
		}
    }
}
