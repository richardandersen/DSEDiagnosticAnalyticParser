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
        static Regex RegExCreateIndex = new Regex(@"\s*create\s+(?:custom\s*)?index\s+(.+)?\s*on\s+(.+)\s+\(\s*(?:(?:keys\(\s*(.+)\s*\))?|(?:entries\(\s*(.+)\s*\))?|(?:full\(\s*(.+)\s*\))?|(.+)?)\).*",
                                        RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCreateIndexUsing = new Regex(@".+using\s*((?:'|""|`)?.+?(?:'|""|`))?",
                                                RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static public void InitializeCQLDDLDataTables(DataTable dtKeySpace,
                                                      DataTable dtTable)
        {
            if (dtKeySpace.Columns.Count == 0)
            {
                dtKeySpace.Columns.Add("Name", typeof(string));//a
                dtKeySpace.Columns.Add("Replication Strategy", typeof(string));
                dtKeySpace.Columns.Add("Data Center", typeof(string));
                dtKeySpace.Columns.Add("Replication Factor", typeof(int));//d
				dtKeySpace.Columns.Add("Tables", typeof(int)).AllowDBNull = true;//e
                dtKeySpace.Columns.Add("Columns", typeof(int)).AllowDBNull = true;//f
                dtKeySpace.Columns.Add("Indexes", typeof(int)).AllowDBNull = true;//g
				dtKeySpace.Columns.Add("solr", typeof(int)).AllowDBNull = true;
				dtKeySpace.Columns.Add("Total", typeof(int)).AllowDBNull = true;//i
				dtKeySpace.Columns.Add("Active", typeof(int)).AllowDBNull = true;//j
				dtKeySpace.Columns.Add("STCS", typeof(int)).AllowDBNull = true;//k
				dtKeySpace.Columns.Add("LCS", typeof(int)).AllowDBNull = true;//l
				dtKeySpace.Columns.Add("DTCS", typeof(int)).AllowDBNull = true;//m
				dtKeySpace.Columns.Add("TCS", typeof(int)).AllowDBNull = true;//n
				dtKeySpace.Columns.Add("TWCS", typeof(int)).AllowDBNull = true;//o
				dtKeySpace.Columns.Add("Other Strategies", typeof(int)).AllowDBNull = true;//p
				dtKeySpace.Columns.Add("DDL", typeof(string));//q

                dtKeySpace.PrimaryKey = new System.Data.DataColumn[] { dtKeySpace.Columns["Name"], dtKeySpace.Columns["Data Center"] };
            }

            if (dtTable.Columns.Count == 0)
            {
                dtTable.Columns.Add("Active", typeof(bool));//a
                dtTable.Columns.Add("Keyspace Name", typeof(string));//b
                dtTable.Columns.Add("Name", typeof(string));
                dtTable.Columns.Add("Partition Key", typeof(string));
                dtTable.Columns.Add("Cluster Key", typeof(string)).AllowDBNull = true;
                dtTable.Columns.Add("Compaction Strategy", typeof(string)).AllowDBNull = true;
                dtTable.Columns.Add("Compression", typeof(string)).AllowDBNull = true; //g
                dtTable.Columns.Add("Index", typeof(bool)).AllowDBNull = true; //h
                dtTable.Columns.Add("Chance", typeof(decimal)).AllowDBNull = true;//i
                dtTable.Columns.Add("DC Chance", typeof(decimal)).AllowDBNull = true;//j
                dtTable.Columns.Add("Policy", typeof(string)).AllowDBNull = true;//k
                dtTable.Columns.Add("GC Grace Period", typeof(TimeSpan)).AllowDBNull = true;//l
                dtTable.Columns.Add("Collections", typeof(int));//m
                dtTable.Columns.Add("Counters", typeof(int));//n
                dtTable.Columns.Add("Blobs", typeof(int));//o
                dtTable.Columns.Add("Static", typeof(int));//p
                dtTable.Columns.Add("Frozen", typeof(int));//q
				dtTable.Columns.Add("Type", typeof(int));//r
				dtTable.Columns.Add("Total", typeof(int));//s
                dtTable.Columns.Add("Associated Table", typeof(string)).AllowDBNull = true;//t
                dtTable.Columns.Add("DDL", typeof(string));//u

                dtTable.PrimaryKey = new System.Data.DataColumn[] { dtTable.Columns["Keyspace Name"], dtTable.Columns["Name"] };
            }
        }
        static public void ReadCQLDDLParseIntoDataTable(IFilePath cqlDDLFilePath,
                                                            string ipAddress,
                                                            string dcName,
                                                            DataTable dtKeySpace,
                                                            DataTable dtTable,
                                                            Dictionary<string, int> cqlHashCheck,
                                                            IEnumerable<string> ignoreKeySpaces)
        {

            InitializeCQLDDLDataTables(dtKeySpace, dtTable);

            var fileLines = cqlDDLFilePath.ReadAllLines();
            string line = null;
            var strCQL = new StringBuilder();
            List<string> parsedValues;
            List<string> parsedComponent;
			Dictionary<string, IEnumerable<string>> types = new Dictionary<string, IEnumerable<string>>();
            string currentKeySpace = null;
            DataRow dataRow;

            try
            {
                for (int nLine = 0; nLine < fileLines.Length; ++nLine)
                {
                    line = fileLines[nLine].Trim();

                    if (string.IsNullOrEmpty(line)
                            || line.Substring(0, 2) == "//"
                            || line.Substring(0, 2) == "--"
                            || line[0] == '\u001b')
                    {
                        continue;
                    }
                    else if (line.Substring(0, 2) == "/*")
                    {
                        for (; nLine < fileLines.Length
                                    && !line.Contains("*/");
                                ++nLine)
                        {
                            line = fileLines[nLine].Trim();
                        }
                        continue;
                    }

                    line = RemoveCommentInLine(line, "/*", "*/");

                    strCQL.Append(" ");
                    strCQL.Append(line);

                    if (line[line.Length - 1] == ';')
                    {
                        string cqlStr = strCQL.ToString().TrimStart();
                        strCQL.Clear();

                        if (cqlStr.ToLower().StartsWith("use "))
                        {
                            parsedValues = Common.StringFunctions.Split(cqlStr,
                                                                            new char[] { ' ', ';' },
                                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                                | Common.StringFunctions.IgnoreWithinDelimiterFlag.Text
                                                                                | Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket,
                                                                            Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                            currentKeySpace = RemoveQuotes(parsedValues.Last());
                            continue;
                        }

                        parsedValues = Common.StringFunctions.Split(cqlStr,
                                                                    new char[] { ',', '{', '}' },
                                                                    Common.StringFunctions.IgnoreWithinDelimiterFlag.AngleBracket
                                                                        | Common.StringFunctions.IgnoreWithinDelimiterFlag.Text
                                                                        | Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket
                                                                        | Common.StringFunctions.IgnoreWithinDelimiterFlag.Parenthese,
                                                                    Common.StringFunctions.SplitBehaviorOptions.Default
                                                                        | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                        if (parsedValues[0].StartsWith("create", StringComparison.OrdinalIgnoreCase))
                        {

                            if (parsedValues[0].Substring(6, 9).TrimStart().ToLower() == "keyspace")
                            {
                                #region keyspace
                                parsedComponent = Common.StringFunctions.Split(parsedValues[0],
                                                                                ' ',
                                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                    | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                                //CREATE KEYSPACE billing WITH replication =
                                //'class': 'NetworkTopologyStrategy'
                                //'us-west-2': '3'
                                //;

                                var ksName = RemoveQuotes(parsedComponent[parsedComponent.Count() - 4]);

                                if (ignoreKeySpaces.Contains(ksName) && !Properties.Settings.Default.AlwayIncludeDLLKeyspacess.Contains(ksName))
                                {
                                    continue;
                                }

                                parsedComponent = Common.StringFunctions.Split(parsedValues[1],
                                                                                ':',
                                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                    | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                                var ksStratery = RemoveNamespace(parsedComponent.Last().Trim());

                                for (int nIndex = 2; nIndex < parsedValues.Count - 1; ++nIndex)
                                {
									dataRow = dtKeySpace.NewRow();
                                    dataRow["Name"] = ksName;
                                    dataRow["Replication Strategy"] = ksStratery;

                                    parsedComponent = Common.StringFunctions.Split(parsedValues[nIndex],
                                                                                    ':',
                                                                                    Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                    Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                        | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                                    dataRow["Data Center"] = RemoveQuotes(parsedComponent[0]);
                                    dataRow["Replication Factor"] = int.Parse(RemoveQuotes(parsedComponent[1]));
                                    dataRow["DDL"] = cqlStr;

									if (dtKeySpace.AsEnumerable().Any(i => i.Field<string>("Name") == ksName
																				&& i.Field<string>("Replication Strategy") == ksStratery
																				&& i.Field<string>("Data Center") == (string) dataRow["Data Center"]
																				&& i.Field<int>("Replication Factor") == (int)dataRow["Replication Factor"]))
									{
										continue;
									}

									dtKeySpace.Rows.Add(dataRow);
                                }
                                #endregion
                            }
                            else if (parsedValues[0].Substring(6, 6).TrimStart().ToLower() == "table" || parsedValues[0].Substring(6, 5).TrimStart().ToLower() == "type")
                            {
								#region table/type
								//CREATE TABLE account_payables(date int, org_key text, product_type text, product_id bigint, product_update_id bigint, vendor_type text, parent_product_id bigint, parent_product_type text, parent_product_update_id bigint, user_id bigint, vendor_detail text, PRIMARY KEY((date, org_key), product_type, product_id, product_update_id, vendor_type)) WITH bloom_filter_fp_chance = 0.100000 AND caching = 'KEYS_ONLY' AND comment = '' AND dclocal_read_repair_chance = 0.100000 AND gc_grace_seconds = 864000 AND index_interval = 128 AND read_repair_chance = 0.000000 AND replicate_on_write = 'true' AND populate_io_cache_on_flush = 'false' AND default_time_to_live = 0 AND speculative_retry = '99.0PERCENTILE' AND memtable_flush_period_in_ms = 0 AND compaction =
								//		'class': 'LeveledCompactionStrategy'
								//AND compression =
								//'sstable_compression': 'LZ4Compressor'
								//;
								//AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
								//AND compaction = {'class': 'org.apache.cassandra.db.compaction.SizeTieredCompactionStrategy'}
								//compression = { 'sstable_compression' : 'Encryptor', 'cipher_algorithm' : 'AES/ECB/PKCS5Padding', 'secret_key_strength' : 128, 'chunk_length_kb' : 1}
								//  s text STATIC,
								//
								//CREATE TYPE nfl_assets.udt_week (id text, created_date timestamp, last_modified_date timestamp, season_value int, week_value int, week_order int, week_type text, season_type text);

								var createType = parsedValues[0].Substring(6, 5).TrimStart().ToLower() == "type";
								var startParan = cqlStr.IndexOf('(');
                                var endParan = cqlStr.LastIndexOf(')');
                                var strFrtTbl = cqlStr.Substring(0, startParan);
                                var strColsTbl = cqlStr.Substring(startParan + 1, endParan - startParan - 1);
                                var strOtpsTbl = cqlStr.Substring(endParan + 1);

                                //Split to Find Table Name
                                parsedComponent = Common.StringFunctions.Split(strFrtTbl,
                                                                                ' ',
                                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                    | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

                                var kstblName = SplitTableName(parsedComponent.Last(), currentKeySpace);

                                if (ignoreKeySpaces.Contains(kstblName.Item1))
                                {
                                    continue;
                                }

								var tableName = createType ? kstblName.Item2 + " (Type)" : kstblName.Item2;
								var keyspacetableName = kstblName.Item1 + "." + kstblName.Item2;

								if (dtTable.AsEnumerable().Any(i => i.Field<string>("Keyspace Name") == kstblName.Item1
																				&& i.Field<string>("Name") == tableName))
								{
									continue;
								}

								dataRow = dtTable.NewRow();
                                dataRow["Keyspace Name"] = kstblName.Item1;
                                dataRow["Name"] = tableName;

                                if (cqlStr.Length > 32760)
                                {
                                    dataRow["DDL"] = cqlStr.Substring(0, 32755) + "...";
                                    cqlStr.Dump(Logger.DumpType.Warning, "CQL DDL String exceed Excels Maximum Length of 32,760 ({0:###,##,##0}). CQL DDL String truncated to 32,775.", cqlStr.Length);
                                    Program.ConsoleWarnings.Increment("CQL DDL Truncated");
                                }
                                else
                                {
                                    dataRow["DDL"] = cqlStr;
                                }

                                //Find Columns
                                var tblColumns = Common.StringFunctions.Split(strColsTbl,
                                                                                ',',
                                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                    | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);


                                if (tblColumns.Last().StartsWith("PRIMARY KEY", StringComparison.OrdinalIgnoreCase))
                                {
                                    var pkClause = tblColumns.Last();
                                    startParan = pkClause.IndexOf('(');
                                    endParan = pkClause.LastIndexOf(')');

                                    var pckList = Common.StringFunctions.Split(pkClause.Substring(startParan + 1, endParan - startParan - 1),
                                                                                    ',',
                                                                                    Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                    Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                        | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries)
                                                        .Select(sf => sf.Trim());

                                    var pkLocation = pckList.First();
                                    if (pkLocation[0] == '(')
                                    {
                                        startParan = pkLocation.IndexOf('(');
                                        endParan = pkLocation.LastIndexOf(')');

                                        var pkList = Common.StringFunctions.Split(pkLocation.Substring(startParan + 1, endParan - startParan - 1),
                                                                                            ',',
                                                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                                                            Common.StringFunctions.SplitBehaviorOptions.Default
                                                                                                | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries)
                                                        .Select(sf => sf.Trim());
                                        var pkdtList = new List<string>();

                                        foreach (var element in pkList)
                                        {
                                            pkdtList.Add(tblColumns.Find(c => c.StartsWith(element)));
                                        }
                                        dataRow["Partition Key"] = string.Join(", ", pkdtList);
                                    }
                                    else
                                    {
                                        dataRow["Partition Key"] = tblColumns.Find(c => c.StartsWith(pkLocation));
                                    }

                                    var cdtList = new List<string>();

                                    for (int nIndex = 1; nIndex < pckList.Count(); ++nIndex)
                                    {
                                        cdtList.Add(tblColumns.Find(c => c.StartsWith(pckList.ElementAt(nIndex))));
                                    }
                                    dataRow["Cluster Key"] = string.Join(", ", cdtList);
                                }
                                else
                                {
                                    //look for keyword Primary Key
                                    var pkVar = tblColumns.Find(c => c.EndsWith("primary key", StringComparison.OrdinalIgnoreCase));

                                    dataRow["Partition Key"] = pkVar?.Substring(0, pkVar.Length - 11).TrimEnd();
                                    dataRow["Cluster Key"] = null;
                                }

                                endParan = tblColumns.Count;

                                if (tblColumns.Last().StartsWith("PRIMARY KEY", StringComparison.OrdinalIgnoreCase)
                                        || tblColumns.Last().StartsWith("WITH ", StringComparison.OrdinalIgnoreCase))
                                {
                                    --endParan;
                                }

                                int nbrCollections = 0;
                                int nbrCounters = 0;
                                int nbrBlobs = 0;
                                int nbrStatic = 0;
                                int nbrFrozen = 0;
								int nbrType = 0;

                                for (int nIndex = 0; nIndex < endParan; ++nIndex)
                                {
                                    if (tblColumns[nIndex].EndsWith("primary key", StringComparison.OrdinalIgnoreCase))
                                    {
                                        tblColumns[nIndex] = tblColumns[nIndex].Substring(0, tblColumns[nIndex].Length - 11).TrimEnd();
                                    }
                                    else if (tblColumns[nIndex].EndsWith(" static", StringComparison.OrdinalIgnoreCase))
                                    {
                                        ++nbrStatic;
                                    }

                                    if(tblColumns[nIndex].IndexOf(" frozen ", StringComparison.OrdinalIgnoreCase) > 3
                                        || tblColumns[nIndex].IndexOf("frozen<", StringComparison.OrdinalIgnoreCase) > 3
                                        || tblColumns[nIndex].IndexOf("<frozen", StringComparison.OrdinalIgnoreCase) > 3)
                                    {
                                        ++nbrFrozen;
                                    }

									if (tblColumns[nIndex].IndexOf(" list", StringComparison.OrdinalIgnoreCase) > 3
												|| tblColumns[nIndex].IndexOf(" map", StringComparison.OrdinalIgnoreCase) > 3
												|| tblColumns[nIndex].IndexOf(" set", StringComparison.OrdinalIgnoreCase) > 3
												|| tblColumns[nIndex].IndexOf("<list", StringComparison.OrdinalIgnoreCase) > 3
												|| tblColumns[nIndex].IndexOf("<map", StringComparison.OrdinalIgnoreCase) > 3
												|| tblColumns[nIndex].IndexOf("<set", StringComparison.OrdinalIgnoreCase) > 3)
									{
										++nbrCollections;
									}

									var typePos = tblColumns[nIndex].LastIndexOfAny(new char[] { ' ', '<' });
									var typeName = tblColumns[nIndex].Substring(typePos + 1).Trim(new char[] { ' ', '>', '<' }).ToLower();

									if (typeName == "counter")
                                    {
                                        ++nbrCounters;
                                    }
                                    else
                                    if (typeName == "blob")
                                    {
                                        ++nbrBlobs;
                                    }
									else
									{
										if(types.ContainsKey(kstblName.Item1 + "." + typeName))
										{
											++nbrType;
										}
									}
                                }

                                dataRow["Collections"] = nbrCollections;
                                dataRow["Counters"] = nbrCounters;
                                dataRow["Blobs"] = nbrBlobs;
                                dataRow["Static"] = nbrStatic;
                                dataRow["Frozen"] = nbrFrozen;
								dataRow["Type"] = nbrType;
								dataRow["Total"] = endParan;

								if(createType)
								{
									if (!types.ContainsKey(keyspacetableName))
									{
										types.Add(keyspacetableName, tblColumns);
									}
								}

								//parse options...
								if (strOtpsTbl.Length > 6)
								{
									parsedComponent = Common.StringFunctions.Split(strOtpsTbl.Substring(5).TrimStart(),
																					" and ",
																					StringComparison.OrdinalIgnoreCase,
																					Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
																					Common.StringFunctions.SplitBehaviorOptions.Default
																						| Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
									string optKeyword;

									for (int nIndex = 0; nIndex < parsedComponent.Count; ++nIndex)
									{
										optKeyword = parsedComponent[nIndex].Trim();

										if (optKeyword[optKeyword.Length - 1] == ';')
										{
											optKeyword = optKeyword.Substring(0, optKeyword.Length - 1);
										}

										if (optKeyword.StartsWith("compaction", StringComparison.OrdinalIgnoreCase))
										{
											var kwOptions = ParseKeyValuePair(optKeyword).Item2;
											var classPos = kwOptions.IndexOf("class");
											var classSplit = kwOptions.Substring(classPos).Split(new char[] { ':', ',', '}' });
											var strategy = classSplit[1].Trim();
											dataRow["Compaction Strategy"] = RemoveNamespace(strategy);
										}
										else if (optKeyword.StartsWith("compression", StringComparison.OrdinalIgnoreCase))
										{
											//AND compression = {'sstable_compression': 'org.apache.cassandra.io.compress.LZ4Compressor'}
											//compression = { 'sstable_compression' : 'Encryptor', 'cipher_algorithm' : 'AES/ECB/PKCS5Padding', 'secret_key_strength' : 128, 'chunk_length_kb' : 1}
											// AND compression = {'chunk_length_in_kb': '64', 'class': 'org.apache.cassandra.io.compress.LZ4Compressor'}
											var kwOptions = ParseKeyValuePair(optKeyword).Item2;
											var jsonItems = ParseJson(kwOptions);

											if (jsonItems.ContainsKey("cipher_algorithm"))
											{
												dataRow["Compression"] = RemoveNamespace((string)jsonItems["cipher_algorithm"]);
											}
											else if (jsonItems.ContainsKey("sstable_compression"))
											{
												dataRow["Compression"] = RemoveNamespace((string)jsonItems["sstable_compression"]);
											}
											else if (jsonItems.ContainsKey("class"))
											{
												dataRow["Compression"] = RemoveNamespace((string)jsonItems["class"]);
											}
										}
										else if (optKeyword.StartsWith("dclocal_read_repair_chance", StringComparison.OrdinalIgnoreCase))
										{
											var assignmentSignPos = optKeyword.IndexOf('=');

											if (assignmentSignPos > 0)
											{
												var numValue = optKeyword.Substring(assignmentSignPos + 1);
												decimal numObj;

												if (decimal.TryParse(numValue, out numObj))
												{
													dataRow["DC Chance"] = numObj;
												}
											}
										}
										else if (optKeyword.StartsWith("gc_grace_seconds", StringComparison.OrdinalIgnoreCase))
										{
											var assignmentSignPos = optKeyword.IndexOf('=');

											if (assignmentSignPos > 0)
											{
												var numValue = optKeyword.Substring(assignmentSignPos + 1);

												dataRow["GC Grace Period"] = new TimeSpan(0, 0, 0, int.Parse(numValue));
											}
										}
										else if (optKeyword.StartsWith("read_repair_chance", StringComparison.OrdinalIgnoreCase))
										{
											var assignmentSignPos = optKeyword.IndexOf('=');

											if (assignmentSignPos > 0)
											{
												var numValue = optKeyword.Substring(assignmentSignPos + 1);
												decimal numObj;

												if (decimal.TryParse(numValue, out numObj))
												{
													dataRow["Chance"] = numObj;
												}
											}
										}
										else if (optKeyword.StartsWith("speculative_retry", StringComparison.OrdinalIgnoreCase))
										{
											var assignmentSignPos = optKeyword.IndexOf('=');

											if (assignmentSignPos > 0)
											{
												dataRow["Policy"] = RemoveQuotes(optKeyword.Substring(assignmentSignPos + 1).Trim());
											}
										}

									}
								}

                                dtTable.Rows.Add(dataRow);
                                #endregion
                            }//end of table
                            else if (parsedValues[0].Substring(6, 6).TrimStart().ToLower() == "index" || parsedValues[0].Substring(6, 7).TrimStart().ToLower() == "custom")
                            {
                                #region index
                                //CREATE INDEX ix_configuration_effective_from ON production_mqh_config.configuration (effective_from);
                                //CREATE INDEX ON users (phones);
                                //CREATE INDEX todo_dates ON users (KEYS(todo));
                                //CREATE CUSTOM INDEX ON users (email) USING 'path.to.the.IndexClass' WITH OPTIONS = {'storage': '/mnt/ssd/indexes/'};
                                //CREATE CUSTOM INDEX taulia_invoice_invoice_business_unit_id_index ON taulia_invoice.invoice (business_unit_id) USING 'com.datastax.bdp.search.solr.Cql3SolrSecondaryIndex';

                                var splits = RegExCreateIndex.Split(cqlStr);
                                Tuple<string, string> indexKSTbl = null;
                                Tuple<string, string> ksTbl;
                                string indexCol = null;

                                if (splits.Length == 4)
                                {
                                    ksTbl = SplitTableName(splits[1], currentKeySpace);
                                    indexCol = splits[2];
                                    indexKSTbl = new Tuple<string, string>(ksTbl.Item1, ksTbl.Item2 + "." + "ix_" + indexCol);
                                }
                                else
                                {
                                    ksTbl = SplitTableName(splits[2], currentKeySpace);
                                    indexKSTbl = SplitTableName(splits[1], currentKeySpace == null ? ksTbl.Item1 : currentKeySpace);
                                    indexCol = splits[3];
                                }

                                if (ignoreKeySpaces.Contains(indexKSTbl.Item1))
                                {
                                    continue;
                                }

								var kstblName = ksTbl.Item2 + "." + indexKSTbl.Item2;

								if (dtTable.AsEnumerable().Any(i => i.Field<string>("Keyspace Name") == indexKSTbl.Item1
																				&& i.Field<string>("Name") == kstblName))
								{
									continue;
								}

								dataRow = dtTable.NewRow();
                                dataRow["Keyspace Name"] = indexKSTbl.Item1;
								dataRow["Name"] = kstblName;
								dataRow["DDL"] = cqlStr;
                                dataRow["Associated Table"] = ksTbl.Item1 + "." + ksTbl.Item2;
                                dataRow["Index"] = true;

                                var lookforUsingClause = RegExCreateIndexUsing.Split(cqlStr);

                                if (lookforUsingClause.Length > 1)
                                {
                                    dataRow["Compaction Strategy"] = RemoveNamespace(lookforUsingClause[1]);
                                }


                                var assocTblRow = dtTable.Rows.Find(new object[] { ksTbl.Item1, ksTbl.Item2 });

                                if (assocTblRow != null)
                                {
                                    var cqlDDL = assocTblRow["DDL"] as string;

                                    if (dataRow["Compaction Strategy"] == DBNull.Value)
                                    {
                                        dataRow["Compaction Strategy"] = assocTblRow["Compaction Strategy"];
                                    }

                                    if (!string.IsNullOrEmpty(cqlDDL))
                                    {
										var cqlStartColPos = cqlDDL.IndexOf("(");
										var strCols = cqlDDL.Substring(cqlStartColPos);
                                        var colPos = strCols.IndexOf(indexCol);

                                        if (colPos > 0)
                                        {
                                            var strCol = strCols.Substring(colPos);
                                            var colEndPos = strCol.IndexOfAny(new char[] { ',', ')' });

                                            if (colEndPos > 0)
                                            {
                                                dataRow["Partition Key"] = strCol.Substring(0, colEndPos);
                                            }
                                        }
                                    }
                                }

                                dtTable.Rows.Add(dataRow);
                                #endregion
                            }
                            else
                            {
                                cqlStr.Dump(Logger.DumpType.Warning, "Unrecognized CQL found in file \"{0}\"", cqlDDLFilePath.PathResolved);
                                Program.ConsoleWarnings.Increment("Unrecognized CQL:", cqlStr);
                            }
                        }
                        else
                        {
                            cqlStr.Dump(Logger.DumpType.Warning, "Unrecognized CQL found in file \"{0}\"", cqlDDLFilePath.PathResolved);
                            Program.ConsoleWarnings.Increment("Unrecognized CQL", cqlStr);
                        }
                    }
                }

				var tableStats = from dr in dtKeySpace.AsEnumerable().DuplicatesRemoved(i => i.Field<string>("Name"))
								let ksName = dr.Field<string>("Name")
								where !string.IsNullOrEmpty(ksName)
								select new
								{
									KSDataRow = dr,
									TblItems = (from tblDr in dtTable.AsEnumerable()
												where tblDr.Field<string>("Keyspace Name") == ksName
												let index = tblDr.Field<bool?>("Index")
												select new
												{
													Compaction = tblDr.Field<string>("Compaction Strategy"),
													Index = index.HasValue ? index.Value : false,
                                                    Columns = index.HasValue && index.Value ? 0 : tblDr.Field<int>("Total")
                                                })
								};

				foreach (var tblItem in tableStats)
				{
					tblItem.KSDataRow.BeginEdit();

					tblItem.KSDataRow["Tables"] = tblItem.TblItems.Count(i => !i.Index);
                    tblItem.KSDataRow["Columns"] = tblItem.TblItems.Sum(i => i.Columns);
                    tblItem.KSDataRow["Indexes"] = tblItem.TblItems.Count(i => i.Index && (string.IsNullOrEmpty(i.Compaction) || !(i.Compaction == "Cql3SolrSecondaryIndex" || i.Compaction == "ThriftSolrSecondaryIndex")));
					tblItem.KSDataRow["Total"] = tblItem.TblItems.Count();
					tblItem.KSDataRow["STCS"] = tblItem.TblItems.Count(i => !i.Index && i.Compaction == "SizeTieredCompactionStrategy");
					tblItem.KSDataRow["LCS"] = tblItem.TblItems.Count(i => !i.Index && i.Compaction == "LeveledCompactionStrategy");
					tblItem.KSDataRow["DTCS"] = tblItem.TblItems.Count(i => !i.Index && i.Compaction == "DateTieredCompactionStrategy");
					tblItem.KSDataRow["TCS"] = tblItem.TblItems.Count(i => !i.Index && i.Compaction == "TieredCompactionStrategy");
					tblItem.KSDataRow["TWCS"] = tblItem.TblItems.Count(i => !i.Index && i.Compaction == "TimeWindowCompactionStrategy");
					tblItem.KSDataRow["solr"] = tblItem.TblItems.Count(i => i.Index && (i.Compaction == "Cql3SolrSecondaryIndex" || i.Compaction == "ThriftSolrSecondaryIndex"));
					tblItem.KSDataRow["Active"] = 0;

					tblItem.KSDataRow["Other Strategies"] = (int)tblItem.KSDataRow["Tables"]
																- ((int)tblItem.KSDataRow["STCS"]
																	+ (int)tblItem.KSDataRow["LCS"]
																	+ (int)tblItem.KSDataRow["DTCS"]
																	+ (int)tblItem.KSDataRow["TCS"]
																	+ (int)tblItem.KSDataRow["TWCS"]);

					tblItem.KSDataRow.EndEdit();
				}

            }
            catch (System.Exception ex)
            {
                Logger.Instance.ErrorFormat("Error: Exception \"{0}\" ({1}) occurred while parsing file \"{2}\" Line \"{5}\" within ReadCQLDDLParseIntoDataTable for IpAddress: {3} ({4})",
                                                ex.Message,
                                                ex.GetType().Name,
                                                cqlDDLFilePath.PathResolved,
                                                ipAddress,
                                                dcName,
                                                line);
                Logger.Instance.Error("CQL DLL Parsing Error", ex);
            }
        }

        static public void UpdateCQLDDLTableActiveStatus(DataTable dtDDLTable, DataTable dtDDLKeyspace)
        {
            foreach (DataRow dataRow in dtDDLTable.Rows)
            {
                var secondaryIndex = dataRow["Index"] == DBNull.Value ? false : (bool)dataRow["Index"];

                if (secondaryIndex 
						&& ((string) dataRow["Compaction Strategy"] == "Cql3SolrSecondaryIndex"
								|| (string)dataRow["Compaction Strategy"] == "ThriftSolrSecondaryIndex"))
                {
                    continue;
                }
				else if(((string)dataRow["Name"]).EndsWith(" (Type)"))
				{
					continue;
				}
                else
                {
                    var activeTbl = ActiveTables.Contains(((string)dataRow["Keyspace Name"])
                                                                    + '.' + ((string)dataRow["Name"])
                                                                    + (secondaryIndex ? " (index)" : string.Empty));
					dataRow["Active"] = activeTbl;

					if (activeTbl && dtDDLKeyspace != null)
					{
						var ksDR = dtDDLKeyspace.AsEnumerable().FirstOrDefault(dr => dr.Field<int?>("Active").HasValue && dr.Field<string>("Name") == dataRow.Field<string>("Keyspace Name"));

						if (ksDR != null)
						{
							var ksActiveCnt = ksDR.Field<int>("Active");

							ksDR.SetField<int>("Active", ksActiveCnt + 1);
						}
					}
                }
            }
        }

    }
}
