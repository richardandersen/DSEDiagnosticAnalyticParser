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
                dtKeySpace.Columns.Add("Views", typeof(int)).AllowDBNull = true;//f
                dtKeySpace.Columns.Add("Columns", typeof(int)).AllowDBNull = true;//g
                dtKeySpace.Columns.Add("Secondary Indexes", typeof(int)).AllowDBNull = true;//h
				dtKeySpace.Columns.Add("solr Indexes", typeof(int)).AllowDBNull = true;
                dtKeySpace.Columns.Add("SAS Indexes", typeof(int)).AllowDBNull = true;
                dtKeySpace.Columns.Add("Custom Indexes", typeof(int)).AllowDBNull = true;
                dtKeySpace.Columns.Add("Triggers", typeof(int)).AllowDBNull = true;
                dtKeySpace.Columns.Add("Total", typeof(int)).AllowDBNull = true;//m
				dtKeySpace.Columns.Add("Active", typeof(int)).AllowDBNull = true;//n
				dtKeySpace.Columns.Add("STCS", typeof(int)).AllowDBNull = true;//o
				dtKeySpace.Columns.Add("LCS", typeof(int)).AllowDBNull = true;//p
				dtKeySpace.Columns.Add("DTCS", typeof(int)).AllowDBNull = true;//q
				dtKeySpace.Columns.Add("TCS", typeof(int)).AllowDBNull = true;//r
				dtKeySpace.Columns.Add("TWCS", typeof(int)).AllowDBNull = true;//s
				dtKeySpace.Columns.Add("Other Strategies", typeof(int)).AllowDBNull = true;//t
				dtKeySpace.Columns.Add("DDL", typeof(string));//u

                dtKeySpace.PrimaryKey = new System.Data.DataColumn[] { dtKeySpace.Columns["Name"], dtKeySpace.Columns["Data Center"] };
            }

            if (dtTable.Columns.Count == 0)
            {
                dtTable.Columns.Add("Active", typeof(bool));//a
                dtTable.Columns.Add("Keyspace Name", typeof(string));//b
                dtTable.Columns.Add("Name", typeof(string));
                dtTable.Columns.Add("Type", typeof(string)).AllowDBNull = false; //d
                dtTable.Columns.Add("Partition Key", typeof(string)).AllowDBNull = true;
                dtTable.Columns.Add("Cluster Key", typeof(string)).AllowDBNull = true;
                dtTable.Columns.Add("Compaction Strategy", typeof(string)).AllowDBNull = true;
                dtTable.Columns.Add("Compression", typeof(string)).AllowDBNull = true; //h
                dtTable.Columns.Add("Chance", typeof(decimal)).AllowDBNull = true;//i
                dtTable.Columns.Add("DC Chance", typeof(decimal)).AllowDBNull = true;//j
                dtTable.Columns.Add("Policy", typeof(string)).AllowDBNull = true;//k
                dtTable.Columns.Add("GC Grace Period", typeof(TimeSpan)).AllowDBNull = true;//l
                dtTable.Columns.Add("TTL", typeof(int)).AllowDBNull = true; //m
                dtTable.Columns.Add("Collections", typeof(int)).AllowDBNull = true;//n
                dtTable.Columns.Add("Counters", typeof(int)).AllowDBNull = true;//o
                dtTable.Columns.Add("Blobs", typeof(int)).AllowDBNull = true;//p
                dtTable.Columns.Add("Static", typeof(int)).AllowDBNull = true;//q
                dtTable.Columns.Add("Frozen", typeof(int)).AllowDBNull = true;//r
                dtTable.Columns.Add("Tuple", typeof(int)).AllowDBNull = true;//s
                dtTable.Columns.Add("UDT", typeof(int)).AllowDBNull = true;//t
				dtTable.Columns.Add("Total", typeof(int)).AllowDBNull = true;//u
                dtTable.Columns.Add("HasOrderBy", typeof(bool)).AllowDBNull = true;//v
                dtTable.Columns.Add("Associated Table", typeof(string)).AllowDBNull = true;//w
                dtTable.Columns.Add("Index", typeof(bool)).AllowDBNull = true; //x
                dtTable.Columns.Add("DDL", typeof(string));//z

                dtTable.PrimaryKey = new System.Data.DataColumn[] { dtTable.Columns["Keyspace Name"], dtTable.Columns["Name"] };
            }
        }
        static public void ReadParseCQLDDLParse(IFilePath cqlDDLFilePath)
        {            
            try
            {
                var ddlParser = new DSEDiagnosticFileParser.cql_ddl(cqlDDLFilePath,
                                                                        "dseCluster",
                                                                        "Local");

                ddlParser.ProcessFile();
                ddlParser.Task?.Wait();
            }
            catch (System.Exception ex)
            {
                Logger.Instance.ErrorFormat("Error: Exception \"{0}\" ({1}) occurred while parsing file \"{2}\" within ReadParseCQLDDLParse",
                                                ex.Message,
                                                ex.GetType().Name,
                                                cqlDDLFilePath.PathResolved);
                Logger.Instance.Error("CQL DLL Parsing Error", ex);
            }
        }

        static public void ProcessCQLDDLIntoDataTable(DataTable dtKeySpace,
                                                         DataTable dtTable,
                                                         IEnumerable<string> ignoreKeySpaces)
        {
            InitializeCQLDDLDataTables(dtKeySpace, dtTable);
            DataRow dataRow;

            foreach(var keySpace in DSEDiagnosticLibrary.Cluster.MasterCluster.Keyspaces)
            {
                if(ignoreKeySpaces.Any(n => n == keySpace.Name))
                {
                    continue;
                }
                
                if(keySpace.LocalStrategy || keySpace.EverywhereStrategy)
                {
                    if (!dtKeySpace.Rows.Contains(new object[] { keySpace.Name, keySpace.DataCenter.Name }))
                    {
                        dataRow = dtKeySpace.NewRow();
                        dataRow["Name"] = keySpace.Name;
                        dataRow["Replication Strategy"] = keySpace.ReplicationStrategy;
                        dataRow["Data Center"] = keySpace.DataCenter.Name;
                        dataRow["Replication Factor"] = 0;
                        dataRow["DDL"] = keySpace.DDL;
                        dtKeySpace.Rows.Add(dataRow);
                    }
                }
                else
                {
                    bool firstRepl = true;

                    foreach(var replication in keySpace.Replications)
                    {
                        if (dtKeySpace.Rows.Contains(new object[] { keySpace.Name, replication.DataCenter.Name }))
                        {
                            continue;
                        }

                        dataRow = dtKeySpace.NewRow();
                        dataRow["Name"] = keySpace.Name;
                        dataRow["Replication Strategy"] = keySpace.ReplicationStrategy;
                        dataRow["Data Center"] = replication.DataCenter.Name;
                        dataRow["Replication Factor"] = (int) replication.RF;
                        dataRow["DDL"] = keySpace.DDL;

                        if(firstRepl)
                        {
                            firstRepl = false;

                            dataRow["Tables"] = (int) keySpace.Stats.Tables;
                            dataRow["Views"] = (int) keySpace.Stats.MaterialViews;
                            dataRow["Columns"] = (int) keySpace.Stats.Columns;
                            dataRow["Secondary Indexes"] = (int)keySpace.Stats.SecondaryIndexes;
                            dataRow["solr Indexes"] = (int)keySpace.Stats.SolrIndexes;
                            dataRow["SAS Indexes"] = (int)keySpace.Stats.SasIIIndexes;
                            dataRow["Custom Indexes"] = (int)keySpace.Stats.CustomIndexes;
                            dataRow["Triggers"] = (int) keySpace.Stats.Triggers;
                            dataRow["Total"] = (int)(keySpace.Stats.Tables
                                                        + keySpace.Stats.MaterialViews
                                                        + keySpace.Stats.SecondaryIndexes
                                                        + keySpace.Stats.SolrIndexes
                                                        + keySpace.Stats.SasIIIndexes
                                                        + keySpace.Stats.CustomIndexes);
                            dataRow["STCS"] = (int)keySpace.Stats.STCS;
                            dataRow["LCS"] = (int)keySpace.Stats.LCS;
                            dataRow["DTCS"] = (int)keySpace.Stats.DTCS;
                            dataRow["TCS"] = (int)keySpace.Stats.TCS;
                            dataRow["TWCS"] = (int)keySpace.Stats.TWCS;
                            dataRow["Other Strategies"] = (int)keySpace.Stats.OtherStrategies;
                            dataRow["Active"] = 0;
                        }

                        dtKeySpace.Rows.Add(dataRow);
                    }
                }

                foreach(var ddlItem in keySpace.DDLs)
                {
                    if (dtTable.Rows.Contains(new object[] { keySpace.Name, ddlItem.Name }))
                    {
                        continue;
                    }

                    dataRow = dtTable.NewRow();

                    dataRow["Keyspace Name"] = keySpace.Name;
                    dataRow["Name"] = ddlItem.Name;
                    dataRow["Type"] = ddlItem.GetType().Name;
                    dataRow["DDL"] = ddlItem.DDL;
                    dataRow["Total"] = ddlItem.Items;

                    if (ddlItem is DSEDiagnosticLibrary.ICQLTrigger)
                    {
                        dataRow["Associated Table"] = ((DSEDiagnosticLibrary.ICQLTrigger)ddlItem).Table.FullName;
                    }
                    else if(ddlItem is DSEDiagnosticLibrary.ICQLIndex)
                    {
                        dataRow["Associated Table"] = ((DSEDiagnosticLibrary.ICQLIndex)ddlItem).Table.FullName;
                        dataRow["Index"] = true;
                        dataRow["Partition Key"] = string.Join(", ", ((DSEDiagnosticLibrary.ICQLIndex)ddlItem).Columns
                                                                        .Select(cf => cf.PrettyPrint()));
                        if (!string.IsNullOrEmpty(((DSEDiagnosticLibrary.ICQLIndex)ddlItem).UsingClass))                        
                        {
                            dataRow["Compaction Strategy"] = RemoveNamespace(((DSEDiagnosticLibrary.ICQLIndex)ddlItem).UsingClass);
                        }
                    }
                    else if(ddlItem is DSEDiagnosticLibrary.ICQLUserDefinedType)
                    {
                        dataRow["Name"] = ddlItem.Name + " (Type)";
                        dataRow["Collections"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.IsCollection);
                        dataRow["Counters"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.IsCounter);
                        dataRow["Blobs"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.IsBlob);
                        dataRow["Static"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.IsStatic);
                        dataRow["Frozen"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.IsFrozen);
                        dataRow["Tuple"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.IsTuple);
                        dataRow["UDT"] = ((DSEDiagnosticLibrary.ICQLUserDefinedType)ddlItem).Columns.Count(c => c.CQLType.IsUDT);
                    }
                    else if (ddlItem is DSEDiagnosticLibrary.ICQLTable)
                    {
                        dataRow["Total"] = (int) ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.NbrColumns;
                        dataRow["Collections"] = (int) ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Collections;
                        dataRow["Counters"] = (int) ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Counters;
                        dataRow["Blobs"] = (int) ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Blobs;
                        dataRow["Static"] = (int) ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Statics;
                        dataRow["Frozen"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Frozens;
                        dataRow["Tuple"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.Tuples;
                        dataRow["UDT"] = (int)((DSEDiagnosticLibrary.ICQLTable)ddlItem).Stats.UDTs;
                        dataRow["HasOrderBy"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).OrderByCols.HasAtLeastOneElement();

                        dataRow["Partition Key"] = string.Join(",", ((DSEDiagnosticLibrary.ICQLTable)ddlItem).PrimaryKeys.Select(k => k.Name + ' ' + k.CQLType.Name));
                        dataRow["Cluster Key"] = string.Join(",", ((DSEDiagnosticLibrary.ICQLTable)ddlItem).ClusteringKeys.Select(k => k.Name + ' ' + k.CQLType.Name));
                        dataRow["Compaction Strategy"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Compaction;
                        dataRow["Compression"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).Compression;
                        dataRow["Chance"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("read_repair_chance");
                        dataRow["DC Chance"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("dclocal_read_repair_chance");
                        dataRow["Policy"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("speculative_retry");
                        dataRow["GC Grace Period"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("gc_grace_seconds");
                        dataRow["TTL"] = ((DSEDiagnosticLibrary.ICQLTable)ddlItem).GetPropertyValue("default_time_to_live");
                        
                        if (ddlItem is DSEDiagnosticLibrary.ICQLMaterializedView)
                        {
                            dataRow["Associated Table"] = ((DSEDiagnosticLibrary.ICQLMaterializedView)ddlItem).Table.FullName;
                        }
                    }

                    dtTable.Rows.Add(dataRow);
                }
            }
        }

        static public void UpdateCQLDDLTableActiveStatus(DataTable dtDDLTable, DataTable dtDDLKeyspace)
        {
            foreach (DataRow dataRow in dtDDLTable.Rows)
            {
                var secondaryIndex = dataRow.IsNull("Index") ? false : (bool)dataRow["Index"];

                if (secondaryIndex 
                        && !dataRow.IsNull("Compaction Strategy")
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

        static object GetPropertyValue(this DSEDiagnosticLibrary.ICQLTable table, string key)
        {
            object value = null;

            if(table.Properties.TryGetValue(key, out value))
            {
                if(value is DSEDiagnosticLibrary.UnitOfMeasure)
                {
                    return (((DSEDiagnosticLibrary.UnitOfMeasure)value).UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.TimeUnits) != 0
                                ? (object) (TimeSpan?)((DSEDiagnosticLibrary.UnitOfMeasure)value)
                                : (object) ((DSEDiagnosticLibrary.UnitOfMeasure)value).Value;
                }
                return value;
            }
            return null;
        }
    }
}
