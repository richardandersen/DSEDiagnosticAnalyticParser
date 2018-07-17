using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using DSEDiagnosticToDataTable;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
        static public void initializeCFStatsDataTable(DataTable dtCFStats)
        {
            if (dtCFStats.Columns.Count == 0)
            {
                dtCFStats.Columns.Add("Source", typeof(string));
                dtCFStats.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCFStats.Columns.Add("Node IPAddress", typeof(string));
                dtCFStats.Columns.Add("KeySpace", typeof(string));
                dtCFStats.Columns.Add("Table", typeof(string)).AllowDBNull = true;
                dtCFStats.Columns.Add("Attribute", typeof(string));
                dtCFStats.Columns.Add("Value", typeof(object));
                dtCFStats.Columns.Add("Unit of Measure", typeof(string)).AllowDBNull = true;

                dtCFStats.Columns.Add("Size in MB", typeof(decimal)).AllowDBNull = true;
                dtCFStats.Columns.Add("Active", typeof(bool)).AllowDBNull = true;
                dtCFStats.Columns.Add("(Value)", typeof(object));
                dtCFStats.Columns.Add("Reconciliation Reference", typeof(object)).AllowDBNull = true;

                //dtCFStats.PrimaryKey = new System.Data.DataColumn[] { dtFSStats.Columns[0],  dtFSStats.Columns[1],  dtFSStats.Columns[2],  dtFSStats.Columns[3], dtFSStats.Columns[4] };
            }

        }

        static public void ReadCFStatsFileParseIntoDataTable(IFilePath cfstatsFilePath,
                                                                string ipAddress,
                                                                string dcName,
                                                                System.Data.DataTable dtCFStats,
                                                                IEnumerable<string> ignoreKeySpaces,
                                                                IEnumerable<string> addToMBColumn)
        {

            initializeCFStatsDataTable(dtCFStats);

            var fileInfo = new DSEDiagnosticFileParser.file_nodetool_cfstats(cfstatsFilePath, "dseCluster", dcName);
            fileInfo.IgnoreWarningsErrosInKeySpaces = ignoreKeySpaces;

            fileInfo.ProcessFile();
            fileInfo.Task?.Wait();

            if(fileInfo.NbrWarnings > 0)
            {
                Program.ConsoleWarnings.Increment("CFStats Parsing Warnings Detected");
            }
            if (fileInfo.NbrErrors > 0)
            {
                Program.ConsoleErrors.Increment("CFStats Parsing Exceptions Detected");
            }

            DataRow dataRow;
            var tblWarningLabels = Properties.Settings.Default.TableUseWarning.ToArray();
            var node = fileInfo.Node;
            var statCollection = node.AggregatedStats.Where(i => i.Class.HasFlag(DSEDiagnosticLibrary.EventClasses.KeyspaceTableViewIndexStats | DSEDiagnosticLibrary.EventClasses.Node));
            bool warn = false;

            foreach (var stat in statCollection)
            {                
                warn = false;

                if (stat.Keyspace != null && tblWarningLabels.Any(n => n == stat.Keyspace.Name || (stat.TableViewIndex != null && stat.TableViewIndex.FullName == n)))
                {
                    warn = stat.Data.Any(s => ((stat.TableViewIndex == null && (s.Key == "Read Count" || s.Key == "Write Count"))
                                                || (stat.TableViewIndex != null && (s.Key == "Local read count" || s.Key == "Local write count"))) && (dynamic)s.Value > 0);
                }

                if (!warn && stat.Keyspace != null && ignoreKeySpaces.Any(n => n == stat.Keyspace.Name))
                {
                    continue;
                }

                var keyspaceName = warn ? stat.Keyspace.Name + " (Warning)" : (stat.Keyspace?.Name ?? "<KS does not Exist>");

                {
                    object errorValue;

                    if (stat.Data.TryGetValue(DSEDiagnosticLibrary.AggregatedStats.DCNotInKS, out errorValue))
                    {
                        keyspaceName += string.Format(" {{!{0}}}", errorValue);
                        dataRow = dtCFStats.NewRow();
                        
                        dataRow.SetField("Source", stat.Source.ToString());
                        dataRow.SetField("Data Center", stat.DataCenter.Name);
                        dataRow.SetField("Node IPAddress", stat.Node.Id.NodeName());
                        dataRow.SetField("KeySpace", keyspaceName);
                        if (stat.TableViewIndex != null)
                        {
                            dataRow.SetField("Table", warn ? stat.TableViewIndex.Name + " (Warning)" : stat.TableViewIndex.Name);
                            dataRow.SetField("Active", stat.TableViewIndex.IsActive);
                        }

                        dataRow.SetField("Attribute", DSEDiagnosticLibrary.AggregatedStats.DCNotInKS);
                        dataRow.SetField("Value",
                                            string.Format("{0} not found within Keyspace \"{1}\"",
                                                            errorValue.ToString(),
                                                            stat.Keyspace.Name));

                        dtCFStats.Rows.Add(dataRow);
                    }                    
                }

                foreach (var item in stat.Data)
                {                    
                    if (item.Key == DSEDiagnosticLibrary.AggregatedStats.DCNotInKS)
                    {
                        continue;
                    }

                    if (item.Key.StartsWith(DSEDiagnosticLibrary.AggregatedStats.Error))
                    {
                        foreach (var strError in (IList<string>)item.Value)
                        {
                            dataRow = dtCFStats.NewRow();

                            dataRow.SetField("Source", stat.Source.ToString());
                            dataRow.SetField("Data Center", stat.DataCenter.Name);
                            dataRow.SetField("Node IPAddress", stat.Node.Id.NodeName());
                            dataRow.SetField("KeySpace", keyspaceName);
                            if (stat.TableViewIndex != null)
                            {
                                dataRow.SetField("Table", warn ? stat.TableViewIndex.Name + " (Warning)" : stat.TableViewIndex.Name);
                                dataRow.SetField("Active", stat.TableViewIndex.IsActive);
                            }

                            dataRow.SetField("Attribute", item.Key);
                            dataRow.SetField("Value", strError);

                            dtCFStats.Rows.Add(dataRow);
                        }

                        continue;
                    }

                    if (warn && !item.Key.StartsWith("Local read") && !item.Key.StartsWith("Local write"))
                    {
                        continue;
                    }

                    dataRow = dtCFStats.NewRow();

                    dataRow.SetField("Source", "CFStats");
                    dataRow.SetField("Data Center", stat.DataCenter.Name);
                    dataRow.SetField("Node IPAddress", stat.Node.Id.NodeName());
                    dataRow.SetField("KeySpace", keyspaceName);

                    if (stat.TableViewIndex != null)
                    {
                        var itemName = stat.TableViewIndex.Name;

                        if (stat.TableViewIndex is DSEDiagnosticLibrary.ICQLIndex)
                            itemName += " (index)";
                        else if (stat.TableViewIndex is DSEDiagnosticLibrary.ICQLIndex)
                            itemName += " (mv)";

                        dataRow.SetField("Table", warn ? itemName + " (Warning)" : itemName);                        
                    }
                    
                    dataRow.SetField("Attribute", item.Key);

                    if (item.Value is DSEDiagnosticLibrary.UnitOfMeasure)
                    {
                        DSEDiagnosticLibrary.UnitOfMeasure uom = (DSEDiagnosticLibrary.UnitOfMeasure)item.Value;
                        if (uom.Value % 1 == 0)
                        {
                            dataRow.SetFieldToLong("Value", uom);
                            dataRow.SetFieldToULong("(Value)", uom);
                        }
                        else
                        {
                            dataRow.SetFieldToDecimal("Value", uom);
                            dataRow.SetFieldToDecimal("(Value)", uom);
                        }

                        dataRow.SetField("Unit of Measure", uom.UnitType.ToString());

                        if ((uom.UnitType & DSEDiagnosticLibrary.UnitOfMeasure.Types.SizeUnits) != 0)
                        {
                            dataRow.SetField("Size in MB", uom.ConvertSizeUOM(DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB));
                        }
                    }
                    else
                    {                        
                        dataRow.SetField("Value", item.Value);

                        if(item.Value is string)
                        { }
                        else if((dynamic)item.Value < 0)
                        {
                            unchecked
                            {
                                dataRow.SetField("(Value)", (ulong)item.Value);                                
                            }
                        }
                        else
                        {
                            dataRow.SetField("(Value)", item.Value);
                        }
                    }

                    if (stat.ReconciliationRefs.HasAtLeastOneElement())
                    {
                        dataRow.SetFieldStringLimit(ColumnNames.ReconciliationRef,
                                                        stat.ReconciliationRefs.IsMultiple()
                                                                ? (object)string.Join(",", stat.ReconciliationRefs)
                                                                : (object)stat.ReconciliationRefs.First());
                    }
                    
                    dtCFStats.Rows.Add(dataRow);
                }
            }

            ReadSolrIndexFileParseIntoDataTable(cfstatsFilePath, ipAddress, dcName, dtCFStats, ignoreKeySpaces, addToMBColumn);
        }


        static private void ReadSolrIndexFileParseIntoDataTable(IFilePath cfstatsFilePath,
                                                                string ipAddress,
                                                                string dcName,
                                                                System.Data.DataTable dtCFStats,
                                                                IEnumerable<string> ignoreKeySpaces,
                                                                IEnumerable<string> addToMBColumn)
        {
            IFilePath jsonFilePath;

            if (cfstatsFilePath.ParentDirectoryPath.MakeFile(Properties.Settings.Default.SolrJsonIndexSizeFilePath, out jsonFilePath)
                    && jsonFilePath.Exist())
            {
                var indexInfoDict = ParseJson(jsonFilePath.ReadAllText());                
                DataRow dataRow;
                
                foreach (var kvItem in indexInfoDict)
                {
                    try
                    {
                        var kstblName = SplitTableName(kvItem.Key);
                        decimal? indexSizeMB = null;

                        if (ignoreKeySpaces != null && ignoreKeySpaces.Contains(kstblName.Item1))
                        {
                            continue;
                        }

                        dataRow = dtCFStats.NewRow();

                        dataRow["Source"] = "Solr Json";
                        dataRow["Data Center"] = dcName;
                        dataRow["Node IPAddress"] = ipAddress;
                        dataRow["KeySpace"] = kstblName.Item1;
                        dataRow["Table"] = kstblName.Item2;
                        dataRow["Attribute"] = "Solr Index Storage Size";
                        dataRow["Value"] = kvItem.Value;
                        dataRow["Unit of Measure"] = "byte";

                        if (kvItem.Value != null)
                        {                        
                            unchecked
                            {
                                dataRow["(Value)"] = ((dynamic)kvItem.Value) < 0 ? (ulong)((dynamic)kvItem.Value) : kvItem.Value;
                            }
                            dataRow["Size in MB"] = indexSizeMB = ((decimal)((dynamic)kvItem.Value)) / BytesToMB;                            
                        }
                        
                        dtCFStats.Rows.Add(dataRow);

                        if(indexSizeMB.HasValue)
                        {
                            var totalStorageSize = (from r in dtCFStats.AsEnumerable()
                                                    where r.Field<string>("Attribute") == "Space used (total)"
                                                             && r.Field<string>("Node IPAddress") == ipAddress
                                                             && r.Field<string>("KeySpace") == kstblName.Item1
                                                             && r.Field<string>("Table") == kstblName.Item2
                                                             && !r.IsNull("Size in MB")
                                                    select r.Field<decimal>("Size in MB"))
                                                    .DefaultIfEmpty()
                                                    .Sum();
                            var storageRatio = totalStorageSize == 0 ? 1m : (indexSizeMB.Value / totalStorageSize);

                            dataRow = dtCFStats.NewRow();

                            dataRow["Source"] = "Solr";
                            dataRow["Data Center"] = dcName;
                            dataRow["Node IPAddress"] = ipAddress;
                            dataRow["KeySpace"] = kstblName.Item1;
                            dataRow["Table"] = kstblName.Item2;
                            dataRow["Attribute"] = "Solr Index Storage Size Ratio";
                            dataRow["Value"] = storageRatio;
                            dataRow["Unit of Measure"] = "ratio";
                            dataRow["(Value)"] = storageRatio;
                            
                            dtCFStats.Rows.Add(dataRow);
                        }
                    }
                    catch (System.Exception ex)
                    {
                        Logger.Instance.Error(string.Format("Parsing for solr index size for Node {0} failed during parsing of item \"{1}, {2}\". item skipped.",
                                                        ipAddress,
                                                        kvItem.Key,
                                                        kvItem.Value),
                                                ex);
                        Program.ConsoleWarnings.Increment("solr index size Parsing Exception; Item Skipped");
                    }
                }

            }
            

        }

        static public void ReadCFStatsFileForKeyspaceTableInfo(IFilePath cfstatsFilePath,
                                                                IEnumerable<string> ignoreKeySpaces,
                                                                List<CKeySpaceTableNames> kstblNames)
        {
            var fileLines = cfstatsFilePath.ReadAllLines();
            string line;
            List<string> parsedLine;
            string currentKS = null;
            string currentTbl = null;
            bool isIndex = false;

            foreach (var element in fileLines)
            {
                line = element.Trim();

                if (!string.IsNullOrEmpty(line) && line[0] != '-')
                {
                    parsedLine = Common.StringFunctions.Split(line,
                                                                ':',
                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                Common.StringFunctions.SplitBehaviorOptions.Default);

                    if (parsedLine[0] == "Keyspace")
                    {
                        if (ignoreKeySpaces != null && ignoreKeySpaces.Contains(parsedLine[1]))
                        {
                            currentKS = null;
                        }
                        else
                        {
                            currentKS = parsedLine[1];
                        }
                        currentTbl = null;
                        isIndex = false;
                    }
                    else if (currentKS == null)
                    {
                        continue;
                    }
                    else if (parsedLine[0] == "Table")
                    {
                        currentTbl = parsedLine[1];
                        isIndex = false;
                    }
                    else if (parsedLine[0] == "Table (index)")
                    {
                        currentTbl = parsedLine[1];
                        isIndex = true;
                    }
                    else
                    {
                        if (!string.IsNullOrEmpty(currentKS) && !string.IsNullOrEmpty(currentTbl))
                        {
                            kstblNames.Add(new CKeySpaceTableNames(currentKS, currentTbl, isIndex));
                        }
                    }
                }
            }
        }

        static public Common.Patterns.Collections.ThreadSafe.List<string> ActiveTables = new Common.Patterns.Collections.ThreadSafe.List<string>();

        static public void UpdateTableActiveStatus(System.Data.DataTable dtCFStats)
        {
            var activeTblView = from r in dtCFStats.AsEnumerable()
                                let tableName = r.Field<string>("Table")
                                where (tableName != null && tableName.EndsWith(" (index)")
                                            ? r.Field<string>("Attribute") == "Local read count"
                                            : r.Field<string>("Attribute") == "Local read count"
                                                    || r.Field<string>("Attribute") == "Local write count")
                                        && r.Field<dynamic>("Value") > 0
                                group r by new { ks = r.Field<string>("KeySpace"), tbl = tableName } into g
                                select g.Key.ks + '.' + g.Key.tbl;

            ActiveTables.AddRange(activeTblView);

            foreach (DataRow dataRow in dtCFStats.Rows)
            {
                if (dataRow["Table"] != DBNull.Value)
                {
					if (dataRow["KeySpace"] == DBNull.Value)
					{
						dataRow["Active"] = ActiveTables.Contains(dataRow.Field<string>("Table"));
					}
					else
					{
						dataRow["Active"] = ActiveTables.Contains(((string)dataRow["KeySpace"]) + '.' + ((string)dataRow["Table"]));
					}
                }
            }
        }
    }
}
