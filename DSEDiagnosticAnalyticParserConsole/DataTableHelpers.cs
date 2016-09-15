using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    public static class DataTableHelpers
    {
        public static DataTable MergeIntoOneDataTable(this Common.Patterns.Collections.LockFree.Stack<DataTable> multipleDataTables,
                                                        Tuple<string, string, DataViewRowState> viewFilterSortRowStateOpts = null)
        {
            DataTable dtComplete = new DataTable();
            DataTable dtItem;
            DataRow[] dtErrors;
            bool firstDT = true;
            string firstTableName = null;
            string lastTableName = null;
            long rowCount = 0;
            int nbrMergedTables = 0;

            dtComplete.BeginLoadData();

            while (multipleDataTables.Pop(out dtItem))
            {
                dtItem.AcceptChanges();
                ++nbrMergedTables;

                if (firstDT)
                {
                    dtItem
                        .Columns
                        .Cast<DataColumn>()
                        .ForEach(dc => dtComplete.Columns.Add(dc.ColumnName, dc.DataType).AllowDBNull = dc.AllowDBNull);
                    firstDT = dtComplete.Columns.Count == 0;
                    firstTableName = dtItem.TableName;
                }

                lastTableName = dtItem.TableName;

                if (dtItem.Rows.Count == 0)
                    continue;

                rowCount += dtItem.Rows.Count;
                dtErrors = dtItem.GetErrors();
                if (dtErrors.Length > 0)
                {
                    dtErrors.Dump(Logger.DumpType.Error, "Table \"{0}\" Has Error", dtItem.TableName);
                }

                using (var dtReader = dtItem.CreateDataReader())
                {
                    dtComplete.Load(dtReader);
                }
            }

            dtComplete.EndLoadData();
            dtComplete.AcceptChanges();

            dtComplete.TableName = new string(firstTableName.ToCharArray()
                                                .Intersect(lastTableName.ToCharArray()).ToArray()) + "-AllRows";

            if(dtComplete.Rows.Count != rowCount)
            {
                Logger.Instance.ErrorFormat("Row Counts do not match when merging from {0} tables into one table. First/Last tables are: \"{1}\"/\"{2}\". Total Rows: {3} Merged Rows: {4}",
                                                nbrMergedTables,
                                                firstTableName,
                                                lastTableName,
                                                rowCount,
                                                dtComplete.Rows.Count);
            }

            if (viewFilterSortRowStateOpts != null)
            {
                var tableName = dtComplete.TableName;
                dtComplete = (new DataView(dtComplete,
                                            viewFilterSortRowStateOpts.Item1,
                                            viewFilterSortRowStateOpts.Item2,
                                            viewFilterSortRowStateOpts.Item3))
                            .ToTable();
                dtComplete.TableName = tableName + "-Filtered";
            }

            return dtComplete;
        }

        public static IEnumerable<DataTable> SplitTable(this DataTable dtComplete, int nbrRowsInSubTables)
        {
            var dtSplits = new List<DataTable>();
            var dtCurrent = new DataTable(dtComplete.TableName + "-Split-0");
            int totalRows = 0;
            long rowNbr = 0;

            if (dtComplete.Rows.Count <= nbrRowsInSubTables)
            {
                dtSplits.Add(dtComplete);
                return dtSplits;
            }

            dtComplete
                .Columns
                .Cast<DataColumn>()
                .ForEach(dc => dtCurrent.Columns.Add(dc.ColumnName, dc.DataType).AllowDBNull = dc.AllowDBNull);

            dtCurrent.BeginLoadData();

            foreach (DataRow drSource in dtComplete.Rows)
            {
                if (totalRows > nbrRowsInSubTables)
                {
                    dtCurrent.EndLoadData();
                    dtSplits.Add(dtCurrent);
                    dtCurrent = new DataTable(dtComplete.TableName + "-Split-" + rowNbr);
                    dtComplete
                        .Columns
                        .Cast<DataColumn>()
                        .ForEach(dc => dtCurrent.Columns.Add(dc.ColumnName, dc.DataType).AllowDBNull = dc.AllowDBNull);
                    dtCurrent.BeginLoadData();
                    totalRows = 0;
                }

                dtCurrent.LoadDataRow(drSource.ItemArray, LoadOption.OverwriteChanges);
                ++totalRows;
                ++rowNbr;
            }

            dtCurrent.EndLoadData();
            dtSplits.Add(dtCurrent);

            return dtSplits;
        }
    }
}
