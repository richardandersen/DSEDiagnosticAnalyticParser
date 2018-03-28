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
        static public bool ReadDSEToolRingFileParseIntoDataTable(IFilePath dseRingFilePath,
                                                                    DataTable dtRingInfo)
        {
			InitializeRingDataTable(dtRingInfo);

            var fileInfo = new DSEDiagnosticFileParser.file_dsetool_ring(dseRingFilePath, "dseCluster", null);

            fileInfo.ProcessFile();
            fileInfo.Task?.Wait();

            DataRow dataRow;
            bool newRow = false;

            foreach (var node in DSEDiagnosticLibrary.Cluster.MasterCluster.Nodes)
            {
                dataRow = dtRingInfo.Rows.Find(node.Id.NodeName());

                if (newRow = (dataRow == null))
                {
                    dataRow = dtRingInfo.NewRow();                   
                }

                dataRow.BeginEdit();

                if (newRow)
                {
                    dataRow.SetField("Data Center", node.DataCenter.Name);
                    dataRow.SetField("Node IPAddress", node.Id.NodeName());
                    dataRow.SetField("Rack", node.DSE.Rack);
                    dataRow.SetField("Status", node.DSE.Statuses.ToString());
                }

                dataRow.SetField("Instance Type", node.DSE.InstanceType.ToString());
                dataRow.SetFieldToDecimal("Storage Used (MB)", node.DSE.StorageUsed, DSEDiagnosticLibrary.UnitOfMeasure.Types.MiB)
                                    .SetFieldToDecimal("Storage Utilization", node.DSE.StorageUtilization, DSEDiagnosticLibrary.UnitOfMeasure.Types.Unknown, true);
                dataRow.SetField("Health Rating", node.DSE.HealthRating);
                if (node.DSE.NbrTokens.HasValue) dataRow.SetField("Nbr VNodes", (int)node.DSE.NbrTokens.Value);

                dataRow.EndEdit();

                if(newRow)
                {
                    dtRingInfo.Rows.Add(dataRow);
                }

                dataRow.AcceptChanges();
            }

            return true;
        }

    }
}
