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
        public static void LoadYamlRingOSInfo(Task<Tuple<DataTable, DataTable, DataTable, Common.Patterns.Collections.ThreadSafe.Dictionary<string, string>>> updateRingWYamlInfoTask,
                                                ExcelPackage excelPkg,
                                                string excelWorkSheetYaml,
                                                string excelWorkSheetRingInfo,
                                                string excelWorkSheetOSMachineInfo)
        {
            updateRingWYamlInfoTask.Wait();

            LoadYamlSettings(excelPkg, updateRingWYamlInfoTask.Result.Item3, excelWorkSheetYaml);
            LoadRingInfo(excelPkg, updateRingWYamlInfoTask.Result.Item2, excelWorkSheetRingInfo);
            LoadOSMachineInfo(excelPkg, updateRingWYamlInfoTask.Result.Item1, excelWorkSheetOSMachineInfo);
        }
    }
}
