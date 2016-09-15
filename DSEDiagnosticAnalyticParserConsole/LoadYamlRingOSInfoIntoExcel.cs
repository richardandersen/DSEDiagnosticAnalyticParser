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
        public static void LoadYamlRingOSInfo(Task updateRingWYamlInfoTask,
                                                ExcelPackage excelPkg,
                                                DataTable dtYaml,
                                                string excelWorkSheetYaml,
                                                DataTable dtRingInfo,
                                                string excelWorkSheetRingInfo,
                                                DataTable dtOSMachineInfo,
                                                string excelWorkSheetOSMachineInfo)
        {
            updateRingWYamlInfoTask?.Wait();

            LoadYamlSettings(excelPkg, dtYaml, excelWorkSheetYaml);
            LoadRingInfo(excelPkg, dtRingInfo, excelWorkSheetRingInfo);
            LoadOSMachineInfo(excelPkg, dtOSMachineInfo, excelWorkSheetOSMachineInfo);
        }
    }
}
