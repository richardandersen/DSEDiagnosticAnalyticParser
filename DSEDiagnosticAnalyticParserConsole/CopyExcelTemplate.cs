using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class DTLoadIntoExcel
    {
        public static void CopyExcelTemplate(string excelTemplateFilePath, string excelFilePath)
        {
            if (!string.IsNullOrEmpty(ParserSettings.ExcelTemplateFilePath))
            {
                var excelTemplateFile = Common.Path.PathUtils.BuildFilePath(excelTemplateFilePath);
                var excelFile = Common.Path.PathUtils.BuildFilePath(excelFilePath);

                if (!excelFile.Exist()
                        && excelTemplateFile.Exist())
                {
                    if (excelTemplateFile.Copy(excelFile))
                    {
                        Logger.Instance.InfoFormat("Created Workbook \"{0}\" from Template \"{1}\"", excelFile.Path, excelTemplateFile.Path);
                    }
                }
            }
        }

    }
}
