using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    public class YamlInfo
    {
        public string YamlType;
        public string IPAddress;
        public string DCName;
        public string Cmd;
        public string CmdParams;
        public IEnumerable<Tuple<string, string>> KeyValueParams;

        public string MakeKeyValue()
        {
            return this.DCName
                        + ": "
                        + this.Cmd
                        + ": "
                        + (this.KeyValueParams == null
                                ? this.CmdParams
                                : string.Join(" ", this.KeyValueParams.Select(kvp => kvp.Item1 + ": " + kvp.Item2)));
        }

        public bool ComparerProperyOnly(YamlInfo compareItem)
        {
            return this.DCName == compareItem.DCName
                    && this.Cmd == compareItem.Cmd
                    && ((this.KeyValueParams == null && compareItem.KeyValueParams == null)
                        || (this.KeyValueParams != null
                                && compareItem.KeyValueParams != null
                                && this.KeyValueParams.Count() == compareItem.KeyValueParams.Count()
                                && this.KeyValueParams.All(item => compareItem.KeyValueParams.Where(kvp => kvp.Item1 == item.Item1).Count() > 0)));
        }

        public string ProperyName()
        {
            return this.Cmd + (this.KeyValueParams == null
                                    ? string.Empty
                                    : "." + string.Join(".", this.KeyValueParams.Select(kvp => kvp.Item1)));
        }

        public string ProperyName(int inxProperty)
        {
            return this.Cmd + (this.KeyValueParams == null || inxProperty == 0
                                    ? string.Empty
                                    : "." + this.KeyValueParams.ElementAt(inxProperty - 1).Item1);
        }

        public object ProperyValue(int inxProperty)
        {
            string strValue = this.KeyValueParams == null || inxProperty == 0
                                    ? this.CmdParams
                                    : this.KeyValueParams.ElementAt(inxProperty - 1).Item2;
            object numValue;

            if (StringFunctions.ParseIntoNumeric(strValue, out numValue))
            {
                return numValue;
            }
            else if (strValue == "false")
            {
                return false;
            }
            else if (strValue == "true")
            {
                return true;
            }

            return strValue;
        }

        public bool AddValueToDR(DataTable dtYamal)
        {
            if (this.KeyValueParams == null)
            {
                var dataRow = dtYamal.NewRow();

                if (this.AddValueToDR(dataRow, 0))
                {
                    dtYamal.Rows.Add(dataRow);
                    return true;
                }

                return false;
            }

            for (int i = 1; i <= this.KeyValueParams.Count(); i++)
            {
                var dataRow = dtYamal.NewRow();

                if (this.AddValueToDR(dataRow, i))
                {
                    dtYamal.Rows.Add(dataRow);
                }
            }

            return true;
        }

        public bool AddValueToDR(DataRow drYama, int inxProperty)
        {
            var maxIndex = this.KeyValueParams == null ? 0 : this.KeyValueParams.Count();

            if (inxProperty > maxIndex)
            {
                return false;
            }

            drYama["Yaml Type"] = this.YamlType;
            drYama["Data Center"] = this.DCName;
            drYama["Node IPAddress"] = this.IPAddress;
            drYama["Property"] = this.ProperyName(inxProperty);
            drYama["Value"] = this.ProperyValue(inxProperty);

            return true;
        }
    }

}
