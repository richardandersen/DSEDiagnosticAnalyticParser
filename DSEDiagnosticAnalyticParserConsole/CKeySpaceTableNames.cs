using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;

namespace DSEDiagnosticAnalyticParserConsole
{
    public class CKeySpaceTableNames
    {
        public CKeySpaceTableNames(string ksName, string tblName)
        {
            if (tblName != null && tblName.EndsWith("(index)"))
            {
                tblName = tblName.Substring(0, tblName.Length - 7).TrimEnd();
            }

            this.KeySpaceName = ksName;
            this.TableName = tblName;
        }

        public CKeySpaceTableNames(DataRow dataRow)
        {
            this.KeySpaceName = dataRow["Keyspace Name"] as string;
            this.TableName = dataRow["Name"] as string;

            if (this.TableName != null && this.TableName.EndsWith("(index)"))
            {
                this.TableName = this.TableName.Substring(0, this.TableName.Length - 7).TrimEnd();
            }
        }

        public string KeySpaceName;
        public string TableName;

        public string NormalizedName { get { return this.KeySpaceName + "." + this.TableName; } }
        public string LogName { get { return this.KeySpaceName + "-" + this.TableName + "-"; } }
        public string ConcatName { get { return this.KeySpaceName + this.TableName; } }

    }

}
