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
        public CKeySpaceTableNames(string ksName, string name, bool isIndex = false)
        {
            if (name != null && name.EndsWith("(index)"))
            {
                name = name.Substring(0, name.Length - 7).TrimEnd();
                isIndex = true;
            }

            if(isIndex)
            {
                var splitName = name.Split('.');

                if (splitName.Length == 2)
                {
                    this.TableName = splitName[0];
                    this.IndexName = splitName[1];
                }
                else
                {
                    this.IndexName = name;
                }
            }
            else
            {
                this.TableName = name;
            }

            this.KeySpaceName = ksName;
            this.Name = name;

            this.SetNames();
        }

        public CKeySpaceTableNames(DataRow dataRow)
        {
            var index = dataRow.Field<bool?>("Index");

            this.KeySpaceName = dataRow["Keyspace Name"] as string;
            this.Name = dataRow["Name"] as string;

            if (this.Name != null && this.Name.EndsWith("(index)"))
            {
                this.Name = this.Name.Substring(0, this.Name.Length - 7).TrimEnd();
                index = true;
            }

            if (index.HasValue && index.Value)
            {
                var splitName = this.Name.Split('.');

                if (splitName.Length == 2)
                {
                    this.TableName = splitName[0];
                    this.IndexName = splitName[1];
                }
                else
                {
                    this.IndexName = this.Name;
                }
            }
            else
            {
                this.TableName = this.Name;
            }

            this.SetNames();
        }

        public readonly string KeySpaceName;
        public readonly string Name;
        public readonly string TableName;
        public readonly string IndexName;

        public string NormalizedName { get; private set; }
        public string LogName { get; private set; }
        public string ConcatName { get; private set; }

        public string SolrIndexName { get; private set; }

        public string DisplayName { get; private set; }

        private void SetNames()
        {
            this.NormalizedName = this.KeySpaceName + '.' + this.Name;
            this.LogName = this.KeySpaceName + '-' + this.Name + '-';
            this.ConcatName = this.KeySpaceName + this.Name;

            if (string.IsNullOrEmpty(this.IndexName))
            {
                this.SolrIndexName = this.NormalizedName;
            }
            else if (this.IndexName.StartsWith(this.KeySpaceName + '_')
                        && this.IndexName.Contains('_' + this.TableName + '_'))
            {
                this.SolrIndexName = this.IndexName;
            }
            else
            {
                this.SolrIndexName = this.KeySpaceName + '_' + this.TableName + '_' + this.IndexName;
            }

            this.DisplayName = this.IndexName == null ? this.NormalizedName : this.NormalizedName + " (index)";
        }

        public override string ToString()
        {
            return this.DisplayName;
        }
    }

}
