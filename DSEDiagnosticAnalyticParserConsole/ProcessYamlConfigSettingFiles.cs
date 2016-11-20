using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    static public partial class ProcessFileTasks
    {
        static public void ReadYamlFileParseIntoList(IFilePath yamlFilePath,
                                                    string ipAddress,
                                                    string dcName,
                                                    string yamlType,
                                                    List<YamlInfo> yamlList)
        {
            var fileLines = yamlFilePath.ReadAllLines();
            string line;
            int posCmdDel;
            string strCmd;
            string parsedValue;
            bool optionsCmdParamsFnd = false;
            bool optionsBrace = false;

            //seed_provider:
            //# Addresses of hosts that are deemed contact points.
            //# Cassandra nodes use this list of hosts to find each other and learn
            //# the topology of the ring.  You must change this if you are running
            //# multiple nodes!
            //	-class_name: org.apache.cassandra.locator.SimpleSeedProvider
            //	 parameters:
            //          # seeds is actually a comma-delimited list of addresses.
            //          # Ex: "<ip1>,<ip2>,<ip3>"
            //          -seeds: "10.27.34.11,10.27.34.12"
            //
            //concurrent_reads: 32
            //
            //server_encryption_options:
            //	internode_encryption: none
            //	keystore: resources/dse/conf /.keystore
            //	keystore_password:  cassandra
            //	truststore: resources/dse/conf/.truststore
            //    truststore_password: cassandra
            //    # More advanced defaults below:
            //    # protocol: TLS
            //    # algorithm: SunX509
            //    # store_type: JKS
            //    # cipher_suites: [TLS_RSA_WITH_AES_128_CBC_SHA,TLS_RSA_WITH_AES_256_CBC_SHA,TLS_DHE_RSA_WITH_AES_128_CBC_SHA,TLS_DHE_RSA_WITH_AES_256_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA]
            //    # require_client_auth: false
            //
            // node_health_options: {enabled: false, refresh_rate_ms: 60000}
            //
            // cassandra_audit_writer_options: {mode: sync, batch_size: 50, flush_time: 500, num_writers: 10,
            //		queue_size: 10000, write_consistency: QUORUM}
            //
            // data_file_directories:
            //      - /data/1/dse/data
            //      - /data/2/dse/data
            //      - /data/3/dse/data

            for (int nIndex = 0; nIndex < fileLines.Length; ++nIndex)
            {
                line = fileLines[nIndex].Trim();

                if (string.IsNullOrEmpty(line)
                    || line[0] == '#'                    
                    || line.StartsWith("if ")
                    || line == "fi")
                {
                    continue;
                }

                if (line[0] == '-')
                {
                    if (yamlList.Count > 0)
                    {
                        parsedValue = RemoveCommentInLine(line.Substring(1).TrimStart().RemoveConsecutiveChar());
                        if (parsedValue != string.Empty && parsedValue[0] != '-')
                        {
                            yamlList.Last().CmdParams += ' ' + parsedValue;
                        }
                    }
                    continue;
                }
                else if (optionsBrace)
                {
                    parsedValue = RemoveCommentInLine(line.RemoveConsecutiveChar());
                    yamlList.Last().CmdParams += ' ' + parsedValue;
                    optionsBrace = !(parsedValue.Length > 0 && parsedValue[parsedValue.Length - 1] == '}');
                    continue;
                }
                else if (line.StartsWith("parameters:")
                            || optionsCmdParamsFnd && fileLines[nIndex][0] == ' ')
                {
                    parsedValue = RemoveCommentInLine(line.RemoveConsecutiveChar());
                    yamlList.Last().CmdParams += ' ' + parsedValue;
                    continue;
                }

                if (optionsCmdParamsFnd)
                {
                    optionsCmdParamsFnd = false;
                }

                posCmdDel = line.IndexOf(':');

                if (posCmdDel < 0)
                {
                    posCmdDel = line.IndexOf('=');

                    if (posCmdDel < 0)
                    {
                        parsedValue = RemoveCommentInLine(line.RemoveConsecutiveChar());
                        yamlList.Last().CmdParams += ' ' + parsedValue;
                        continue;
                    }
                }

                strCmd = line.Substring(0, posCmdDel);

                if (strCmd.EndsWith("_options"))
                {
                    optionsCmdParamsFnd = true;
                }

                parsedValue = RemoveCommentInLine(line.Substring(posCmdDel + 1).Trim().RemoveConsecutiveChar());

                if (parsedValue.Length > 2 && parsedValue[0] == '{')
                {
                    if (parsedValue[parsedValue.Length - 1] != '}')
                    {
                        optionsBrace = true;
                    }
                }

                yamlList.Add(new YamlInfo()
                {
                    YamlType = yamlType,
                    Cmd = strCmd,
                    DCName = dcName,
                    IPAddress = ipAddress,
                    CmdParams = parsedValue
                });
            }

            foreach (var element in yamlList)
            {
                if (!element.Cmd.EndsWith("_directories"))
                {
                    var parsedValues = ParseCommandParams(element.CmdParams, string.Empty);

                    element.CmdParams = parsedValues.Item1;
                    element.KeyValueParams = parsedValues.Item2;
                }
            }
        }

        static Tuple<string, IEnumerable<Tuple<string, string>>> ParseCommandParams(string cmdParams, string orgSubCmd)
        {
            var separateParams = Common.StringFunctions.Split(cmdParams,
                                                                new char[] { ',', ' ', '=' },
                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace,
                                                                Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

            if (separateParams.Count <= 1)
            {
                var paramValue = separateParams.FirstOrDefault();

                if (paramValue != null
                    && paramValue.Length > 1
                    && paramValue[0] == '{'
                    && paramValue[paramValue.Length - 1] == '}')
                {
                    return ParseCommandParams(paramValue.Substring(1, paramValue.Length - 1), orgSubCmd);
                }

                return new Tuple<string, IEnumerable<Tuple<string, string>>>(orgSubCmd + DetermineProperFormat(separateParams.FirstOrDefault()), null);
            }
            else
            {
                var keyValues = new List<Tuple<string, string>>();
                bool optionsFnd = false;
                bool keywordFnd = false;
                string subCmd = orgSubCmd;

                for (int nIndex = 0; nIndex < separateParams.Count; ++nIndex)
                {
                    if (!string.IsNullOrEmpty(separateParams[nIndex])
                            && separateParams[nIndex].Length > 1
                            && separateParams[nIndex][0] == '{'
                            && separateParams[nIndex][separateParams[nIndex].Length - 1] == '}')
                    {
                        var paramItems = ParseCommandParams(separateParams[nIndex].Substring(1, separateParams[nIndex].Length - 2), subCmd);

                        if (paramItems.Item1 != null)
                        {
                            throw new ArgumentException("Argument Param parsing Error. Argument: \"{0}\"", separateParams[nIndex]);
                        }
                        if (paramItems.Item2 != null)
                        {
                            keyValues.AddRange(paramItems.Item2);
                        }
                        continue;
                    }

                    if (separateParams[nIndex][separateParams[nIndex].Length - 1] == ':')
                    {
                        separateParams[nIndex] = separateParams[nIndex].Substring(0, separateParams[nIndex].Length - 1);

                        if (separateParams[nIndex + 1][separateParams[nIndex + 1].Length - 1] == ':')
                        {
                            subCmd = orgSubCmd + separateParams[nIndex] + '.';
                            var paramItems = ParseCommandParams(string.Join(" ", separateParams.Skip(nIndex + 1)), subCmd);

                            if (paramItems.Item1 != null)
                            {
                                keyValues.Add(new Tuple<string, string>(separateParams[nIndex], paramItems.Item1));
                            }
                            if (paramItems.Item2 != null)
                            {
                                keyValues.AddRange(paramItems.Item2);
                            }

                            break;
                        }
                        keywordFnd = true;
                    }
                   
                    if (separateParams[nIndex].EndsWith("_options"))
                    {
                        optionsFnd = true;
                        subCmd += separateParams[nIndex] + '.';
                    }
                    else if (optionsFnd)
                    {
                        keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false), DetermineProperFormat(separateParams[++nIndex])));
                    }
                    else if (separateParams[nIndex] != "parameters")
                    {
                        optionsFnd = false;

                        if (keywordFnd
                                && separateParams[nIndex][separateParams[nIndex].Length - 1] == 's'
                                && nIndex + 2 < separateParams.Count)
                        {
                            keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false),
                                                                        string.Join(",", separateParams.Skip(nIndex + 1))));
                            break;
                        }
                        else
                        {
                            keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false), DetermineProperFormat(separateParams[++nIndex])));
                        }
                    }

                    keywordFnd = false;
                }

                return new Tuple<string, IEnumerable<Tuple<string, string>>>(null, keyValues.OrderBy(v => v.Item1));
            }
        }

        static public void ParseYamlListIntoDataTable(Common.Patterns.Collections.LockFree.Stack<List<YamlInfo>> yamlStackList,
                                                        DataTable dtCYaml)
        {
            List<YamlInfo> yamlList;
            List<YamlInfo> masterYamlList = new List<YamlInfo>();

            while (yamlStackList.Pop(out yamlList))
            {
                masterYamlList.AddRange(yamlList);
            }

            if (masterYamlList.Count == 0)
            {
                return;
            }

            var removeDups = masterYamlList.DuplicatesRemoved(item => item.MakeKeyValue());

            if (dtCYaml.Columns.Count == 0)
            {
                dtCYaml.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCYaml.Columns.Add("Node IPAddress", typeof(string));
                dtCYaml.Columns.Add("Yaml Type", typeof(string));
                dtCYaml.Columns.Add("Property", typeof(string));
                dtCYaml.Columns.Add("Value", typeof(object));
            }

            var yamlItems = removeDups.ToArray();

            foreach (var element in yamlItems)
            {
                if (yamlItems.Count(i => i.ComparerProperyOnly(element)) < 2)
                {
                    element.IPAddress = "<Common>";
                }

                element.AddValueToDR(dtCYaml);
            }
        }

    }
}
