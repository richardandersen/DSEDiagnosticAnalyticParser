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
                                                    List<YamlInfo> yamlList,
                                                    bool parsingPropertiesFile = false)
        {
            var fileLines = yamlFilePath.ReadAllLines();
            string line;
            int posCmdDel;
            string strCmd;
            string parsedValue;
            bool optionsCmdParamsFnd = false;
            bool optionsBrace = false;
            YamlInfo lastYaml = null;

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
                line = RemoveCommentInLine(fileLines[nIndex]).RemoveConsecutiveChar().Trim();

                if (string.IsNullOrEmpty(line)
                    || line[0] == '#'
                    || line.StartsWith("if ")
                    || line == "fi")
                {
                    continue;
                }

                if (line[0] == '-')
                {
                    if (lastYaml != null)
                    {
                        parsedValue = line.Substring(1).TrimStart();
                        if (parsedValue != string.Empty && parsedValue[0] != '-')
                        {
                            lastYaml.CmdParams += ' ' + parsedValue;
                        }
                    }
                    continue;
                }
                else if (optionsBrace)
                {
                    lastYaml.CmdParams += ' ' + line;
                    optionsBrace = !(line.Length > 0 && line[line.Length - 1] == '}');
                    continue;
                }
                else if (line.StartsWith("parameters:")
                            || (optionsCmdParamsFnd && fileLines[nIndex][0] == ' '))
                {
                    lastYaml.CmdParams += ' ' + line;
                    continue;
                }
                else if (lastYaml != null
                            && string.IsNullOrEmpty(lastYaml.CmdParams)
                            && fileLines[nIndex][0] == ' ')
                {
                    lastYaml.CmdParams = line;
                    lastYaml.OptionsCmd = true;
                    optionsCmdParamsFnd = true;
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
                        lastYaml.CmdParams += ' ' + line;
                        continue;
                    }
                }

                strCmd = line.Substring(0, posCmdDel);

                if (strCmd.EndsWith("_options"))
                {
                    optionsCmdParamsFnd = true;
                }

                parsedValue = line.Substring(posCmdDel + 1).Trim();

                if (parsedValue.Length > 2 && parsedValue[0] == '{')
                {
                    if (parsedValue[parsedValue.Length - 1] != '}')
                    {
                        optionsBrace = true;
                    }
                }

                yamlList.Add(lastYaml = new YamlInfo()
                {
                    YamlType = yamlType,
                    Cmd = strCmd,
                    DCName = dcName,
                    IPAddress = ipAddress,
                    CmdParams = parsedValue,
                    OptionsCmd = optionsCmdParamsFnd
                });
            }

            if (!parsingPropertiesFile)
            {
                var propList = new List<YamlInfo>();

                foreach (var element in yamlList)
                {
                    if (element.Cmd == "endpoint_snitch")
                    {
                        if (ParserSettings.SnitchFiles.ContainsKey(RemoveNamespace(element.CmdParams).ToLower()))
                        {
                            var propFilePath = Common.Path.PathUtils.BuildPath(ParserSettings.SnitchFiles[RemoveNamespace(element.CmdParams).ToLower()],
                                                                                yamlFilePath.ParentDirectoryPath.Path,
                                                                                null,
                                                                                true,
                                                                                true,
                                                                                true) as IFilePath;
                            if (propFilePath != null
                                    && propFilePath.Exist())
                            {
                                ReadYamlFileParseIntoList(propFilePath, ipAddress, dcName, propFilePath.FileName, propList, true);
                            }
                        }
                    }
                    else if (element.Cmd.EndsWith("_directories"))
                    {
                        element.CmdParams = element.CmdParams.Trim().Split(' ').OrderBy(i => i).Join(", ", i => i);
                    }
                    else
                    {
                        var parsedValues = ParseCommandParams(element.CmdParams, string.Empty);

                        element.CmdParams = parsedValues.Item1;
                        element.KeyValueParams = parsedValues.Item2;
                    }
                }

                yamlList.AddRange(propList);
            }
        }

        static Tuple<string, IEnumerable<Tuple<string, string>>> ParseCommandParams(string cmdParams, string orgSubCmd, string topSubCmd = null)
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
                    return ParseCommandParams(paramValue.Substring(1, paramValue.Length - 1), orgSubCmd, topSubCmd);
                }

                return new Tuple<string, IEnumerable<Tuple<string, string>>>(orgSubCmd + DetermineProperFormat(separateParams.FirstOrDefault()), null);
            }
            else
            {
                var keyValues = new List<Tuple<string, string>>();
                string subCmd = orgSubCmd;
                bool keyWord = false;

                for (int nIndex = 0; nIndex < separateParams.Count; ++nIndex)
                {
                    if (!string.IsNullOrEmpty(separateParams[nIndex])
                            && separateParams[nIndex].Length > 1
                            && separateParams[nIndex][0] == '{'
                            && separateParams[nIndex][separateParams[nIndex].Length - 1] == '}')
                    {
                        var paramItems = ParseCommandParams(separateParams[nIndex].Substring(1, separateParams[nIndex].Length - 2), subCmd, topSubCmd);

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

						if(nIndex + 1 == separateParams.Count)
						{
							keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false),
																		string.Empty));
							continue;
						}

                        if (separateParams[nIndex + 1][separateParams[nIndex + 1].Length - 1] == ':')
                        {
							if(separateParams[nIndex].EndsWith("_address")
								|| separateParams[nIndex].EndsWith("_interface")
								|| separateParams[nIndex].EndsWith("_password")
								|| separateParams[nIndex].EndsWith("_host")
								|| separateParams[nIndex].EndsWith("store")
								|| separateParams[nIndex].EndsWith("directory")
								|| separateParams[nIndex].EndsWith("_in_mb")
								|| separateParams[nIndex].EndsWith("_in_ms")
								|| separateParams[nIndex].EndsWith("_in_kb"))
							{
								keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false),
																		string.Empty));
								continue;
							}

                            if (topSubCmd == null)
                            {
                                subCmd = orgSubCmd + separateParams[nIndex] + '.';
                            }
                            else
                            {
                                subCmd = topSubCmd + separateParams[nIndex] + '.';
                            }

                            var paramItems = ParseCommandParams(string.Join(" ", separateParams.Skip(nIndex + 1)),
                                                                    subCmd,
                                                                    topSubCmd == null
                                                                        ? orgSubCmd
                                                                        : topSubCmd);

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
                        keyWord = true;
                    }

                    bool noNamespace = separateParams[nIndex] == "seeds"
                                        || separateParams[nIndex].EndsWith("_address")
                                        || separateParams[nIndex].EndsWith("_interface")
                                        || separateParams[nIndex].EndsWith("_password")
                                        || separateParams[nIndex].EndsWith("_host")
										|| separateParams[nIndex].EndsWith("store")
										|| separateParams[nIndex].EndsWith("directory");

                    if (keyWord
                            && nIndex + 3 <= separateParams.Count
                            && separateParams[nIndex + 2][separateParams[nIndex + 2].Length - 1] != ':')
                    {
                        //Determine if reminding items are values
                        var subList = separateParams.GetRange(nIndex + 1, separateParams.Count - nIndex - 1);
                        var valuesList = subList.TakeWhile(i => Common.StringFunctions.IndexOf(i, ':') < 0);

                        keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false),
                                                                string.Join(", ",
                                                                                valuesList.Select(i => DetermineProperFormat(i, false, !noNamespace))
                                                                                            .Sort())));
                        nIndex += valuesList.Count();
                        continue;
                    }

                    keyValues.Add(new Tuple<string, string>(DetermineProperFormat(subCmd + separateParams[nIndex], true, false),
                                                                DetermineProperFormat(separateParams[++nIndex], false, !noNamespace)));
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

            if (dtCYaml.Columns.Count == 0)
            {
                dtCYaml.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCYaml.Columns.Add("Node IPAddress", typeof(string));
                dtCYaml.Columns.Add("Yaml Type", typeof(string));
                dtCYaml.Columns.Add("Property", typeof(string));
                dtCYaml.Columns.Add("Value", typeof(object));
            }

            var keyvalueOccurrences = masterYamlList.GroupBy(item => item.MakeKeyValue())
                                            .Select(g => new { Key=g.First().MakeKey(), YamlItems = g, count = g.Count() });
            var masterKeys = masterYamlList.GroupBy(item => item.MakeKey())
                                                .Select(g => g.First().MakeKey());

            foreach (var key in masterKeys)
            {
                var nbrChanges = keyvalueOccurrences.Where(i => i.Key == key).OrderByDescending(i => i.count);
                int rangePos = 0;

                if (nbrChanges.Count() == 1
                        && nbrChanges.First().count > 1)
                {
                    var element = nbrChanges.First();
                    element.YamlItems.First().IPAddress = "<Common>";
                    element.YamlItems.First().AddValueToDR(dtCYaml);
                    rangePos = 1;
                }

                foreach(var element in nbrChanges.GetRange(rangePos))
                {
                    foreach (var subElement in element.YamlItems)
                    {
                        subElement.AddValueToDR(dtCYaml);
                    }
                }
            }
        }

    }
}
