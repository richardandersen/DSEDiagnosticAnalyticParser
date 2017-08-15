using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using System.Globalization;
using Common;
using System.Text.RegularExpressions;

namespace DSEDiagnosticAnalyticParserConsole
{
    static partial class ProcessFileTasks
    {
        readonly static Regex IPFileNameRegEx = new Regex(@"^(\d{1,3}[.\-_\ ]\d{1,3}[.\-_\ ]\d{1,3}[.\-_\ ]\d{1,3})|^.+[.\-_ ](\d{1,3}[.\-_\ ]\d{1,3}[.\-_\ ]\d{1,3}[.\-_\ ]\d{1,3})", RegexOptions.Compiled | RegexOptions.IgnoreCase);

        static public bool DetermineIPDCFromFileName(string pathItem, DataTable dtRingInfo, out string ipAddress, out string dcName)
        {
            var ipMatch = IPFileNameRegEx.Match(pathItem);
            ipAddress = null;

            if (ipMatch.Success && ipMatch.Groups.Count == 3)
            {
                var matchedAddr = string.IsNullOrEmpty(ipMatch.Groups[1].Value)
                                        ? ipMatch.Groups[2].Value
                                        : ipMatch.Groups[1].Value;
                ipAddress = Regex.Replace(matchedAddr, @"[\-_\ ]", @".") ?? matchedAddr;
            }
            else
            {
                var possibleAddress = Common.StringFunctions.Split(pathItem,
                                                                new char[] { ' ', '-', '_' },
                                                                Common.StringFunctions.IgnoreWithinDelimiterFlag.Text,
                                                                Common.StringFunctions.SplitBehaviorOptions.Default | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);
                
                if (possibleAddress.Count() == 1)
                {
                    if (!IPAddressStr(possibleAddress[0], out ipAddress))
                    {
                        dcName = null;
                        return false;
                    }
                }
                else
                {
                    string fileNamePart;

                    for (int nIndex = 0; nIndex < possibleAddress.Count; ++nIndex)
                    {
                        fileNamePart = possibleAddress[nIndex];

                        var parts = Common.StringFunctions.CountOccurrences(fileNamePart, '.');

                        if (parts == 0)
                        {
                            if (fileNamePart.Length <= 3)
                            {
                                if (Common.StringFunctions.IsValidNumeric(fileNamePart, true))
                                {
                                    if (possibleAddress.Count > nIndex + 3
                                            && possibleAddress.GetRange(nIndex, 4).TrueForAll(item => item.Length <= 3 && Common.StringFunctions.IsValidNumeric(item, true))
                                            && IPAddressStr(fileNamePart + "." + possibleAddress[nIndex + 1] + "." + possibleAddress[nIndex + 2] + "." + possibleAddress[nIndex + 3], out ipAddress))
                                    {
                                        break;
                                    }

                                    var ipView = new DataView(dtRingInfo,
                                                                string.Format("[Node IPAddress] like '%.{0}'", fileNamePart),
                                                                null,
                                                                DataViewRowState.CurrentRows);

                                    if (ipView.Count == 1)
                                    {
                                        ipAddress = (string)ipView[0]["Node IPAddress"];
                                        dcName = ipView[0]["Data Center"] as string;
                                        return true;
                                    }
                                }
                            }

                            continue;
                        }
                        else if (parts > 3)
                        {
                            var extPos = fileNamePart.LastIndexOf('.');
                            fileNamePart = fileNamePart.Substring(0, extPos);
                        }

                        if (IPAddressStr(fileNamePart, out ipAddress))
                        {
                            break;
                        }

                        ipAddress = null;
                    }

                    if (string.IsNullOrEmpty(ipAddress))
                    {
                        dcName = null;
                        return false;
                    }
                }
            }

            var dcRow = dtRingInfo.Rows.Count == 0 ? null : dtRingInfo.Rows.Find(ipAddress);

            if (dcRow == null)
            {
                dcName = null;
            }
            else
            {
                dcName = dcRow["Data Center"] as string;
            }

            return true;
        }

        static public bool DetermineDataCenterFromIPAddress(string ipAddress, out string dcName, DataTable dtRingInfo = null)
        {
            if(dtRingInfo == null)
            {
                dtRingInfo = Program.DTRingInfo;
            }

            if(dtRingInfo == null || string.IsNullOrEmpty(ipAddress))
            {
                dcName = null;
                return false;
            }
            
            var dcRow = dtRingInfo.Rows.Count == 0 ? null : dtRingInfo.Rows.Find(ipAddress);

            if (dcRow == null)
            {
                dcName = null;
            }
            else
            {
                dcName = dcRow["Data Center"] as string;
            }

            return true;
        }

        static bool LookForIPAddress(string value, string ignoreIPAddress, out string ipAddress)
        {

            if (string.IsNullOrEmpty(value) || value.Length < 7)
            {
                ipAddress = null;
                return false;
            }

            if (value[0] == '/')
            {
                string strIP;
                int nPortPoa = value.IndexOfAny(new char[] { ':', '\'' });

                if (nPortPoa > 7)
                {
                    value = value.Substring(0, nPortPoa);
                }

                if (IPAddressStr(value.Substring(1), out strIP))
                {
                    if (strIP != ignoreIPAddress)
                    {
                        ipAddress = strIP;
                        return true;
                    }
                }
            }
            else if (Char.IsDigit(value[0]))
            {
                string strIP;
                int nPortPoa = value.IndexOfAny(new char[] { ':', '\'' });

                if (nPortPoa > 6)
                {
                    value = value.Substring(0, nPortPoa);
                }

                if (IPAddressStr(value, out strIP))
                {
                    if (strIP != ignoreIPAddress)
                    {
                        ipAddress = strIP;
                        return true;
                    }
                }
            }
            else if (value[0] == '[')
            {
                var newValue = value.Substring(1);

                if (newValue[newValue.Length - 1] == ']')
                {
                    newValue = newValue.Substring(0, newValue.Length - 1);
                }

                var items = newValue.Split(new char[] { ' ', ',', '>' });

                foreach (var element in items)
                {
                    if (LookForIPAddress(element, ignoreIPAddress, out ipAddress))
                    {
                        return true;
                    }
                }
            }
            else if (value[0] == '(')
            {
                var newValue = value.Substring(1);

                if (newValue[newValue.Length - 1] == ')')
                {
                    newValue = newValue.Substring(0, newValue.Length - 1);
                }

                var items = newValue.Split(new char[] { ' ', ',', '>' });

                foreach (var element in items)
                {
                    if (LookForIPAddress(element, ignoreIPAddress, out ipAddress))
                    {
                        return true;
                    }
                }
            }

            ipAddress = null;
            return false;
        }

        static bool IsIPv4(string value)
        {
            var quads = value.Split('.');

            // if we do not have 4 quads, return false
            if (!(quads.Length == 4)) return false;

            var portPos = quads[3].IndexOf(':');

            if(portPos > 0)
            {
                quads[3] = quads[3].Substring(0, portPos);
            }
            // for each quad
            foreach (var quad in quads)
            {
                int q;
                // if parse fails
                // or length of parsed int != length of quad string (i.e.; '1' vs '001')
                // or parsed int < 0
                // or parsed int > 255
                // return false
                if (!Int32.TryParse(quad, out q)
                    || !q.ToString().Length.Equals(quad.Length)
                    || q < 0
                    || q > 255)
                { return false; }

            }

            return true;
        }

        static bool IPAddressStr(string ipAddress, out string formattedAddress)
        {
			if (!string.IsNullOrEmpty(ipAddress))
			{
				if (ipAddress[0] == '/')
				{
					ipAddress = ipAddress.Substring(1);
				}

				if (IsIPv4(ipAddress))
				{
					var portPos = ipAddress.IndexOf(':');
					string port = null;
					System.Net.IPAddress objIP;

					if (portPos > 0)
					{
						port = ipAddress.Substring(portPos);
						ipAddress = ipAddress.Substring(0, portPos);
					}

					if (System.Net.IPAddress.TryParse(ipAddress, out objIP))
					{
						if (port == null)
						{
							formattedAddress = objIP.ToString();
						}
						else
						{
							formattedAddress = objIP.ToString() + port;
						}
						return true;
					}
				}
			}

			formattedAddress = ipAddress;
            return false;
        }

        static public string RemoveQuotes(string item, bool checkbrackets = true)
        {
            RemoveQuotes(item, out item, checkbrackets);
            return item;
        }

        static public bool RemoveQuotes(string item, out string newItem, bool checkbrackets = true)
        {
            if (item.Length > 2
                    && ((item[0] == '\'' && item[item.Length - 1] == '\'')
                            || (item[0] == '"' && item[item.Length - 1] == '"')
                            || (checkbrackets && item[0] == '[' && item[item.Length - 1] == ']')))
            {
                newItem = item.Substring(1, item.Length - 2);
                return true;
            }

            newItem = item;
            return false;
        }

        static Tuple<string, string> SplitTableName(string cqlTableName, string defaultKeySpaceName)
        {
            var nameparts = Common.StringFunctions.Split(cqlTableName,
                                                            new char[] {'.', '/'},
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default
                                                                | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

            if (nameparts.Count == 1)
            {
                return new Tuple<string, string>(defaultKeySpaceName, RemoveQuotes(nameparts[0]));
            }

            return new Tuple<string, string>(RemoveQuotes(nameparts[0]), RemoveQuotes(nameparts[1]));
        }

		static Tuple<string, string> SplitTableName(string cqlTableName)
		{
			if (cqlTableName[0] == '[' || cqlTableName[0] == '(')
			{
				cqlTableName = cqlTableName.Substring(1, cqlTableName.Length - 2);
			}

			return SplitTableName(cqlTableName, null);
		}

		static Tuple<string, string> ParseKeyValuePair(string pairKeyValue)
        {
            var valueList = pairKeyValue.Split('=');

            if (valueList.Length == 1)
            {
                return new Tuple<string, string>(valueList[0].Trim(), null);
            }

            return new Tuple<string, string>(valueList[0].Trim(), valueList[1].Trim());
        }

        const decimal BytesToMB = 1048576m;
        static decimal ConvertInToMB(string strSize, string type)
        {
            switch (type.ToLower())
            {
                case "bytes":
                case "byte":
                case "byt":
                    return decimal.Parse(strSize, NumberStyles.AllowThousands) / BytesToMB;
                case "kb":
				case "kib":
                    return decimal.Parse(strSize, NumberStyles.Number) / 1024m;
                case "mb":
				case "mib":
                    return decimal.Parse(strSize, NumberStyles.Number);
                case "gb":
				case "gib":
                    return decimal.Parse(strSize, NumberStyles.Number) * 1024m;
                case "tb":
				case "tib":
                    return decimal.Parse(strSize, NumberStyles.Number) * 1048576m;
            }

            return -1;
        }

        static decimal ConvertInToMB(string strSizeAndType)
        {
            var spacePos = strSizeAndType.IndexOf(' ');

            if (spacePos <= 0)
            {
                return -1;
            }

            return ConvertInToMB(strSizeAndType.Substring(0, spacePos), strSizeAndType.Substring(spacePos + 1));
        }

        static DateTime FromUnixTime(long unixTime)
        {
            var epoch = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
            return epoch.AddMilliseconds(unixTime);
        }

        static DateTime FromUnixTime(string unixTime)
        {
            return FromUnixTime(long.Parse(unixTime));
        }

        static string RemoveNamespace(string className)
        {
            className = RemoveQuotes(className);

            if (!className.Contains('/'))
            {
                var lastPeriod = className.LastIndexOf('.');

                if (lastPeriod >= 0)
                {
                    return className.Substring(lastPeriod + 1);
                }
            }

            return className;
        }

        static string DetermineProperFormat(string strValue, bool ignoreBraces = false, bool removeNamespace = true)
        {
            var result = DetermineProperObjectFormat(strValue, ignoreBraces, removeNamespace);

            return result == null ? null : (result is string ? (string)result : result.ToString());
        }

        static object DetermineProperObjectFormat(string strValue, bool ignoreBraces = false, bool removeNamespace = true)
        {
            string strValueA;
            object item;

            if (string.IsNullOrEmpty(strValue))
            {
                return strValue;
            }

            strValue = strValue.Trim();

            if (strValue == string.Empty)
            {
                return strValue;
            }

            if (strValue == "null")
            {
                return null;
            }

            if (!ignoreBraces)
            {
                if (strValue[0] == '{')
                {
                    strValue = strValue.Substring(1);
                }
                if (strValue[strValue.Length - 1] == '}')
                {
                    strValue = strValue.Substring(0, strValue.Length - 1);
                }


                if (strValue[0] == '['
                    && (strValue[strValue.Length - 1] == ']'))
                {
                    strValue = strValue.Substring(1, strValue.Length - 2);

                    var splitItems = strValue.Split(',');

                    if (splitItems.Length > 1)
                    {
                        var fmtItems = splitItems.Select(i => DetermineProperFormat(i, ignoreBraces, removeNamespace)).Sort();
                        return "[" + string.Join(", ", fmtItems) + "]";
                    }
                }
            }

            if (RemoveQuotes(strValue, out strValue, false))
            {
                var splitItems = strValue.Split(',');

                if (splitItems.Length > 1)
                {
                    var fmtItems = splitItems.Select(i => DetermineProperFormat(i, ignoreBraces, removeNamespace)).Sort();
                    return string.Join(", ", fmtItems);
                }
            }

            if (IPAddressStr(strValue, out strValueA))
            {
                return strValueA;
            }

            if(strValue.IndexOfAny(new char[] { '/', '\\', ':' }) >= 0)
            {
                return strValue;
            }

            if (StringFunctions.ParseIntoNumeric(strValue, out item))
            {
                return item;
            }

            return removeNamespace ? RemoveNamespace(strValue) : strValue;
        }

        static string RemoveCommentInLine(string line, char commentChar = '#')
        {
            var commentPos = line.IndexOf(commentChar);

            if (commentPos >= 0)
            {
                return line.Substring(0, commentPos).TrimEnd();
            }

            return line;
        }

        static string RemoveCommentInLine(string line, string strCommentStart, string strCommentEnd)
        {
            var commentStartPos = line.IndexOf(strCommentStart);

            if (commentStartPos >= 0)
            {
                var commentEndPos = line.LastIndexOf(strCommentStart);

                return (line.Substring(0, commentStartPos)
                            + (commentStartPos >= 0
                                    ? line.Substring(commentEndPos + strCommentEnd.Length)
                                    : string.Empty)).TrimEnd();
            }

            return line;
        }

        static object DetermineTime(string strTime)
        {
            var timeAbbrPos = strTime.IndexOfAny(new char[] { 'm', 's', 'h' });
            object numTime;

            if (timeAbbrPos > 0)
            {
                strTime = strTime.Substring(0, timeAbbrPos);
            }

            if (StringFunctions.ParseIntoNumeric(strTime, out numTime))
            {
                return numTime;
            }

            return strTime;
        }

        static decimal ConvertToTimeMS(string strTime, string unitOfTime)
        {
            if(string.IsNullOrEmpty(strTime))
            {
                return 0;
            }

            if(unitOfTime.Length < 3)
            {
                unitOfTime = unitOfTime + "   ";
            }

            switch (unitOfTime.Substring(0,3).ToLower())
            {
                case "mic":
                case "us ":
                    return decimal.Parse(strTime) / 1000m;
                case "ms ":
                case "mil":
                case "f  ":
                    return decimal.Parse(strTime);
                case "sec":
                case "s  ":
                    return decimal.Parse(strTime) * 1000m;
                case "min":
                case "m  ":
                    return decimal.Parse(strTime) * 60000m;
                case "hou":
                case "hr ":
                case "hrs":
                case "h  ":
                    return decimal.Parse(strTime) * 360000m;
                default:
                    break;
            }

            return -1;
        }

        static Dictionary<string, object> ParseJson(string strJson)
        {
            strJson = strJson.Trim();

            if (strJson[0] == '{')
            {
                strJson = strJson.Substring(1, strJson.Length - 2);
            }

            var keyValuePair = StringFunctions.Split(strJson,
                                                        new char[] { ':', ',' },
                                                        StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace | Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket,
                                                        Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);

            var jsonDict = new Dictionary<string, object>();
            var pairLength = keyValuePair.Count == 1 ? (keyValuePair[0] == string.Empty ? 0 : 1) : keyValuePair.Count;

            for (int nIndex = 0; nIndex < pairLength; ++nIndex)
            {
                jsonDict.Add(RemoveQuotes(keyValuePair[nIndex].Trim()).Trim(),
                                ++nIndex < pairLength ? ParseJsonValue(keyValuePair[nIndex]) : null);
            }

            return jsonDict;
        }

        static object ParseJsonValue(string jsonValue)
        {
            if (string.IsNullOrEmpty(jsonValue))
            {
                return jsonValue;
            }

            jsonValue = RemoveQuotes(jsonValue.Trim(), false);

            if (jsonValue == string.Empty)
            {
                return jsonValue;
            }

            if (jsonValue.Length > 1)
            {
                var endPos = jsonValue.Length - 1;

                if (jsonValue[0] == '[')
                {
                    if (jsonValue[endPos] == ']')
                    {
                        var arrayValues = StringFunctions.Split(jsonValue.Substring(1, endPos - 1),
                                                                    ',',
                                                                    StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace | Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket,
                                                                    Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);

                        var arrayLength = arrayValues.Count == 1 ? (arrayValues[0] == string.Empty ? 0 : 1) : arrayValues.Count;
                        var array = new object[arrayLength];

                        for (int nIndex = 0; nIndex < arrayLength; ++nIndex)
                        {
                            array[nIndex] = ParseJsonValue(arrayValues[nIndex]);
                        }
                        return array;
                    }
                }
                else if (jsonValue[0] == '{')
                {
                    if (jsonValue[endPos] == '}')
                    {
                        return ParseJson(jsonValue);
                    }
                }
            }

            return DetermineProperObjectFormat(jsonValue, true, false);
        }

        static object NonNullValue(object o1, object o2)
        {
            return o1 == null ? o2 : o1;
        }


        public static V TryGetValue<K,V>(this Dictionary<K,V> collection, K key)
            where V : class
        {
            V getValue;

            if(collection != null && collection.TryGetValue(key, out getValue))
            {
                return getValue;
            }

            return default(V);
        }

        public static V TryGetValue<K, V, T>(this Dictionary<K, V> collection, K key, T defaultValue)
            where V : class
        {
            V getValue;

            if (collection != null && collection.TryGetValue(key, out getValue))
            {
                return getValue;
            }

            return defaultValue as V;
        }

		public static bool ExtractFileToFolder(IFilePath filePath, out IDirectoryPath extractedFolder, bool forceExtraction = false)
		{
			extractedFolder = filePath.ParentDirectoryPath;

			if(filePath.Exist()
					&& (forceExtraction || ParserSettings.ExtractFilesWithExtensions.Contains(filePath.FileExtension)))
			{
				var newExtractedFolder = filePath.ParentDirectoryPath.MakeChild(filePath.FileNameWithoutExtension);

				if(!newExtractedFolder.Exist())
				{
					System.IO.Compression.ZipFile.ExtractToDirectory(filePath.PathResolved, newExtractedFolder.PathResolved);
				}

				extractedFolder = (IDirectoryPath) newExtractedFolder;

				return true;
			}

			return false;
		}

        
    }
}
