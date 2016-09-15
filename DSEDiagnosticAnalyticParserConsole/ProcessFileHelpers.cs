using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;

namespace DSEDiagnosticAnalyticParserConsole
{
    static partial class ProcessFileTasks
    {
        static public bool DetermineIPDCFromFileName(string pathItem, DataTable dtRingInfo, out string ipAddress, out string dcName)
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
                var lastPartName = possibleAddress.Last();

                if (Common.StringFunctions.CountOccurrences(lastPartName, '.') > 3)
                {
                    var extPos = lastPartName.LastIndexOf('.');
                    lastPartName = lastPartName.Substring(0, extPos);
                }

                //Ip Address is either the first part of the name or the last
                if (!IPAddressStr(possibleAddress[0], out ipAddress))
                {
                    if (!IPAddressStr(lastPartName, out ipAddress))
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
                dcName = dcRow[1] as string;
            }

            return true;
        }

        static bool LookForIPAddress(string value, string ignoreIPAddress, out string ipAddress)
        {

            if (string.IsNullOrEmpty(value))
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
            if (IsIPv4(ipAddress))
            {
                System.Net.IPAddress objIP;

                if (System.Net.IPAddress.TryParse(ipAddress, out objIP))
                {
                    formattedAddress = objIP.ToString();
                    return true;
                }
            }

            formattedAddress = ipAddress;
            return false;
        }

        static public string RemoveQuotes(string item)
        {
            RemoveQuotes(item, out item);
            return item;
        }

        static public bool RemoveQuotes(string item, out string newItem)
        {
            if (item.Length > 2
                    && ((item[0] == '\'' && item[item.Length - 1] == '\'')
                            || (item[0] == '"' && item[item.Length - 1] == '"')))
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
                                                            '.',
                                                            Common.StringFunctions.IgnoreWithinDelimiterFlag.All,
                                                            Common.StringFunctions.SplitBehaviorOptions.Default
                                                                | Common.StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries);

            if (nameparts.Count == 1)
            {
                return new Tuple<string, string>(defaultKeySpaceName, RemoveQuotes(nameparts[0]));
            }

            return new Tuple<string, string>(RemoveQuotes(nameparts[0]), RemoveQuotes(nameparts[1]));
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
                    return decimal.Parse(strSize) / BytesToMB;
                case "kb":
                    return decimal.Parse(strSize) / 1024m;
                case "mb":
                    return decimal.Parse(strSize);
                case "gb":
                    return decimal.Parse(strSize) * 1024m;
                case "tb":
                    return decimal.Parse(strSize) * 1048576m;
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

            if (RemoveQuotes(strValue, out strValue))
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
                                                        StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);

            var jsonDict = new Dictionary<string, object>();

            for (int nIndex = 0; nIndex < keyValuePair.Count; ++nIndex)
            {
                jsonDict.Add(RemoveQuotes(keyValuePair[nIndex].Trim()).Trim(),
                                ParseJsonValue(keyValuePair[++nIndex]));
            }

            return jsonDict;
        }

        static object ParseJsonValue(string jsonValue)
        {
            if (string.IsNullOrEmpty(jsonValue))
            {
                return jsonValue;
            }

            jsonValue = RemoveQuotes(jsonValue.Trim());

            if (jsonValue == string.Empty)
            {
                return jsonValue;
            }

            if (jsonValue.Length > 2)
            {
                var endPos = jsonValue.Length - 1;

                if (endPos >= 2)
                {
                    if (jsonValue[0] == '[')
                    {
                        if (jsonValue[endPos] == ']')
                        {
                            var arrayValues = StringFunctions.Split(jsonValue.Substring(1, endPos - 1),
                                                                        ',',
                                                                        StringFunctions.IgnoreWithinDelimiterFlag.Text | Common.StringFunctions.IgnoreWithinDelimiterFlag.Brace | Common.StringFunctions.IgnoreWithinDelimiterFlag.Bracket,
                                                                        StringFunctions.SplitBehaviorOptions.RemoveEmptyEntries | Common.StringFunctions.SplitBehaviorOptions.StringTrimEachElement);
                            var array = new object[arrayValues.Count];

                            for (int nIndex = 0; nIndex < array.Length; ++nIndex)
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
                            return ParseJson(jsonValue.Substring(1, endPos - 1));
                        }
                    }
                }
            }

            return DetermineProperObjectFormat(jsonValue, true, false);
        }

        static object NonNullValue(object o1, object o2)
        {
            return o1 == null ? o2 : o1;
        }

    }
}
