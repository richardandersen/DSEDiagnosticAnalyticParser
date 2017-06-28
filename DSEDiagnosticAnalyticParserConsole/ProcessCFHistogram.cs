using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Data;
using Common;
using System.Text.RegularExpressions;

namespace DSEDiagnosticAnalyticParserConsole
{

    static public partial class ProcessFileTasks
    {
        //10.23.8.21	hpnplgmddb01
        //keyspace/table histograms
        //Percentile SSTables     Write Latency      Read Latency    Partition Size        Cell Count
        //                             (micros)          (micros)           (bytes)                  
        //50%             0.00            149.00              0.00               NaN NaN
        //75%             0.00            179.00              0.00               NaN NaN
        //95%             0.00            179.00              0.00               NaN NaN
        //98%             0.00            179.00              0.00               NaN NaN
        //99%             0.00            179.00              0.00               NaN NaN
        //Min             0.00              3.00              0.00               NaN NaN
        //Max             0.00            179.00              0.00               NaN NaN
        //keyspace/table histograms
        //Percentile SSTables     Write Latency      Read Latency    Partition Size        Cell Count
        //                             (micros)          (micros)           (bytes)                  
        //50%             1.00           1109.00            642.00            379022              4768
        //75%             1.00           1597.00            924.00           1955666             24601
        //95%            20.00           3311.00         315852.00          12108970            152321
        //98%            24.00           4768.00         315852.00          17436917            219342
        //99%            35.00           5722.00         379022.00          25109160            263210
        //Min             0.00             73.00            104.00              3974                51
        //Max            42.00        7007506.00         654949.00          52066354            545791

        static Regex RegExCFHistAddr = new Regex(@"\s*(\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})\s+([^ ]+)?",
                                               RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCFHistKSTbl = new Regex(@"\s*(\w+)(?:/|\.)(\w+)\s+histograms\s*",
                                               RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCFHistHdr = new Regex(@"\s*Percentile\s+SSTables\s+Write\s+Latency\s+Read\s+Latency\s+Partition\s+Size\s+Cell\s+Count\s*",
                                                    RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCFHistHdr2 = new Regex(@"\s*(\(\w+\))\s+(\(\w+\))\s+(\(\w+\))\s*",
                                                   RegexOptions.IgnoreCase | RegexOptions.Compiled);
        static Regex RegExCFHistLine = new Regex(@"\s*((?:max|min|50%|75%|95%|98%|99%))\s*((?:\d+\.\d\d|nan))\s*((?:\d+\.\d\d|nan))\s*((?:\d+\.\d\d|nan))\s*((?:\d+|nan))\s*((?:\d+|nan))",
                                                   RegexOptions.IgnoreCase | RegexOptions.Compiled);

        static public void initializeCFHistogramDataTable(DataTable dtCFHistogram)
        {
            if (dtCFHistogram.Columns.Count == 0)
            {
                dtCFHistogram.Columns.Add("Source", typeof(string));
                dtCFHistogram.Columns.Add("Data Center", typeof(string)).AllowDBNull = true;
                dtCFHistogram.Columns.Add("Node IPAddress", typeof(string));
                dtCFHistogram.Columns.Add("KeySpace", typeof(string));
                dtCFHistogram.Columns.Add("Table", typeof(string));
                dtCFHistogram.Columns.Add("Attribute", typeof(string));

                dtCFHistogram.Columns.Add("50%", typeof(object)).AllowDBNull = true;
                dtCFHistogram.Columns.Add("75%", typeof(object)).AllowDBNull = true;
                dtCFHistogram.Columns.Add("95%", typeof(object)).AllowDBNull = true;
                dtCFHistogram.Columns.Add("98%", typeof(object)).AllowDBNull = true;
                dtCFHistogram.Columns.Add("99%", typeof(object)).AllowDBNull = true;
                dtCFHistogram.Columns.Add("Min", typeof(object)).AllowDBNull = true;
                dtCFHistogram.Columns.Add("Max", typeof(object)).AllowDBNull = true;

                //dtCFHistogram.PrimaryKey = new System.Data.DataColumn[] { dtCFHistogram.Columns["Source"],
                //                                                            dtCFHistogram.Columns["Node IPAddress"],
                //                                                            dtCFHistogram.Columns["KeySpace"],
                //                                                            dtCFHistogram.Columns["Table"],
                //                                                            dtCFHistogram.Columns["Attribute"]};
            }
        }

        static public void ReadCFHistogramFileParseIntoDataTable(IFilePath cfhistogramFilePath,
                                                                    string ipAddress,
                                                                    string dcName,
                                                                    DataTable dtRingInfo,
                                                                    DataTable dtCFHistogram)
        {

            initializeCFHistogramDataTable(dtCFHistogram);

            var fileLines = cfhistogramFilePath.ReadAllLines();
            var attributes = new string[] { "SSTables", "Write Latency", "Read Latency", "Partition Size", "Cell Count" };
            var checkIPAddress = string.IsNullOrEmpty(ipAddress);

            for (int nIndex = 0; nIndex < fileLines.Length; ++nIndex)
            {
                if(string.IsNullOrEmpty(fileLines[nIndex].Trim()))
                {
                    continue;
                }

                if(checkIPAddress)
                {
                    var ipAddrSplit = RegExCFHistAddr.Split(fileLines[nIndex]);

                    if(ipAddrSplit.Length >= 3)
                    {
                        if (IPAddressStr(ipAddrSplit[1], out ipAddress))
                        {
                            var dcRow = dtRingInfo == null || dtRingInfo.Rows.Count == 0
                                            ? null
                                            : dtRingInfo.Rows.Find(ipAddress);

                            if (dcRow == null)
                            {
                                dcName = null;

                                cfhistogramFilePath.Path.Dump(Logger.DumpType.Warning, "DataCenter Name was not found in the CFHistogram file.");
                                Program.ConsoleWarnings.Increment("DataCenter Name Not Found");
                            }
                            else
                            {
                                dcName = dcRow["Data Center"] as string;
                            }
                            continue;
                        }
                        else
                        {
                            ipAddress = null;
                        }
                    }

                    if (string.IsNullOrEmpty(ipAddress))
                    {
                        if (ParserSettings.ParsingExcelOptions.ParseRingInfoFiles.IsEnabled())
                        {
                            cfhistogramFilePath.Path.Dump(Logger.DumpType.Warning, "IPAdress was not found in the CFHistogram file. File Ignored.");
                            Program.ConsoleWarnings.Increment("IPAdress Not Found");
                        }
                        break;
                    }
                }

                var kstblSplit = RegExCFHistKSTbl.Split(fileLines[nIndex]);

                if (kstblSplit.Length != 4)
                {
                    kstblSplit.Dump(Logger.DumpType.Warning,
                                        "Processing CFHistogram file \"{0}\" on line \"{1}\" expected a Keyspace/Table line header. Skipping to net valid section.",
                                         cfhistogramFilePath.PathResolved,
                                         fileLines[nIndex]);
                    Program.ConsoleWarnings.Increment("CFHistogram Invalid Keyspace/Table Line...");
                    continue;
                }

                var hdrMatch = RegExCFHistHdr.Match(fileLines[++nIndex]);

                if(!hdrMatch.Success)
                {
                    fileLines[++nIndex].Dump(Logger.DumpType.Warning,
                                                "Processing CFHistogram file \"{0}\" header line. Skipping to net valid section.",
                                                 cfhistogramFilePath.PathResolved);
                    Program.ConsoleWarnings.Increment("CFHistogram Invalid Header Line...");
                    continue;
                }

                var hdr1Split = RegExCFHistHdr2.Split(fileLines[++nIndex]);
                var unitsOfMessure = hdr1Split.Select(item => string.IsNullOrEmpty(item)
                                                                    ? null : (item[0] == '(' && item[item.Length - 1] == ')'
                                                                                    ? item.Substring(1, item.Length - 2)
                                                                                    : item)).ToArray();
                var dataRows = attributes.Select(attr =>
                                                    {
                                                        var dataRow = dtCFHistogram.NewRow();
                                                        dataRow["Source"] = "cfhistograms";
                                                        dataRow["Data Center"] = dcName;
                                                        dataRow["Node IPAddress"] = ipAddress;
                                                        dataRow["KeySpace"] = RemoveQuotes(kstblSplit[1]);
                                                        dataRow["Table"] = RemoveQuotes(kstblSplit[2]);
                                                        dataRow["Attribute"] = attr;
                                                        return dataRow;
                                                    }).ToArray();

                for (++nIndex; nIndex < fileLines.Length; ++nIndex)
                {
                    if (string.IsNullOrEmpty(fileLines[nIndex].Trim()))
                    {
                        continue;
                    }

                    if (checkIPAddress && RegExCFHistAddr.Match(fileLines[nIndex]).Success)
                    {
                        --nIndex;
                        break;
                    }
                    else if (RegExCFHistKSTbl.Match(fileLines[nIndex]).Success)
                    {
                        --nIndex;
                        break;
                    }

                    var lineSplit = RegExCFHistLine.Split(fileLines[nIndex]);

                    if (lineSplit.Length == 8)
                    {
                        var strPercentile = lineSplit[1];
                        object numValue;

                        for (int nVIdx = 0; nVIdx < attributes.Length; ++nVIdx)
                        {
                            var uom = unitsOfMessure[nVIdx];
                            var lineItem = lineSplit[nVIdx + 2];

                            if (string.IsNullOrEmpty(uom))
                            {
                                if (StringFunctions.ParseIntoNumeric(lineItem, out numValue))
                                {
                                    if ((((dynamic)numValue) % 1) == 0)
                                    {
                                        dataRows[nVIdx][strPercentile] = (long)((dynamic)numValue);
                                    }
                                    else
                                    {
                                        dataRows[nVIdx][strPercentile] = numValue;
                                    }
                                }
                                else
                                {
                                    if (lineItem.ToLower() != "nan")
                                    {
                                        dataRows[nVIdx][strPercentile] = lineItem;
                                    }
                                }
                            }
                            else if(lineItem.ToLower() == "nan")
                            { }
                            else
                            {
                                var tryTime = ConvertToTimeMS(lineItem, uom);

                                if(tryTime >= 0)
                                {
                                    dataRows[nVIdx][strPercentile] = tryTime;
                                }
                                else
                                {
                                    var trySpace = ConvertInToMB(lineItem, uom);

                                    if (trySpace >= 0)
                                    {
                                        dataRows[nVIdx][strPercentile] = trySpace;
                                    }
                                }
                            }
                        }
                    }
                    else
                    {
                        fileLines[nIndex].Dump(Logger.DumpType.Warning,
                                                "Processing CFHistogram file \"{0}\" has an invalid line. Line Skipped.",
                                                 cfhistogramFilePath.PathResolved);
                        Program.ConsoleWarnings.Increment("CFHistogram Invalid Line...");
                    }
                }

                dataRows.ForEach(dataRow => dtCFHistogram.Rows.Add(dataRow));
            }
        }


    }
}
