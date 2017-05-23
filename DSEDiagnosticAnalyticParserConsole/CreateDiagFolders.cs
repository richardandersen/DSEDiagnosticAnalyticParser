using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using Common;
using Common.Directory;

namespace DSEDiagnosticAnalyticParserConsole
{
    public static class CreateDiagFolders
    {
        public static void CreateFolders(string[] nodes, IDirectoryPath diagPath)
        {
            var diagNodePath = diagPath.MakeChild(ParserSettings.DiagNodeDir) as Common.IDirectoryPath;
            IPath nodePath; 

            foreach (var node in nodes)
            {
                nodePath = diagNodePath.MakeChild(node);

                if (nodePath.Create())
                {
                    ConsoleDisplay.Console.WriteLine("Creating Node Directory {0}", nodePath.PathResolved);

                    nodePath.MakeChild(ParserSettings.NodetoolDir).Create();
                    nodePath.MakeChild(ParserSettings.DSEToolDir).Create();
                    nodePath.MakeChild(ParserSettings.ConfCassandraDir).Create();
                    nodePath.MakeChild(ParserSettings.ConfDSEDir).Create();
                    nodePath.MakeChild(ParserSettings.CQLDDLDirFile).Create();
                    nodePath.MakeChild(Properties.Settings.Default.LogsDir)
                            .MakeChild("cassandra").Create();


                }
                else
                {
                    ConsoleDisplay.Console.WriteLine("Creating Node Directory {0} FAILED!", nodePath.PathResolved);
                }                             
            }

        }
    }
}
