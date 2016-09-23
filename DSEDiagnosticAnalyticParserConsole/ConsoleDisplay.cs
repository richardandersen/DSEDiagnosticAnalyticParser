using Common;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading;

namespace DSEDiagnosticAnalyticParserConsole
{
    public class ConsoleDisplay
    {
        static ConsoleWriter consoleWriter = new ConsoleWriter();
        static Timer timer = new Timer(TimerCallback, consoleWriter, Timeout.Infinite, Timeout.Infinite);
        static List<ConsoleDisplay> ConsoleDisplays = new List<ConsoleDisplay>();

        private long _counter = 0;
        private Common.Patterns.Collections.ThreadSafe.List<string> _taskItems = new Common.Patterns.Collections.ThreadSafe.List<string>();

        public ConsoleDisplay(string displayString, int maxLines = 2, bool enableSpinner = true)
        {
            this.LineFormat = displayString;
            this.Spinner = enableSpinner;
            consoleWriter.ReserveRwWriteConsoleSpace(ConsoleDisplays.Count.ToString(), maxLines, -1);
            ConsoleDisplays.Add(this);
        }

        public string LineFormat
        {
            get;
            set;
        }

        public long Counter
        {
            get { return Common.Patterns.Threading.LockFree.Read(ref this._counter); }
            set { Common.Patterns.Threading.LockFree.Update(ref this._counter, value); }
        }

        public bool Spinner
        {
            get;
            set;
        }

        public long Increment(string taskItem = null)
        {
            if (!string.IsNullOrEmpty(taskItem))
            {
                if(!this._taskItems.TryAdd(taskItem))
                {
                    return -1;
                }
            }

            return Interlocked.Increment(ref this._counter);
        }

        public long Increment(Common.IFilePath filePath)
        {
            return this.Increment(filePath?.FileName);
        }

        public long Decrement(string taskItem = null)
        {
            if (!string.IsNullOrEmpty(taskItem))
            {
                if(!this._taskItems.Remove(taskItem))
                {
                    return -1;
                }
            }

            var value = Interlocked.Decrement(ref this._counter);

            return value < 0 ? Common.Patterns.Threading.LockFree.Exchange(ref this._counter, 0) : value;
        }

        public long Decrement(Common.IFilePath filePath)
        {
            return this.Decrement(filePath?.FileName);
        }

        public bool TaskEnd(string taskItem)
        {
            return this._taskItems.Remove(taskItem);
        }

        public bool TaskEnd(Common.IFilePath filePath)
        {
            return this.TaskEnd(filePath?.FileName);
        }

        public int Pending
        {
            get { return this._taskItems.Count; }
        }

        public string Line(int pos, bool retry = true)
        {
            try
            {
                var taskItem = this._taskItems.Count == 0
                                       ? string.Empty
                                       : (pos >= 0 && pos < this._taskItems.Count) ? this._taskItems.ElementAtOrDefault(pos) : this._taskItems.LastOrDefault();

                return string.Format(LineFormat,
                                        Common.Patterns.Threading.LockFree.Read(ref this._counter),
                                        this._taskItems.Count,
                                        taskItem);
            }
            catch(System.ArgumentOutOfRangeException)
            {
            	if(retry)
                {
                    return this.Line(-1, false);
                }
            }
            return string.Empty;
        }

        int _runningCnt = 0;
        public string Line()
        {
            var currCount = this._taskItems.Count;

            if(currCount > 1 && currCount == this._runningCnt)
            {
                var rnd = new Random();
                return this.Line(rnd.Next(0, currCount - 1));
            }

            this._runningCnt = currCount;

            return this.Line(-1);
        }

        public void Terminate()
        {
            this.Terminated = true;
            this._taskItems.Clear();
        }

        public bool Terminated
        {
            get;
            private set;
        }

        public static void Start()
        {                        
            timer.Change(1000, 500);
        }

        public static void End()
        {
            timer.Change(0, 0);
            consoleWriter.ClearSpinner();
            //consoleWriter.DisableSpinner();
        }

        public static ConsoleWriter Console { get { return consoleWriter; } }

        static void TimerCallback(object state)
        {
            var consoleWriter = (ConsoleWriter)state;

            for (int nIndex = 0; nIndex < ConsoleDisplays.Count; ++nIndex)
            {
                if (ConsoleDisplays[nIndex].Terminated)
                {
                    consoleWriter.ReWrite(nIndex.ToString(), ConsoleDisplays[nIndex].Line());
                }
                else
                {
                    if (ConsoleDisplays[nIndex].Spinner && ConsoleDisplays[nIndex].Counter != 0)
                    {
                        consoleWriter.ReWriteAndTurn(nIndex.ToString(), ConsoleDisplays[nIndex].Line());
                    }
                    else
                    {
                        consoleWriter.ReWrite(nIndex.ToString(), ConsoleDisplays[nIndex].Line());
                    }
                }
            }
        }
    }
}
