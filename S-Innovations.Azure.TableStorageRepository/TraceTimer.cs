using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository
{
    /// <summary>
    /// Wrap TraceTimer around a code block with using and it will write out the time used for the block to trace.
    /// </summary>
    public class TraceTimer : IDisposable
    {
        /// <summary>
        /// Set the TraceLevel that should be written to, default is verbose.
        /// </summary>
        public TraceLevel TraceLevel { get; set; }
        /// <summary>
        /// If a value is set then the trace will only be written if it takes longer than the threshold.
        /// </summary>
        public long Threshold { get; set; }

        string _action;
        private double time;

        /// <summary>
        /// Get the starting time for this trace timer.
        /// </summary>
        public DateTime Start { get; private set; }

        public TraceTimer(string action)
        {
            _action = action;
            Start = DateTime.Now;
            TraceLevel = TraceLevel.Verbose;
            Threshold = 0;


        }

        private string TraceString(string format, params object[] args)
        {

            string msInfo = time.ToString("#########");

            return string.Format("{0}ms : {1}", msInfo.PadLeft(9), string.Format(format, args));
        }

        public void Dispose()
        {
            time = (DateTime.Now - Start).TotalMilliseconds;
            if (time > Threshold)
                switch (TraceLevel)
                {
                    case TraceLevel.Error:
                        Trace.TraceError(TraceString(_action));
                        break;
                    case TraceLevel.Info:
                        Trace.TraceInformation(TraceString(_action));
                        break;
                    case TraceLevel.Warning:
                        Trace.TraceWarning(TraceString(_action));
                        break;
                    default:
                        Trace.WriteLine(TraceString(_action), "Verbose");
                        break;
                }



        }
    }
}
