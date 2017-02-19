using Microsoft.Extensions.Logging;
using System;

namespace SInnovations.Azure.TableStorageRepository
{
    /// <summary>
    /// Wrap TraceTimer around a code block with using and it will write out the time used for the block to trace.
    /// </summary>
    public class TraceTimer : IDisposable
    {
        private readonly ILogger Logger;
        
        /// <summary>
        /// Set the TraceLevel that should be written to, default is verbose.
        /// </summary>
        public LogLevel TraceLevel { get; set; }
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

        public TraceTimer(ILogger logger, string action)
        {
            _action = action;
            Start = DateTime.Now;
            TraceLevel = LogLevel.Trace;
            Threshold = 0;
            this.Logger = logger;


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
                Logger.Log<object>(TraceLevel, new EventId(), null, null, (s, e) => TraceString(_action));

        }
    }
}
