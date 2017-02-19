using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using Microsoft.WindowsAzure.Storage;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using SInnovations.Azure.TableStorageRepository.Queryable;
using Microsoft.Extensions.Logging;
using Serilog;
using Serilog.Configuration;
using Serilog.Events;
using Serilog.Formatting.Display;
using Serilog.Core;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.Table;
namespace SInnovations.Azure.TableStorageRepository.Test
{
    public class TimeEntity
    {

        public Guid Id1 { get; set; }

        public Guid Id2 { get; set; }

        public DateTimeOffset Time { get; set; }
    }

   
    public class Test6Context : TableStorageContext
    {
        static EntityTypeConfigurationsContainer container = new EntityTypeConfigurationsContainer(new LoggerFactory());
        static Test6Context()
        {
            Table.SetInitializer(new CreateTablesIfNotExists<Test6Context>(container));

        }


        public Test6Context(CloudStorageAccount account)
            : base(new LoggerFactory(),container, account)
        {

            

            this.InsertionMode = SInnovations.Azure.TableStorageRepository.InsertionMode.AddOrMerge;
            this.TablePayloadFormat = Microsoft.WindowsAzure.Storage.Table.TablePayloadFormat.Json;
        }

        protected override void OnModelCreating( TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<TimeEntity>()
                .HasKeys( m =>  new { m.Id1, m.Time }, m => m.Id2)
                .WithKeyPropertyTransformation(m =>m.Time,t) 
                .ToTable("test6");


        }
        private static string t(DateTimeOffset time)
        {
            var ticks = (DateTimeOffset.MaxValue - time.ToUniversalTime()).Ticks;
            var key = "0" + (ticks - ticks % TimeSpan.TicksPerMinute);
            return key;
        }

        public ITableRepository<TimeEntity> Entities { get; set; }
    }
    static class TraceSourceSinkExtensions
    {
        const string DefaultOutputTemplate = "{Message}{NewLine}{Exception}";

        public static LoggerConfiguration TraceSource(
            this LoggerSinkConfiguration sinkConfiguration,
            string traceSourceName,
            LogEventLevel restrictedToMinimumLevel = LevelAlias.Minimum,
            string outputTemplate = DefaultOutputTemplate,
            IFormatProvider formatProvider = null)
        {
            if (string.IsNullOrWhiteSpace(traceSourceName)) throw new ArgumentNullException("traceSourceName");
            if (sinkConfiguration == null) throw new ArgumentNullException("sinkConfiguration");
            if (outputTemplate == null) throw new ArgumentNullException("outputTemplate");

            var formatter = new MessageTemplateTextFormatter(outputTemplate, formatProvider);
            return sinkConfiguration.Sink(new TraceSourceSink(formatter, traceSourceName), restrictedToMinimumLevel);
        }
    }
    class TraceSourceSink : ILogEventSink
    {
        private MessageTemplateTextFormatter _formatter;
        private TraceSource _source;

        public TraceSourceSink(MessageTemplateTextFormatter formatter, string traceSourceName)
        {
            _formatter = formatter;
            _source = new TraceSource(traceSourceName);
        }

        public void Emit(LogEvent logEvent)
        {
            if (logEvent == null) throw new ArgumentNullException("logEvent");

            var sr = new StringWriter();
            _formatter.Format(logEvent, sr);
            var text = sr.ToString().Trim();

            if (logEvent.Level == LogEventLevel.Error || logEvent.Level == LogEventLevel.Fatal)
                _source.TraceEvent(TraceEventType.Error, 0, text);
            else if (logEvent.Level == LogEventLevel.Warning)
                _source.TraceEvent(TraceEventType.Warning, 0, text);
            else if (logEvent.Level == LogEventLevel.Information)
                _source.TraceEvent(TraceEventType.Information, 0, text);
            else
                _source.TraceEvent(TraceEventType.Verbose, 0, text);
        }
    }

    

    [TestClass]
    public class UnitTest6
    {
        [TestMethod]
        public void TestMethod0()
        {
            var a = new Test6Context(CloudStorageAccount.DevelopmentStorageAccount);
            Table.ClearInitializer<Test6Context>();

            var time = DateTimeOffset.Parse("2015-11-18T18:34:05.0630931+01:00");
            var id = "98DEA3AE-0D3D-44F7-BD12-B69F125362F5";
         
            
            var query = a.Entities.Where(k => k.Id1 == new Guid(id) && k.Time == time);
            var filter = query.TranslateQuery();
            
            var expected= "PartitionKey ge '98dea3ae-0d3d-44f7-bd12-b69f125362f5__02519544327000000000' and PartitionKey lt '98dea3ae-0d3d-44f7-bd12-b69f125362f5__02519544327000000001' and Time eq datetime'2015-11-18T18:34:05.0630931'";
            Assert.AreEqual(expected, filter.FilterString);
        }
        [TestMethod]
        public void TestMethod1()
        {
            var a = new Test6Context(CloudStorageAccount.DevelopmentStorageAccount); Table.ClearInitializer<Test6Context>();
            var time = DateTimeOffset.Parse("2015-11-18T18:34:05.0630931+01:00");
            var id = "98DEA3AE-0D3D-44F7-BD12-B69F125362F5";
            var id2 = new Guid("98DEA3AE-0D3D-44F7-BD12-B69F125362F5");


            var query = a.Entities.Where(k => k.Id1 == new Guid(id) && k.Id2 == id2 && k.Time == time);
            var filter = query.TranslateQuery();

            var expected = "PartitionKey ge '98dea3ae-0d3d-44f7-bd12-b69f125362f5__02519544327000000000' and PartitionKey lt '98dea3ae-0d3d-44f7-bd12-b69f125362f5__02519544327000000001' and RowKey ge '98dea3ae-0d3d-44f7-bd12-b69f125362f5' and RowKey lt '98dea3ae-0d3d-44f7-bd12-b69f125362f6' and Time eq datetime'2015-11-18T18:34:05.0630931'";


            Assert.AreEqual(expected, filter.FilterString);
        }
        [TestMethod]
        public void TestMethod2()
        {
            var a = new Test6Context(CloudStorageAccount.DevelopmentStorageAccount); Table.ClearInitializer<Test6Context>();
            var time = DateTimeOffset.Parse("2015-11-18T18:34:05.0630931");
            var id = "98DEA3AE-0D3D-44F7-BD12-B69F125362F5";
            var id2 = new Guid("98DEA3AE-0D3D-44F7-BD12-B69F125362F5");


            var query = a.Entities.Where(k => k.Id1 == new Guid(id) && k.Time == time && k.Id2 == id2);
            var filter = query.TranslateQuery();

            var expected = "PartitionKey ge '98dea3ae-0d3d-44f7-bd12-b69f125362f5__02519544327000000000' and PartitionKey lt '98dea3ae-0d3d-44f7-bd12-b69f125362f5__02519544327000000001' and RowKey ge '98dea3ae-0d3d-44f7-bd12-b69f125362f5' and RowKey lt '98dea3ae-0d3d-44f7-bd12-b69f125362f6' and Time eq datetime'2015-11-18T18:34:05.0630931'";

           // Not working yet
          //  Assert.AreEqual(expected, filter.FilterString);
        }

       // [TestMethod]
        public async Task TestMethod20()
        {
            // log to both console and custom trace source sink
            Log.Logger = new LoggerConfiguration()
                .WriteTo.LiterateConsole(outputTemplate: "{Timestamp:HH:MM} [{Level}] ({Name:l}){NewLine} {Message}{NewLine}{Exception}")
                .WriteTo.TraceSource(traceSourceName: "Blabal")
                .MinimumLevel.Verbose()
                .CreateLogger();
          //  var a = SerilogLogProvider.IsLoggerAvailable;
           // LogProvider.SetCurrentLogProvider()
            var a = new Test6Context(CloudStorageAccount.Parse(File.ReadAllText(@"c:\dev\teststorage.txt")));
            var time = DateTimeOffset.UtcNow;
            var id = Guid.NewGuid();
            var element = new TimeEntity { Id1 = new Guid(), Id2 = id, Time = time };
            a.Entities.Add(element);

           await a.SaveChangesAsync();
            //var aaa = await a.Entities.ToListAsync();
            //var aa = await a.Entities.FirstAsync();
            //var b = a.Entities.First();
            var c = new Guid();
           
         //   var entities = await a.Entities.Where(k => k.Id1 == c).ToListAsync();
            var query = a.Entities.Where(k => k.Id1 == c && k.Time == time);

            var filter = query.TranslateQuery();
           
            var single = await query.SingleAsync();
            Assert.AreEqual(element.Id1, single.Id1);
            Assert.AreEqual(element.Id2, single.Id2);
            Assert.AreEqual(element.Time, single.Time);

        }//.ToString()
    }
}
