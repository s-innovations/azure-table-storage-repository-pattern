using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using Microsoft.WindowsAzure.Storage;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.IO;
using System.Threading.Tasks;
using System.Linq;

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
        static Test6Context()
        {
            Table.SetInitializer(new CreateTablesIfNotExists<Test6Context>());

        }


        public Test6Context()
            : base(CloudStorageAccount.Parse(File.ReadAllText(@"c:\dev\teststorage.txt")))
        {

            

            this.InsertionMode = SInnovations.Azure.TableStorageRepository.InsertionMode.AddOrMerge;
            this.TablePayloadFormat = Microsoft.WindowsAzure.Storage.Table.TablePayloadFormat.Json;
        }
        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<TimeEntity>()
                .HasKeys(m => new { m.Id1, A = t(m.Time) }, m => m.Id2)
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

    [TestClass]
    public class UnitTest6
    {
//[TestMethod]
        public async Task TestMethod1()
        {

            var a = new Test6Context();

            a.Entities.Add(new TimeEntity { Id1 = new Guid(), Id2 = Guid.NewGuid(), Time = DateTimeOffset.UtcNow });

            await a.SaveChangesAsync();

            var b = a.Entities.First();
        }
    }
}
