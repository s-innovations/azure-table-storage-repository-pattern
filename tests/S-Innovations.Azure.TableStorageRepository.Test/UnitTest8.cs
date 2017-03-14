using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.Extensions.Logging;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using Microsoft.WindowsAzure.Storage;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.Linq;
using SInnovations.Azure.TableStorageRepository.Queryable;

namespace SInnovations.Azure.TableStorageRepository.Test
{

    public class TestKeyEntity
    {
        public string FlowId { get; set; }
        public string Id { get; set; }

        public string Test { get; set; }

    }

    public class TestKeyEntityContext : TableStorageContext
    {
        static EntityTypeConfigurationsContainer container = new EntityTypeConfigurationsContainer(new LoggerFactory());
        static TestKeyEntityContext()
        {
         //   Table.SetInitializer(new CreateTablesIfNotExists<TestKeyEntityContext>(container));

        }


        public TestKeyEntityContext(CloudStorageAccount account)
            : base(new LoggerFactory(), container, account)
        {

            this.InsertionMode = SInnovations.Azure.TableStorageRepository.InsertionMode.AddOrReplace;
            this.TablePayloadFormat = Microsoft.WindowsAzure.Storage.Table.TablePayloadFormat.Json;
        }

        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<TestKeyEntity>()
                .HasKeys(k => new { k.FlowId, k.Id })
                .WithKeyPropertyTransformation(k => k.FlowId, (p) => p.Replace('/', '_'))
                .ToTable("test12");


        }


        public ITableRepository<TestKeyEntity> Entities { get; set; }
    }
    [TestClass]
    public class UnitTest8
    {

        [TestMethod]
        public void TestMethod1()
        {


            var _context  = new TestKeyEntityContext(CloudStorageAccount.DevelopmentStorageAccount);
            var input = new TestKeyEntity
            {
                Id = "my",
                Test = "a"
            };

            var key = "test/as";
            //       a.Entities.Add(input);
            var s = _context.Entities.Where(k => k.FlowId == key).Take(1).AsTableQuery().FilterString;

            Assert.AreEqual("PartitionKey ge 'test_as' and PartitionKey lt 'test_at'", s);
 

        }
    }
}
