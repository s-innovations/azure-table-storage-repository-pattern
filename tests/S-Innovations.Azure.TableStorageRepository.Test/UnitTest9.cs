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

    public class TestKeyEntity1
    {
         
        public string Id { get; set; }

        public string Test { get; set; }

    }

    public class TestKeyEntityContext1 : TableStorageContext
    {
        static EntityTypeConfigurationsContainer container = new EntityTypeConfigurationsContainer(new LoggerFactory());
        


        public TestKeyEntityContext1(CloudStorageAccount account)
            : base(new LoggerFactory(), container, account)
        {

            this.InsertionMode = SInnovations.Azure.TableStorageRepository.InsertionMode.AddOrReplace;
            this.TablePayloadFormat = Microsoft.WindowsAzure.Storage.Table.TablePayloadFormat.Json;
        }

        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<TestKeyEntity1>()
                .HasKeys(k => new {  k.Id })
                .WithKeyPropertyTransformation(k => k.Id, (p) => p.Replace('/', '_'))
                .ToTable("test12");


        }


        public ITableRepository<TestKeyEntity1> Entities { get; set; }
    }
    [TestClass]
    public class UnitTest9
    {

        [TestMethod]
        public void TestMethod1()
        {


            var _context  = new TestKeyEntityContext1(CloudStorageAccount.DevelopmentStorageAccount);
            

            var key = "test/as";
            //       a.Entities.Add(input);
            var s = _context.Entities.Where(k => k.Id == key).Take(1).AsTableQuery().FilterString;

            Assert.AreEqual("PartitionKey eq 'test_as'", s);
 

        }
    }
}
