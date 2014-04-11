using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.WindowsAzure.Storage;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Auth;

namespace SInnovations.Azure.TableStorageRepository.Test
{

    public class POCOModel
    {
        public string Test { get; set; }
    }
    public class TableEntityModel : TableEntity
    {

        public string Test { get; set; }
    }

    public class MyTableStorageContext : TableStorageContext
    {   
        public MyTableStorageContext() : base(new CloudStorageAccount(new StorageCredentials("c1azuretests","GB2JACAMd2Y7Hw1KCGgw+pwzLmvaR2YRY5nlcdQb9AkjmhQKCeYlkOGvZoGAWoA4zjvZhEnRJgDqJuboy/ZU4A=="),true))
        {

        }
        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<POCOModel>().HasKeys((c) => c.Test, c => Guid.NewGuid().ToString())
                .ToTable("PocoTable");
            modelbuilder.Entity<TableEntityModel>().HasKeys((c) => c.Test, c => Guid.NewGuid().ToString())
                .ToTable("EntityModelTable");
        }

        public ITableRepository<POCOModel> People { get; set; }
        public ITableRepository<TableEntityModel> Pets { get; set; }
       
    }
    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            return;// This test only work local when connectionstring is updated.

            var context = new MyTableStorageContext();
            context.People.Add(new POCOModel() { Test = "hello" });
            context.Pets.Add(new TableEntityModel { Test = "hello" });
            context.SaveChangesAsync().Wait();

            var iqueryable_querys = from ent in context.Pets.Source
                                    where ent.PartitionKey == "hello"
                                    select ent;
            var arr0 = iqueryable_querys.ToArray();
            // Above only works when model type has base of TableEntity.
            //Else fluent querys are needed.
            var arr1 = context.People.FluentQuery(TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, "hello")).ToArray();

        }
    }
}
