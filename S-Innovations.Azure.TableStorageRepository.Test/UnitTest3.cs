using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Auth;
using System.IO;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.Threading.Tasks;
using System.Linq;
using Microsoft.WindowsAzure.Storage.Table;
using System.Collections.Generic;
using Newtonsoft.Json;
using System.Diagnostics;
using SInnovations.Azure.TableStorageRepository.Queryable;

namespace SInnovations.Azure.TableStorageRepository.Test
{
//Cant run these tests on myget build services
#if DEBUG
   
    public class MyModel
    {
        public string Item1 { get; set; }
        public string Item2 { get; set; }
        public string Item3 { get; set; }
        public string Item4 { get; set; }
        public string Item5 { get; set; }
    }

    public class Context : TableStorageContext
    {
        public Context()
            : base(new CloudStorageAccount(
            new StorageCredentials("c1azuretests",File.ReadAllText("C:\\dev\\storagekey.txt")), true))
        {

            this.InsertionMode = InsertionMode.AddOrReplace;
        }
        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {

            modelbuilder.Entity<MyModel>()
                .UseBase64EncodingFor(m => m.Item2)
                .HasKeys(m => m.Item1, m => "")
                .WithIndex(m => new { m.Item2, m.Item3 })
                .ToTable("testTable003");

            
            base.OnModelCreating(modelbuilder);
        }

        public ITableRepository<MyModel> Models { get; set; }
        
    }
    [TestClass]
    public class UnitTest3
    {

          [TestMethod]
        public async Task TestLocalItems()
        {
            var context = new Context();
              
            context.Models.Add(new MyModel { Item1 = Guid.NewGuid().ToString(), Item2="poul",Item3="google" });


            await context.SaveChangesAsync();

            var result = await context.Models.FindByIndexAsync("poul","google");

            var testAsync = await (from ent in context.Models
                            where ent.Item2 == "poul"
                            select ent).ToListAsync();
            Assert.AreEqual((from ent in context.Models where ent.Item2 == "poul" select ent).ToArray().Length, testAsync.Count);

            Trace.TraceInformation(testAsync.Count.ToString());
        }
       
   
    }
#endif
}
