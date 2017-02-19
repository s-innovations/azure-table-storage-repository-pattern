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
using SInnovations.Azure.TableStorageRepository;
using Microsoft.Extensions.Logging;

namespace SInnovations.Azure.TableStorageRepository.Test
{

#if DEBUG
    public class MyTestPoco
    {
        public string Name { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }
        public Guid Id { get; set; }

        public virtual ICollection<SubItem> Items { get; set; }
    }

    public class MyTestPocoEntity : TableEntity
    {
        public string Name { get; set; }
        public string LastName { get; set; }
        public int Age { get; set; }
        public Guid Id { get; set; }

        public virtual ICollection<SubItem> Items { get; set; }
    }

    public class MyTestClass
    {
        public string LolKey { get; set; }
        public string Name { get; set; }

        public ICollection<SubItem> Items {get;set;}
    }
    public class SubItem
    {
        public string Name { get; set; }
        public string LastName { get; set; }
    }
    public class NewTableStorageContext : TableStorageContext
    {

        static EntityTypeConfigurationsContainer container = new EntityTypeConfigurationsContainer(new LoggerFactory());

        public NewTableStorageContext()
            : base(new LoggerFactory(),container,new CloudStorageAccount(
            new StorageCredentials("c1azuretests",File.ReadAllText("C:\\dev\\storagekey.txt")), true))
        {

            this.InsertionMode = InsertionMode.AddOrMerge;
        }
        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<SubItem>().HasKeys(t=>t.Name,t=>t.LastName).ToTable("testtable");
            modelbuilder.Entity<MyTestPoco>()
                .HasKeys(y => y.Name, y => y.LastName)
                .WithCollectionOf(t => t.Items, (source, item) => (from i in source where i.Name == item.Name select i))
                .ToTable("testsfds");
            modelbuilder.Entity<MyTestPocoEntity>()
                .HasKeys(y => y.Name, y => y.LastName)
                .WithCollectionOf(t=>t.Items,(source,item) => (from i in source where i.Name == item.Name select i))
                .ToTable("testsfds");


            modelbuilder.Entity<MyTestClass>()
                .HasKeys(y => y.LolKey, t => "")
                .WithPropertyOf(t => t.Items)//, 
                  //  p => JsonConvert.DeserializeObject<List<SubItem>>(p.StringValue), 
                  //  p => new EntityProperty(JsonConvert.SerializeObject(p)))
                .ToTable("tesdsfaa");
                
            
            base.OnModelCreating(modelbuilder);
        }

        public ITableRepository<MyTestPoco> People { get; set; }
        public ITableRepository<MyTestPocoEntity> People1 { get; set; }
        public ITableRepository<SubItem> SubItems { get; set; }

        public ITableRepository<MyTestClass> TestLocalItems { get; set; }
        
    }
    [TestClass]
    public class UnitTest2
    {

          [TestMethod]
        public async Task TestLocalItems()
        {
            var context = new NewTableStorageContext();

            context.TestLocalItems.Add(
                new MyTestClass
                {
                    LolKey = "adsad",
                    Name = "adsad",
                    Items = new List<SubItem>{ 
                        new SubItem
                        {
                            LastName="adsa", 
                            Name="sda"
                        },
                        new SubItem
                        { 
                            LastName="adsa",
                            Name="sda"
                        } 
                    }
                });
            await context.SaveChangesAsync();

            var items = (from ent in context.TestLocalItems select ent).ToArray();
            Trace.WriteLine(JsonConvert.SerializeObject(items));
            Trace.WriteLine(items.First().Items.GetType().ToString());
        }
        [TestMethod]
        public async Task SelectAll()
        {

            var context = new NewTableStorageContext();

            context.People.Add(new MyTestPoco { LastName = "as", Name = "adsad" });

            await context.SaveChangesAsync();

            var query = from ent in context.People select ent;

            var result = query.ToArray();
        }

        [TestMethod]
        public async Task SelectWithWhere()
        {

            var context = new NewTableStorageContext();

            context.People.Add(new MyTestPoco { LastName = "as", Name = "adsad1" });
            context.People.Add(new MyTestPoco { LastName = "as", Name = "adsad2" });
            context.People.Add(new MyTestPoco { LastName = "as", Name = "adsad3" });
            context.People.Add(new MyTestPoco { LastName = "as", Name = "adsad4" });
            context.People.Add(new MyTestPoco { LastName = "as", Name = "adsad5" });
            context.People.Add(new MyTestPoco { LastName = "as", Name = "adsad6" });

            await context.SaveChangesAsync();

            //This call should project name to PartitionKey
            var query = from ent in context.People
                        where ent.Name == "adsad1"
                        select ent;

            //The following query get build such only the Name column is
            //retrieved from the server, and its mapped such it know to do
            //the filter on partition key and not the name. (Model Builder)
            var result = query.Select(t=>t.Name).ToArray();
        }

        [TestMethod]
        public async Task SelectServerSideProjection()
        {

            var context = new NewTableStorageContext();

            context.SubItems.Add(new SubItem { Name = "adsad1", LastName= "blasad1" });
            context.SubItems.Add(new SubItem { Name = "adsad1", LastName = "blasad2" });
            context.SubItems.Add(new SubItem { Name = "adsad2", LastName = "blasa3d" });
            context.SubItems.Add(new SubItem { Name = "adsad2", LastName = "blasad4" });
            
            await context.SaveChangesAsync();


            //This call should project name to PartitionKey
            var query = from ent in context.People
                        select new { ent.Name, ent.Age };

            //The following query get build such only the Name column is
            //retrieved from the server, and its mapped such it know to do
            //the filter on partition key and not the name. (Model Builder)
            var result = query.Select(t => t).Where(t=>true).Select(t=>t.Name).ToArray();

            var withTableEntity = (from ent in context.People1
                                  select new { ent.Name }).ToArray();

            var adsad1 = await context.People1.FindByKeysAsync("adsad1", "as");
            var subitems = (from ent in adsad1.Items
                           select ent).ToArray();
            var tst = (from ent in ((ITableRepository<SubItem>)adsad1.Items).BaseQuery
                      select ent).ToArray();

            var allsubs = (from ent in context.SubItems select ent).ToArray();
            
        }
    }
#endif
}
