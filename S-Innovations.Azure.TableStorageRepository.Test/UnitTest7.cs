using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using Microsoft.WindowsAzure.Storage;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.IO;
using System.Threading.Tasks;
using System.Linq;
using SInnovations.Azure.TableStorageRepository.Queryable;
using SInnovations.Azure.TableStorageRepository.Logging;
using Serilog;
using Serilog.Configuration;
using Serilog.Events;
using Serilog.Formatting.Display;
using Serilog.Core;
using System.Diagnostics;
using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json.Linq;
using System.Collections.Generic;
using System.Text;
using SInnovations.Azure.TableStorageRepository.PropertyEncoders;

namespace SInnovations.Azure.TableStorageRepository.Test
{
    public class TestEntity
    {

        public string Id { get; set; }
        public JObject Geometry { get; set; }

        public IDictionary<string, string> Tags { get; set; } = new Dictionary<string, string>();

    }

   
    public class Test7Context : TableStorageContext
    {
        static Test7Context()
        {
            Table.SetInitializer(new CreateTablesIfNotExists<Test7Context>());

        }


        public Test7Context(CloudStorageAccount account)
            : base( account)
        {
            
            this.InsertionMode = SInnovations.Azure.TableStorageRepository.InsertionMode.AddOrMerge;
            this.TablePayloadFormat = Microsoft.WindowsAzure.Storage.Table.TablePayloadFormat.Json;
        }

        protected override void OnModelCreating( TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<TestEntity>()
                .HasKeys( m =>  m.Id, m => "a")
                .WithPropertyOf(m=>m.Geometry,GeometryDecode,GeometryEncode)
                .WithTagsProperty(m=>m.Tags)
                .ToTable("test7");


        }
     
        public static Task<EntityProperty> GeometryEncode( JObject plainText)
        {
            return Task.FromResult(new EntityProperty(Encoding.UTF8.GetBytes(plainText.ToString())));
        }

        public static Task<JObject> GeometryDecode(EntityProperty prop)
        {
            return Task.FromResult(JObject.Parse(Encoding.UTF8.GetString(prop.BinaryValue)));
        }

        public ITableRepository<TestEntity> Entities { get; set; }
    }
   
    

    [TestClass]
    public class UnitTest7
    {
      //  [TestMethod]
        public async Task TestMethod0()
        {
            var a = new Test7Context(CloudStorageAccount.DevelopmentStorageAccount);
            var input = new TestEntity
            {
                Id = "my",
                Geometry = new JObject(new JProperty("test", "hello")),
                Tags = new Dictionary<string, string> { { "test", "ttaa" }, { "ab", "bb" } }
            };
            a.Entities.Add(input);
            await a.SaveChangesAsync();
            var output = a.Entities.First();

            Assert.AreEqual(input.Id, output.Id);
            Assert.AreEqual(input.Geometry.ToString(), output.Geometry.ToString());
            Assert.AreEqual(input.Tags.Count, output.Tags.Count);
            Assert.AreEqual(string.Join("",input.Tags.Keys), string.Join("", output.Tags.Keys));
            Assert.AreEqual(string.Join("", input.Tags.Keys.Select(k=>input.Tags[k])), string.Join("", output.Tags.Keys.Select(k => output.Tags[k])));
        }
       
    }
}
