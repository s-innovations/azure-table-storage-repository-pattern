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
using SInnovations.Azure.TableStorageRepository.Spatial;
using Newtonsoft.Json;
using SInnovations.Azure.TableStorageRepository.Spatial.DotSpatial;

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
                .HasKeys( m =>  m.Id, m => "")
                .WithGeometry(m=>m.Geometry)
                .WithTagsProperty(m=>m.Tags)
                .WithSpatialIndex(m=>m.Geometry,m=>m.Id, new ExtentProvider())
                .ToTable("test11");


        }
     
      
        public ITableRepository<TestEntity> Entities { get; set; }
    }

    public class ExtentProvider : ISpatialExtentProvider<TestEntity>, ISpatialExtentProvider<JObject>
    {
        public double[] GetExtent(JObject entity)
        {
            var coordinates = entity.SelectToken("geometry.coordinates");

            return Extent(coordinates);
        }

        public double[] GetExtent(TestEntity entity)
        {
            //minX, minY, maxX, maxY
            var coordinates = entity.Geometry.SelectToken("coordinates");

            return Extent(coordinates);
        }

        private static double[] Extent(JToken coordinates)
        {
            var geom = coordinates.ToObject<double[][][]>();
            var extent = new double[] { Double.MaxValue, double.MaxValue, double.MinValue, double.MinValue };
            foreach (var coord in geom.SelectMany(k => k))
            {
                extent[0] = Math.Min(coord[0], extent[0]);
                extent[1] = Math.Min(coord[1], extent[1]);
                extent[2] = Math.Max(coord[0], extent[2]);
                extent[3] = Math.Max(coord[1], extent[3]);
            }

            return extent;
        }

        public bool SplitByExtent(TestEntity entity, double[] extentTest)
        {
            return false;
        }

        public bool SplitByExtent(JObject entity, double[] extentTest)
        {
            return false;
        }
    }

    [TestClass]
    public class UnitTest7
    {

      //  [TestMethod]
        public async Task TestMethod5()
        {
            var tilesystem = new TileSystem();
            var a = new Test7Context(CloudStorageAccount.DevelopmentStorageAccount);

            foreach (var z in Enumerable.Range(0, 5)) {
                foreach (var x in Enumerable.Range(0, 1<<z))
                    foreach (var y in Enumerable.Range(0, 1 << z))
                    {
                        var input = new TestEntity
                        {
                            Id = tilesystem.TileXYToQuadKey(x,y,z).PadRight(10,'_'),
                          //  Id2 = new string(TileSystem.TileXYToQuadKey(x, y, z).Reverse().ToArray()).PadRight(10, '_'),
                            Geometry = new JObject(
                            new JProperty("type", "Polygon"),
                            new JProperty("coordinates", JArray.Parse(("[[[ 9.082260131835936,55.697711785689854 ],[9.082260131835936,55.781593089920264], [9.24224853515625,55.781593089920264 ], [ 9.24224853515625, 55.697711785689854],[  9.082260131835936,55.697711785689854]]]")
                            ))),
                            Tags = new Dictionary<string, string> { { "test", "ttaa" }, { "ab", "bb" } }
                        };
                        a.Entities.Add(input);
                }
            }

            await a.SaveChangesAsync();
        }
     //   [TestMethod]
        public async Task TestMethod0()
        {
            var a = new Test7Context(CloudStorageAccount.DevelopmentStorageAccount);
            var input = new TestEntity
            {
                Id = "my",
                Geometry = new JObject(
                new JProperty("type", "Polygon"),
                new JProperty("coordinates", JArray.Parse(("[[[ 9.082260131835936,55.697711785689854 ],[9.082260131835936,55.781593089920264], [9.24224853515625,55.781593089920264 ], [ 9.24224853515625, 55.697711785689854],[  9.082260131835936,55.697711785689854]]]")
                ))),
                Tags = new Dictionary<string, string> { { "test", "ttaa" }, { "ab", "bb" } }
            };
            a.Entities.Add(input);

            await a.SaveChangesAsync();
            var output = a.Entities.Where(k => k.Id == "my").First();

            Assert.AreEqual(input.Id, output.Id);
            Assert.AreEqual(input.Geometry.ToString(), output.Geometry.ToString());
            Assert.AreEqual(input.Tags.Count, output.Tags.Count);
            Assert.AreEqual(string.Join("", input.Tags.Keys), string.Join("", output.Tags.Keys));
            Assert.AreEqual(string.Join("", input.Tags.Keys.Select(k => input.Tags[k])), string.Join("", output.Tags.Keys.Select(k => output.Tags[k])));
        }

    //    [TestMethod]
        public async Task TestMethod1()
        {
            var a = new Test7Context(CloudStorageAccount.DevelopmentStorageAccount);


            var obj = JObject.Load(new JsonTextReader(new StreamReader(this.GetType().Assembly.GetManifestResourceStream("SInnovations.Azure.TableStorageRepository.Test.test.json"))));
            var features = obj.SelectToken("features").Select(f => new TestEntity
            {
                Id = Guid.NewGuid().ToString("N"),
                Geometry = f.SelectToken("geometry") as JObject,
                Tags = new Dictionary<string, string> { { "test", "ttaa" }, { "ab", "bb" } }
            }).ToArray();
            foreach (var e in features)
                a.Entities.Add(e);

            await a.SaveChangesAsync();

            

        }

      //  [TestMethod]
        public async Task TestMethod4()
        {
            var a = new Test7Context(CloudStorageAccount.DevelopmentStorageAccount);
            var intersect = JObject.Load(new JsonTextReader(new StreamReader(this.GetType().Assembly.GetManifestResourceStream("SInnovations.Azure.TableStorageRepository.Test.intersect.json"))));
            var test = await a.Entities.SpatialIntersectAsync(intersect, new ExtentProvider(),new SpatialIntersectService<TestEntity,JObject>(k=>Task.FromResult(k.Geometry),k=>Task.FromResult(k.SelectToken("geometry") as JObject)));


            Assert.AreEqual(4, test.Count());

        }

     //   [TestMethod]
        public async Task TestMethod2()
        {
            var tableQuery = new TableQuery<EntityAdapter<TestEntity>>();
            var quadKey = "02301";
            var tilePrefix = new string(quadKey.Reverse().ToArray()).PadLeft(30, '_');
            var lastChar = tilePrefix.Last();
            var lessThan = tilePrefix.Substring(0, tilePrefix.Length - 1) + ++lastChar;
            tableQuery.FilterString = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, tilePrefix);
            tableQuery.FilterString = TableQuery.CombineFilters(tableQuery.FilterString, TableOperators.And, TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, lessThan));



            Console.WriteLine(tableQuery.FilterString);
        }
    }
}
