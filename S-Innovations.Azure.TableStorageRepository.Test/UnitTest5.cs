using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using System.IO;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.Linq;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Test
{
    public class Product
    {
        public int ProductId { get; set; }
        public string Name { get; set; }
        public string Description { get; set; }
        public string ImageName { get; set; }
        public string Store { get; set; }
        public string Category { get; set; }
    }
    public class ProductContext : TableStorageContext
    {

        static ProductContext()
        {
            //TO DROP THE TEST TABLE USE COMMENTED LINE. Dont use in production :)
            Table.SetInitializer(new CreateTablesIfNotExists<ProductContext>());
           // Table.SetInitializer(new DropTablesAndCreateIfExist<ProductContext>());
            
        }
        public ProductContext()
            : base(CloudStorageAccount.Parse(File.ReadAllText(@"c:\dev\teststorage.txt")))
        {
          
            this.InsertionMode = InsertionMode.AddOrMerge;
         
        }
        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<Product>()
                .HasKeys(pk => pk.Category, rk =>new {  rk.Store, rk.ProductId})                
                .ToTable("unittest7");

            base.OnModelCreating(modelbuilder);
        }
        public ITableRepository<Product> Products { get; set; }
    }

    [TestClass]
    public class UnitTest5
    {
        [TestMethod]
        public async Task TestMethod1()
        {
            var cat = "test";
            var ctx = new ProductContext();
            var insertObj = new Product
            {
                ProductId = 1,
                Name = "MyName",
                ImageName = "atest",
                Category = cat,
                Description = "adsad",
                Store = "test_abc",
            };
            ctx.Products.Add(insertObj);
            ctx.SaveChangesAsync().Wait();

            var products = (from f in ctx.Products
                            where f.Category == cat
                            select f).ToArray();

            Assert.AreEqual(insertObj.ProductId, products[0].ProductId, "ProductId");
            Assert.AreEqual(insertObj.Store, products[0].Store, "Store");
            Assert.AreEqual(insertObj.Name, products[0].Name, "Name");
            Assert.AreEqual(insertObj.ImageName, products[0].ImageName, "ImageName");
            Assert.AreEqual(insertObj.Description, products[0].Description, "Description");

            var tes = await ctx.Products.FindByKeysAsync("test", "test_abc__1");

            //All is okay?

        }
    }
}
