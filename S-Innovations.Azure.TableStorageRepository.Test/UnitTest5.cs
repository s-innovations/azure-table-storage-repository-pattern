using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using Microsoft.WindowsAzure.Storage;
using System.IO;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.Linq;

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
            Table.SetInitializer(new CreateTablesIfNotExists<ProductContext>());
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
             //   .HasKeys(pk => pk.Category,rk=>string.Format("{0}_{1}", rk.Store.ToLower(), rk.ProductId))              
                .ToTable("unittest5");

            base.OnModelCreating(modelbuilder);
        }
        public ITableRepository<Product> Products { get; set; }
    }

    [TestClass]
    public class UnitTest5
    {
        [TestMethod]
        public void TestMethod1()
        {
            var cat = "test";
            var ctx = new ProductContext();
            ctx.Products.Add(new Product
            {
                ProductId = 1,
                Name = "MyName",
                ImageName = "atest",
                Category = cat,
                Description = "adsad",
                Store = "test",
            });
            ctx.SaveChangesAsync().Wait();

            var products = (from f in ctx.Products
                            where f.Category == cat
                            select f).ToArray();

        }
    }
}
