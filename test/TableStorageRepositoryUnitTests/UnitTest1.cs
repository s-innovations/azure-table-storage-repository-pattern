using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using SInnovations.Azure.TableStorageRepository;
using SInnovations.Azure.TableStorageRepository.Queryable;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System.Linq;

namespace TableStorageRepositoryUnitTests
{

    public class EventGridModel
    {
        public string TenantId { get; set; }
    }
    public class EventGridContext : TableStorageContext
    {
      
        public EventGridContext(ILoggerFactory logFactory, IEntityTypeConfigurationsContainer container, CloudStorageAccount storage)
            : base(logFactory, container, storage)
        {
            this.InsertionMode = InsertionMode.AddOrReplace;
            this.EnsureModelBuilded();
        }
        protected override void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {
            modelbuilder.Entity<EventGridModel>()
                .HasKeys(k => k.TenantId)
                .WithKeyTransformation(k => k.TenantId, s => s.Replace("-", ""), "PartitionKey")
                
                            .ToTable("EventGrid");
 
        }


        public ITableRepository<EventGridModel> Grids { get; set; }
        
    }

    [TestClass]
    public class UnitTest1
    {
        [TestMethod]
        public void TestMethod1()
        {
            var factory = new LoggerFactory();
            var container = new EntityTypeConfigurationsContainer(factory);
            var context = new EventGridContext(factory, container, CloudStorageAccount.DevelopmentStorageAccount);
            var q = context.Grids.Where(test => test.TenantId == "hej-med-dig").AsTableQuery().FilterString;

            Assert.AreEqual("PartitionKey eq 'hejmeddig'",q);

        }
    }
}
