using Microsoft.Azure.Cosmos.Table;
using Microsoft.Extensions.Logging;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository
{
    internal static class Factory
    {
        internal static MethodInfo EntityConfigTypeMethod = typeof(EntityTypeConfigurationsContainer).GetRuntimeMethods().Single(x => x.IsGenericMethod && x.Name == "Entity");
        internal static Type TablePocoRepositoryType = typeof(TablePocoRepository<>);
        internal static Type EntityConfigurationType = typeof(EntityTypeConfiguration<>);
        internal static Type LazyType = typeof(Lazy<>);
        internal static Type FuncType = typeof(Func<>);

        public static ITableRepository RepositoryFactory(ILoggerFactory loggerFactory, IEntityTypeConfigurationsContainer container, ITableStorageContext ctx, Type EntityType)
        {
            // var TableRepositoryType = IsEntityType ? TableEntityRepositoryType : TablePocoRepositoryType;

            var EntityConfigType = EntityConfigurationType.MakeGenericType(EntityType);
            var lazyType = LazyType.MakeGenericType(EntityConfigType);
            var argumetns = new Object[] 
                    {
                        loggerFactory,
                        ctx,                         
                        Activator.CreateInstance( //Lazy<EntityTypeConfiguration<EntityType>>
                            lazyType, 
                            new Object[]{
                                EntityConfigTypeMethod.MakeGenericMethod(EntityType).CreateDelegate(FuncType.MakeGenericType(EntityConfigType),container)
                            }) 
                    };


            var rep = Activator.CreateInstance(
                TablePocoRepositoryType.MakeGenericType(EntityType), argumetns) as ITableRepository;


            return rep;
        }
        public static ITableRepository<TEntity> RepositoryFactory<TEntity>(ILoggerFactory factory, IEntityTypeConfigurationsContainer container, ITableStorageContext ctx)
        {
            return RepositoryFactory(factory, container,ctx, typeof(TEntity)) as ITableRepository<TEntity>;
        }


    }
   public enum InsertionMode
   {
       Add,
       AddOrMerge,
       AddOrReplace
   }
    public abstract class TableStorageContext : ITableStorageContext
    {
        private readonly ILogger Logger;
        private readonly ILoggerFactory logFactory;
        private readonly IEntityTypeConfigurationsContainer container;

        public int MaxDegreeOfParallelism { get; set; } = 10;
        public CloudStorageAccount StorageAccount {get; private set;}
        private static object _buildLock = new object();
       
        public InsertionMode InsertionMode { get; set; }

        private readonly Lazy<CloudTableClient> _client;
        private List<ITableRepository> repositories = new List<ITableRepository>();

        public bool AutoSaveOnDispose { get; set; }
        private static object _lock = new object();
        public TableStorageContext(ILoggerFactory logFactory, IEntityTypeConfigurationsContainer container, CloudStorageAccount storage)
        {
            this.container = container;
            this.logFactory = logFactory;
            Logger = logFactory.CreateLogger<TableStorageContext>();
            StorageAccount = storage;
            _client = new Lazy<CloudTableClient>(CreateClient);


            TableStorageModelBuilder builder = container.GetModelBuilder(this, OnModelCreating);
               
            //How long time does it take.
            using (new TraceTimer(Logger,"Using reflection to set propeties.") { TraceLevel = Microsoft.Extensions.Logging.LogLevel.Trace, Threshold = 0 })
            {
                BuildModel(builder);

            }
          
          
           

            InsertionMode = InsertionMode.Add;
        }
        protected void EnsureModelBuilded()
        {
            this.repositories.ForEach(m=>m.ForceModelCreation());
        }
        public void Dispose()
        {
            this.Dispose(true);
            GC.SuppressFinalize(this);
        }
        protected virtual void Dispose(bool disposing)
        {
          
        }
        

        //public TableEntityRepository<TEntity> TableEntityRepository<TEntity>() where TEntity : ITableEntity,new()
        //{
        //    throw new NotImplementedException();
        //}

        public virtual Task SaveChangesAsync()
        {

           return Task.WhenAll(repositories.Select(rep => rep.SaveChangesAsync()));
        }


        protected virtual void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {

        }

        private void BuildModel(TableStorageModelBuilder modelbuilder)
        {
         
            foreach(var repository in this.GetType().GetRuntimeProperties().Where(p=>p.PropertyType.GetTypeInfo().IsGenericType))
            {
                if (typeof(ITableRepository<>).GetTypeInfo().IsAssignableFrom(repository.PropertyType.GetGenericTypeDefinition().GetTypeInfo()))
                {
                    var EntityType = repository.PropertyType.GenericTypeArguments[0];
                    var rep = Factory.RepositoryFactory(logFactory, container,this, EntityType);
                  
                    repositories.Add(rep);
                    repository.SetValue(this, rep);
                    

                }
            }
        }
       

        public CloudTable GetTable(string name)
        {
          
            var tbl= _client.Value.GetTableReference(name);
        
            //if (createIfNotExists)
            //{
            //    tbl.CreateIfNotExists();
            //}
            return tbl;
        }


        public CloudTable GetTable<T1>()
        {
            return _client.Value.GetTableReference(container.Entity<T1>().TableName(this));
        }


        private CloudTableClient CreateClient()
        {
           var client= StorageAccount.CreateCloudTableClient();

           
           if (this.RetryPolicy != null)
               client.DefaultRequestOptions.RetryPolicy = RetryPolicy;
            if(this.TablePayloadFormat.HasValue)
                client.DefaultRequestOptions.PayloadFormat = TablePayloadFormat;
           return client;
        }



        public IRetryPolicy RetryPolicy
        {
            get;set;
        }
        public TablePayloadFormat? TablePayloadFormat
        {
            get;
            set;
        }
        private static string sep = null;
        public static string KeySeparator
        {
            get { return sep ?? "__"; }
            set { sep = value; }
        }
    }
}
