using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
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
        internal static MethodInfo EntityConfigTypeMethod = typeof(EntityTypeConfigurationsContainer).GetMethods().Single(x => x.IsGenericMethod && x.Name == "Entity");
        internal static Type TablePocoRepositoryType = typeof(TablePocoRepository<>);
        internal static Type EntityConfigurationType = typeof(EntityTypeConfiguration<>);
        internal static Type LazyType = typeof(Lazy<>);
        internal static Type FuncType = typeof(Func<>);

        public static ITableRepository RepositoryFactory(ITableStorageContext ctx, Type EntityType)
        {
            // var TableRepositoryType = IsEntityType ? TableEntityRepositoryType : TablePocoRepositoryType;

            var EntityConfigType = EntityConfigurationType.MakeGenericType(EntityType);
            var lazyType = LazyType.MakeGenericType(EntityConfigType);
            var argumetns = new Object[] 
                    { 
                        ctx, 
                        Activator.CreateInstance( //Lazy<EntityTypeConfiguration<EntityType>>
                            lazyType, 
                            new Object[]{ 
                                Delegate.CreateDelegate(FuncType.MakeGenericType(EntityConfigType),
                                EntityConfigTypeMethod.MakeGenericMethod(EntityType))
                            }) 
                    };


            var rep = Activator.CreateInstance(
                TablePocoRepositoryType.MakeGenericType(EntityType), argumetns) as ITableRepository;


            return rep;
        }
        public static ITableRepository<TEntity> RepositoryFactory<TEntity>(ITableStorageContext ctx)
        {
            return RepositoryFactory(ctx, typeof(TEntity)) as ITableRepository<TEntity>;
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
        public int MaxDegreeOfParallelism { get; set; } = 10;
        public CloudStorageAccount StorageAccount {get; private set;}
        private static object _buildLock = new object();
       
        public InsertionMode InsertionMode { get; set; }

        private readonly Lazy<CloudTableClient> _client;
        private List<ITableRepository> repositories = new List<ITableRepository>();

        public bool AutoSaveOnDispose { get; set; }
        private static object _lock = new object();
        public TableStorageContext(CloudStorageAccount storage)
        {
            StorageAccount = storage;
            _client = new Lazy<CloudTableClient>(CreateClient);


            TableStorageModelBuilder builder =
                EntityTypeConfigurationsContainer.ModelBuilders.GetOrAdd(
                    this.GetType(), (key) => new Lazy<TableStorageModelBuilder>(() =>
                    {
                        var abuilder = new TableStorageModelBuilder();
                        abuilder.Builder = () =>
                        {

                            OnModelCreating(abuilder);
                            if (Table.inits.ContainsKey(this.GetType()))
                            {
                                var init = Table.inits[this.GetType()];
                                init.Initialize(this, abuilder);
                                Table.inits.Remove(this.GetType());
                            }
                        };
                        return abuilder;
                    }, LazyThreadSafetyMode.ExecutionAndPublication)).Value;
            //TableStorageModelBuilder builder;
            //if (EntityTypeConfigurationsContainer.ModelBuilders.ContainsKey(this.GetType()))
            //    EntityTypeConfigurationsContainer.ModelBuilders.TryGetValue(this.GetType(), out builder);
            //else
            //{
            //    lock(_lock)
            //    {
            //        //Check that if it was added
            //        if (!(EntityTypeConfigurationsContainer.ModelBuilders.ContainsKey(this.GetType()) 
            //            && EntityTypeConfigurationsContainer.ModelBuilders.TryGetValue(this.GetType(), out builder)))
            //        {
            //            builder = new TableStorageModelBuilder();

            //            OnModelCreating(builder, modelBuilderParams);
            //            EntityTypeConfigurationsContainer.ModelBuilders.TryAdd(this.GetType(), builder);
            //        }
            //    }
            //}


            //How long time does it take.
            using (new TraceTimer("Using reflection to set propeties.") { TraceLevel = System.Diagnostics.TraceLevel.Verbose, Threshold = 0 })
            {
                BuildModel(builder);

            }
          
          
           

            InsertionMode = InsertionMode.Add;
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
         
            foreach(var repository in this.GetType().GetProperties().Where(p=>p.PropertyType.IsGenericType))
            {
                if (typeof(ITableRepository<>).IsAssignableFrom(repository.PropertyType.GetGenericTypeDefinition()))
                {
                    var EntityType = repository.PropertyType.GenericTypeArguments[0];
                    var rep = Factory.RepositoryFactory(this, EntityType);
                  
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
            return _client.Value.GetTableReference(EntityTypeConfigurationsContainer.Entity<T1>().TableName);
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



        public Microsoft.WindowsAzure.Storage.RetryPolicies.IRetryPolicy RetryPolicy
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
