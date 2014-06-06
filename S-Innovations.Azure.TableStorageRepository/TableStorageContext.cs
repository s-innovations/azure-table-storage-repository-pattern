using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository
{
    internal static class Factory
    {
        public static ITableRepository<TEntity> RepositoryFactory<TEntity>(ITableStorageContext ctx, EntityTypeConfiguration config)
        {
            var TableEntityRepositoryType = typeof(TableEntityRepository<>);
            var TablePocoRepositoryType = typeof(TablePocoRepository<>);

            var EntityType = typeof(TEntity);
            var IsEntityType = (typeof(ITableEntity).IsAssignableFrom(EntityType));
            var TableRepositoryType = IsEntityType ? TableEntityRepositoryType : TablePocoRepositoryType;

            var argumetns = new Object[] { ctx, config };

            var rep = Activator.CreateInstance(
                        TableRepositoryType.MakeGenericType(EntityType), argumetns) as ITableRepository<TEntity>;

            //if(parent!=null)
            //    (( TableRepository<TEntity>)rep).parentQuery = from ent in parent.Source
            //                                                   where ent.

            return rep;
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
        private readonly CloudStorageAccount _storage;
        private static object _buildLock = new object();
       
        public InsertionMode InsertionMode { get; set; }

        private readonly Lazy<CloudTableClient> _client;
        private List<ITableRepository> repositories = new List<ITableRepository>();

        public bool AutoSaveOnDispose { get; set; }
        private static object _lock = new object();
        public TableStorageContext(CloudStorageAccount storage)
        {
            _storage = storage;
            _client = new Lazy<CloudTableClient>(CreateClient);


            //TableStorageModelBuilder builder =
            //    EntityTypeConfigurationsContainer.ModelBuilders.GetOrAdd(
            //        this.GetType(), (key) => new Lazy<TableStorageModelBuilder>(() =>
            //        {
            //                var abuilder = new TableStorageModelBuilder();
            //                OnModelCreating(abuilder);
            //                return abuilder;
            //        }));
            TableStorageModelBuilder builder;
            if (EntityTypeConfigurationsContainer.ModelBuilders.ContainsKey(this.GetType()))
                EntityTypeConfigurationsContainer.ModelBuilders.TryGetValue(this.GetType(), out builder);
            else
            {
                lock(_lock)
                {
                    //Check that if it was added
                    if (!(EntityTypeConfigurationsContainer.ModelBuilders.ContainsKey(this.GetType()) 
                        && EntityTypeConfigurationsContainer.ModelBuilders.TryGetValue(this.GetType(), out builder)))
                    {
                        builder = new TableStorageModelBuilder();

                        OnModelCreating(builder);
                        EntityTypeConfigurationsContainer.ModelBuilders.TryAdd(this.GetType(), builder);
                    }
                }
            }


            BuildModel(builder);

          
            if(Table.inits.ContainsKey(this.GetType()))
            {
                var init = Table.inits[this.GetType()];
                init.Initialize(this, builder);
                Table.inits.Remove(this.GetType());
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
        

        public TableEntityRepository<TEntity> TableEntityRepository<TEntity>() where TEntity : ITableEntity,new()
        {
            throw new NotImplementedException();
        }

        public Task SaveChangesAsync()
        {
           // foreach (var rep in repositories)
           // {
           //     await rep.SaveChangesAsync();
           // }
           return Task.WhenAll(repositories.Select(rep => rep.SaveChangesAsync()));
        }


        protected virtual void OnModelCreating(TableStorageModelBuilder modelbuilder)
        {

        }

        private void BuildModel(TableStorageModelBuilder modelbuilder)
        {
            //TODO CACHE THIS

            var type = this.GetType();
            var TableEntityRepositoryType = typeof(TableEntityRepository<>);
            var TablePocoRepositoryType = typeof(TablePocoRepository<>);

            foreach(var repository in type.GetProperties().Where(p=>p.PropertyType.IsGenericType))
            {
                if (typeof(ITableRepository<>).IsAssignableFrom(repository.PropertyType.GetGenericTypeDefinition()))
                {
                    var EntityType = repository.PropertyType.GenericTypeArguments[0];
                    var IsEntityType= (typeof(ITableEntity).IsAssignableFrom(EntityType));
                    var TableRepositoryType = IsEntityType?TableEntityRepositoryType:TablePocoRepositoryType;

                    var argumetns = new Object[] { this, EntityTypeConfigurationsContainer.Configurations[EntityType] };

                    var rep = Activator.CreateInstance(
                        TableRepositoryType.MakeGenericType(EntityType), argumetns) as ITableRepository;
                    repositories.Add(rep);
                    repository.SetValue(this, rep);
                    

                }
            }
        }



        public CloudTable GetTable(string name)
        {
            var tbl= _client.Value.GetTableReference(name);
            tbl.CreateIfNotExists();
            return tbl;
        }

        public CloudTable GetTable<T1>()
        {
            return _client.Value.GetTableReference(EntityTypeConfigurationsContainer.Entity<T1>().TableName);
        }


        private CloudTableClient CreateClient()
        {
           var client= _storage.CreateCloudTableClient();

           
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
