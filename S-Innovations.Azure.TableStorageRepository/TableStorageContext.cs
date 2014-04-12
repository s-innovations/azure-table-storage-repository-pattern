using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
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
        private readonly TableStorageModelBuilder modelbuilder;
        public InsertionMode InsertionMode { get; set; }

        private readonly CloudTableClient _client;
        private List<ITableRepository> repositories = new List<ITableRepository>();
        public TableStorageContext(CloudStorageAccount storage)
        {

            _client = storage.CreateCloudTableClient();
            modelbuilder = new TableStorageModelBuilder();
            OnModelCreating(modelbuilder);
            BuildModel(modelbuilder);


            InsertionMode = InsertionMode.Add;
        }
        public TableEntityRepository<TEntity> TableEntityRepository<TEntity>() where TEntity : ITableEntity,new()
        {
            throw new NotImplementedException();
        }

        public Task SaveChangesAsync()
        {
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
                 
                    var argumetns = new Object[] {this, modelbuilder._configurations[EntityType]};

                    var rep = Activator.CreateInstance(
                        TableRepositoryType.MakeGenericType(EntityType), argumetns) as ITableRepository;
                    repositories.Add(rep);
                    repository.SetValue(this, rep);
                    

                }
            }
        }



        public CloudTable GetTable(string name)
        {
            var tbl= _client.GetTableReference(name);
            tbl.CreateIfNotExists();
            return tbl;
        }

        public CloudTable GetTable<T1>()
        {
            return _client.GetTableReference(modelbuilder._configurations[typeof(T1)].TableName);
        }
    }
}
