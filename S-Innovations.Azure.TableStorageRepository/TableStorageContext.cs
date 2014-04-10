using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository
{
   
    public abstract class TableStorageContext : ITableStorageContext
    {
        private readonly CloudTableClient _client;
        private List<ITableRepository> repositories = new List<ITableRepository>();
        public TableStorageContext(CloudStorageAccount storage)
        {

            _client = storage.CreateCloudTableClient();
            TableStorageModelBuilder modelbuilder = new TableStorageModelBuilder();
            OnModelCreating(modelbuilder);
            BuildModel(modelbuilder);
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

            foreach(var repository in type.GetProperties())
            {
                if (typeof(ITableRepository<>).IsAssignableFrom(repository.PropertyType.GetGenericTypeDefinition()))
                {
                    var EntityType = repository.PropertyType.GenericTypeArguments[0];
                    var IsEntityType= (typeof(ITableEntity).IsAssignableFrom(EntityType));
                    var TableRepositoryType = IsEntityType?TableEntityRepositoryType:TablePocoRepositoryType;
                    var Table = _client.GetTableReference(modelbuilder._configurations[EntityType].TableName);
                    var argumetns = new Object[] { Table, modelbuilder._configurations[EntityType].KeyMapper};

                        var rep = Activator.CreateInstance(
                            TableRepositoryType.MakeGenericType(EntityType), argumetns) as ITableRepository;
                        repositories.Add(rep);
                        repository.SetValue(this, rep);
                    

                }
            }
        }


    }
}
