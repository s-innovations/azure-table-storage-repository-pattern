using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.TableStorageRepository
{
    public interface ITableStorageContext: IDisposable
    {
        InsertionMode InsertionMode { get; set; }
        CloudTable GetTable(string name);
    }
    public enum EntityState
    {
        Added,
        Updated,
        Deleted,
        Unmodified,
    }
    public static class ITableEntityExtensions
    {
        public static EntityState GetState(this ITableEntity entity)
        {
            if (string.IsNullOrEmpty(entity.ETag))
                return EntityState.Added;
            else
                return EntityState.Updated;
        }
    }

    public struct EntityStateWrapper<T> where T : ITableEntity
    {
        public EntityState State { get; set; }
        public T Entity { get; set; }
    }

    
    public class TableEntityRepository<TEntity> : 
        TableRepository<TEntity>,
        ITableRepository<TEntity> 
        where TEntity : ITableEntity, new()
    {
      

        private static MethodInfo QueryFilterMethod = typeof(CollectionConfiguration).GetMethod("GetFilterQuery");

        public IQueryable<TEntity> parentQuery { get; set; }


        public TableEntityRepository(ITableStorageContext context, EntityTypeConfiguration configuration) : base(context,configuration)
        {
        

        }


        public virtual async Task<TEntity> FindByIndexAsync(params object[] keys)
        {


            foreach (var index in configuration.Indexes.Values)
            {
                var table = context.GetTable(index.TableName ?? configuration.TableName + "Index");

                var opr = TableOperation.Retrieve<IndexEntity>(string.Join("__", keys.Select(k => k.ToString())), "");
                var result = await table.ExecuteAsync(opr);
                if (result.Result != null)
                {
                    var idxEntity = result.Result as IndexEntity;
                    var entity = await this.table.ExecuteAsync(TableOperation.Retrieve<TEntity>(idxEntity.RefPartitionKey, idxEntity.RefRowKey));
                    return SetCollections((TEntity)entity.Result);
                }

            }
            return default(TEntity);
        }
        public virtual async Task<TEntity> FindByKeysAsync(string partitionkey, string rowkey)
        {
            var result = await table.ExecuteAsync(TableOperation.Retrieve<TEntity>(partitionkey, rowkey));
            return SetCollections( (TEntity)result.Result);
        }
        protected T SetCollections<T>(T entity)
        {
            if (entity == null)
                return entity;

            foreach(var collectionInfo in configuration.Collections)
            {


                var repository = collectionInfo.Activator(context);

                QueryFilterMethod.MakeGenericMethod(collectionInfo.ParentEntityType, collectionInfo.EntityType)
                    .Invoke(collectionInfo, new object[] { repository, entity });

                collectionInfo.PropertyInfo.SetValue(entity, repository);
            }
            return entity;
        }


        //public override TableQuery<TEntity> Source
        //{
        //    get { return table.CreateQuery<TEntity>(); }
        //}

        public IEnumerable<TEntity> FluentQuery(string filter)
        {
            return table.ExecuteQuery(new TableQuery<TEntity>().Where(filter));
        }








        public void Clear()
        {
         //   _cache.Clear();
        }

        public bool Contains(TEntity item)
        {
            return false;// _cache.Any(t => t.Entity.Equals(item));
        }

        public void CopyTo(TEntity[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get 
            {
                return 0;// _cache.Count; 
            }
        }

        public bool IsReadOnly
        {
            get { return true; }
        }

        public bool Remove(TEntity item)
        {
            this.Delete(item);
            return true;
        }
        public IEnumerator<TEntity> GetEnumerator()
        {
            return ((IEnumerable<TEntity>)Provider.Execute(Expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)Provider.Execute(Expression)).GetEnumerator();
        }

        public Type ElementType
        {
            get { return typeof(TEntity); }
        }

        public System.Linq.Expressions.Expression Expression
        {
            get {
                if (parentQuery != null)
                    return parentQuery.Expression;
                return table.CreateQuery<TEntity>().Expression;
            }
        }

        public IQueryProvider Provider
        {
            get
            {
                if (parentQuery != null)
                    return parentQuery.Provider;
                return table.CreateQuery<TEntity>().Provider;
            }
        }
    }

}
