using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.TableStorageRepository
{
    public interface ITableStorageContext
    {
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

    public struct EntityStateWrapper<T>  where T: ITableEntity 
    {
        public EntityState State { get; set; }
        public T Entity { get; set; }
    }
    public interface ITableRepository
    {
        Task SaveChangesAsync();
    }
    public interface ITableRepository<TEntity> : ITableRepository
    {
        void Add(TEntity entity);
        void Delete(TEntity entity);
        void Update(TEntity entity);
         
        TableQuery<TEntity> Source{get;}       
        IEnumerable<TEntity> FluentQuery(string filter);
    }

    public class TablePocoRepository<TEntity> : TableEntityRepository<EntityAdapter<TEntity>>, ITableRepository<TEntity> where TEntity : new()
    {
        private KeysMapper<TEntity> keys;

        public TablePocoRepository(CloudTable table, KeysMapper<TEntity> keys)
            : base(table)
        {
            this.keys = keys;
        }
        protected override EntityAdapter<TEntity> SetKeys(EntityAdapter<TEntity> entity)
        {
      
            entity.PartitionKey = this.keys.PartitionKeyMapper(entity.InnerObject);
            entity.RowKey = this.keys.RowKeyMapper(entity.InnerObject);
            return entity;
        
        }

        public new TableQuery<TEntity> Source { get { throw new NotSupportedException(); } } 

        public void Add(TEntity entity)
        {
            base.Add(new EntityAdapter<TEntity>(entity));
        }
        public void Delete(TEntity entity)
        {
            base.Delete(new EntityAdapter<TEntity>(entity));

        }
        public void Update(TEntity entity)
        {
             base.Update(new EntityAdapter<TEntity>(entity));

        }
        
        public IEnumerable<TEntity> FluentQuery(string filter)
        {
            return base.FluentQuery(filter).Select(o=>o.InnerObject);
        }
    }


    public class TableEntityRepository<TEntity> : ITableRepository<TEntity> where TEntity : ITableEntity,new()
    {
        private ConcurrentBag<EntityStateWrapper<TEntity>> _cache = new ConcurrentBag<EntityStateWrapper<TEntity>>();
        private readonly CloudTable _table;
        private KeysMapper<TEntity>? keys;
        public TableEntityRepository(CloudTable table, KeysMapper<TEntity>? keys = null)
        {
            this._table = table;
            this.keys = keys;
        }

        public virtual void Add(TEntity entity)
        {
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Added, Entity = entity });
        }
        public void Delete(TEntity entity)
        {
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Deleted, Entity = entity });

        }
        public void Update(TEntity entity)
        {
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Updated, Entity = entity });

        }
        public virtual TableQuery<TEntity> Source
        {
            get { return _table.CreateQuery<TEntity>(); }
        }

        public IEnumerable<TEntity> FluentQuery(string filter)
        {
            return _table.ExecuteQuery(new TableQuery<TEntity>().Where(filter));
        }




        protected virtual TEntity SetKeys(TEntity entity)
        {
            if (keys.HasValue)
            {
                entity.PartitionKey = keys.Value.PartitionKeyMapper(entity);
                entity.RowKey = keys.Value.RowKeyMapper(entity);
            }

             return entity;
        }
        public async Task SaveChangesAsync()
        {
            if (!_cache.Any())
                return;
            await _table.CreateIfNotExistsAsync();


            var actionblock = new ActionBlock<EntityStateWrapper<TEntity>[]>(async (batch) =>
            {
                var batchOpr = new TableBatchOperation();
                foreach (var item in batch)
                {
                    switch (item.State)
                    {
                        case EntityState.Added:
                            batchOpr.Add(TableOperation.Insert(item.Entity));
                            break;
                        case EntityState.Updated:
                            batchOpr.Add(TableOperation.Merge(item.Entity));
                            break;
                        case EntityState.Deleted:
                            batchOpr.Add(TableOperation.Delete(item.Entity));
                            break;
                        default:
                            break;
                    }

                }
                await _table.ExecuteBatchAsync(batchOpr);

            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExecutionDataflowBlockOptions.Unbounded });


            var batches = _cache.GroupBy(b => SetKeys(b.Entity).PartitionKey);
            foreach(var group in batches)
            foreach (var batch in group
                  .Select((x, i) => new { Group = x, Index = i })
                  .GroupBy(x => x.Index / 100)
                  .Select(x=>x.Select(v=>v.Group).ToArray())
                  .Select(t=>t.ToArray()))                  
            {
                actionblock.Post(batch);
            }
            actionblock.Complete();
            await actionblock.Completion;
            _cache = new ConcurrentBag<EntityStateWrapper<TEntity>>();

        }
    }

}
