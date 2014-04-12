using Microsoft.WindowsAzure.Storage.Table;
using System;
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
    public interface ITableRepository
    {
        Task SaveChangesAsync();
    }
    public interface ITableRepository<TEntity> : ITableRepository, 
        ICollection<TEntity>,
        IQueryable<TEntity>
    {
        //void Add(TEntity entity);
        void Delete(TEntity entity);
        void Update(TEntity entity);

        
        TableQuery<TEntity> Source { get; }
        IEnumerable<TEntity> FluentQuery(string filter);

        Task<TEntity> FindByIndexAsync(params object[] keys);
        Task<TEntity> FindByKeysAsync(string partitionKey, string rowKey);
    }

    public class TablePocoRepository<TEntity> : 
        TableEntityRepository<EntityAdapter<TEntity>>, 
        ITableRepository<TEntity> where TEntity : new()
    {

        private readonly EntityTypeConfiguration configuration;
        public TablePocoRepository(ITableStorageContext context, EntityTypeConfiguration configuration)
            : base(context, configuration)
        {
            this.configuration = configuration;
        }
        protected override EntityAdapter<TEntity> SetKeys(EntityAdapter<TEntity> entity)
        {
            var mapper = this.configuration.GetKeyMappers<TEntity>();
            entity.PartitionKey = mapper.PartitionKeyMapper(entity.InnerObject);
            entity.RowKey = mapper.RowKeyMapper(entity.InnerObject);
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

        public new IEnumerable<TEntity> FluentQuery(string filter)
        {
            return base.FluentQuery(filter).Select(o => o.InnerObject);
        }


        public new async Task<TEntity> FindByIndexAsync(params Object[] keys)
        {
            var result =  (await base.FindByIndexAsync(keys));
            if(result==null)
                return default(TEntity);
            return result.InnerObject;
        }






        public new async Task<TEntity> FindByKeysAsync(string partitionKey, string rowKey)
        {

            var result = (await base.FindByKeysAsync(partitionKey, rowKey));
            if(result==null)
                return default(TEntity);
            return SetCollections<TEntity>(result.InnerObject);
        }


        public bool Contains(TEntity item)
        {
            return ((ICollection<EntityAdapter<TEntity>>)this).Any(t => t.InnerObject.Equals(item));
        }

        public void CopyTo(TEntity[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public bool Remove(TEntity item)
        {
            throw new NotImplementedException();
        }

        public new IEnumerator<TEntity> GetEnumerator()
        {
            throw new NotImplementedException();
        }

        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            throw new NotImplementedException();
        }
    }


    public abstract class TableRepository<TEntity>
    {
        internal IQueryable<TEntity> parentQuery { get; set; }

        public abstract TableQuery<TEntity> Source { get; }
    }
    public class TableEntityRepository<TEntity> : 
        TableRepository<TEntity>,
        ICollection<TEntity>, 
        IQueryable<TEntity>,
        ITableRepository<TEntity> 
        where TEntity : ITableEntity, new()
    {
        private readonly ITableStorageContext _context;
        private List<EntityStateWrapper<TEntity>> _cache = new List<EntityStateWrapper<TEntity>>();

        private static MethodInfo QueryFilterMethod = typeof(CollectionConfiguration).GetMethod("GetFilterQuery");

        private readonly CloudTable _table;
        //   private KeysMapper<TEntity>? keys;
        //   private Dictionary<string, CloudTable> indexTables;
        private readonly EntityTypeConfiguration configuration;
        public TableEntityRepository(ITableStorageContext context, EntityTypeConfiguration configuration)
        {
            this._context = context;
            this.configuration = configuration;
            this._table = context.GetTable(configuration.TableName);
            //    this.keys = keys;
        }


        public virtual async Task<TEntity> FindByIndexAsync(params object[] keys)
        {


            foreach (var index in configuration.Indexes.Values)
            {
                var table = _context.GetTable(index.TableName ?? configuration.TableName + "Index");

                var opr = TableOperation.Retrieve<IndexEntity>(string.Join("__", keys.Select(k => k.ToString())), "");
                var result = await table.ExecuteAsync(opr);
                if (result.Result != null)
                {
                    var idxEntity = result.Result as IndexEntity;
                    var entity = await this._table.ExecuteAsync(TableOperation.Retrieve<TEntity>(idxEntity.RefPartitionKey, idxEntity.RefRowKey));
                    return SetCollections((TEntity)entity.Result);
                }

            }
            return default(TEntity);
        }
        public virtual async Task<TEntity> FindByKeysAsync(string partitionkey, string rowkey)
        {
            var result = await _table.ExecuteAsync(TableOperation.Retrieve<TEntity>(partitionkey, rowkey));
            return SetCollections( (TEntity)result.Result);
        }
        protected T SetCollections<T>(T entity)
        {
            if (entity == null)
                return entity;

            foreach(var collectionInfo in configuration.Collections)
            {


                var repository = collectionInfo.Activator(_context);

                QueryFilterMethod.MakeGenericMethod(collectionInfo.ParentEntityType, collectionInfo.EntityType)
                    .Invoke(collectionInfo, new object[] { repository, entity });

                collectionInfo.PropertyInfo.SetValue(entity, repository);
            }
            return entity;
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
        public override TableQuery<TEntity> Source
        {
            get { return _table.CreateQuery<TEntity>(); }
        }

        public IEnumerable<TEntity> FluentQuery(string filter)
        {
            return _table.ExecuteQuery(new TableQuery<TEntity>().Where(filter));
        }




        protected virtual TEntity SetKeys(TEntity entity)
        {
            var mapper = this.configuration.GetKeyMappers<TEntity>();
            entity.PartitionKey = mapper.PartitionKeyMapper(entity);
            entity.RowKey = mapper.RowKeyMapper(entity);
            return entity;
        }
        public async Task SaveChangesAsync()
        {
            if (!_cache.Any())
                return;
            await _table.CreateIfNotExistsAsync();

            var indexes = new ConcurrentBag<EntityStateWrapper<IndexEntity>>();

            var actionblock = new ActionBlock<EntityStateWrapper<TEntity>[]>(async (batch) =>
            {
                var batchOpr = new TableBatchOperation();
                foreach (var item in batch)
                {
                    switch (item.State)
                    {
                        case EntityState.Added:

                            foreach (var index in configuration.Indexes.Values)
                            {
                                var indexkey = index.GetIndexKey(item.Entity);
                                if(indexkey==null)
                                    continue;
                                indexes.Add(new EntityStateWrapper<IndexEntity>
                                {
                                    State = EntityState.Added,
                                    Entity =
                                        new IndexEntity
                                        {
                                            Config = index,
                                            PartitionKey =indexkey,
                                            RowKey = "",
                                            RefRowKey = item.Entity.RowKey,
                                            RefPartitionKey = item.Entity.PartitionKey,
                                        }
                                });
                            }
                            batchOpr.Add(GetInsertionOperation(item.Entity));
                            break;
                        case EntityState.Updated:
                            foreach (var collection in configuration.Collections)
                            {
                               var rep= collection.PropertyInfo.GetValue(item.Entity) as ITableRepository;
                               await rep.SaveChangesAsync();
                            }


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

            using (new TraceTimer("Handling Entities"))
            {
                var batches = _cache.GroupBy(b => SetKeys(b.Entity).PartitionKey);
                foreach (var group in batches)
                    foreach (var batch in group
                          .Select((x, i) => new { Group = x, Index = i })
                          .GroupBy(x => x.Index / 100)
                          .Select(x => x.Select(v => v.Group).ToArray())
                          .Select(t => t.ToArray()))
                    {



                        actionblock.Post(batch);
                    }
                actionblock.Complete();
                await actionblock.Completion;
            }
            var block = new ActionBlock<Tuple<CloudTable, EntityStateWrapper<IndexEntity>>>(async (item) =>
            {
                switch (item.Item2.State)
                {
                    case EntityState.Added:
                        await item.Item1.ExecuteAsync(GetInsertionOperation(item.Item2.Entity));
                        break;
                    case EntityState.Updated:
                        await item.Item1.ExecuteAsync(TableOperation.Merge(item.Item2.Entity));
                        break;
                    case EntityState.Deleted:
                        await item.Item1.ExecuteAsync(TableOperation.Delete(item.Item2.Entity));
                        break;
                    default:
                        break;
                }

            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExecutionDataflowBlockOptions.Unbounded });

            using (new TraceTimer("Handling Indexes"))
            {
                foreach (var indexkey in indexes.GroupBy(idx => idx.Entity.Config.TableName ?? configuration.TableName + "Index"))
                {
                    var table = _context.GetTable(indexkey.Key);
                    foreach (var item in indexkey)
                        block.Post(new Tuple<CloudTable, EntityStateWrapper<IndexEntity>>(table, item));

                }
                block.Complete();
                await block.Completion;
            }
            _cache = new List<EntityStateWrapper<TEntity>>();

        }

        private TableOperation GetInsertionOperation<Entity>(Entity item) where Entity : ITableEntity
        {
            TableOperation opr;
            switch (this._context.InsertionMode)
            {
                case InsertionMode.Add:
                    opr = TableOperation.Insert(item);
                    break;
                case InsertionMode.AddOrMerge:
                    opr = TableOperation.InsertOrMerge(item);
                    break;
                case InsertionMode.AddOrReplace:
                    opr = TableOperation.InsertOrReplace(item);
                    break;
                default:
                    throw new NotSupportedException();

            }
            return opr;
        }


        public void Clear()
        {
            _cache.Clear();
        }

        public bool Contains(TEntity item)
        {
            return _cache.Any(t => t.Entity.Equals(item));
        }

        public void CopyTo(TEntity[] array, int arrayIndex)
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { return _cache.Count; }
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
            if (parentQuery != null)
                return parentQuery.GetEnumerator();
            return Source.GetEnumerator();
            //var query = from ent in Source
            //       where ent.PartitionKey == "Sorensen"
            //       select ent;
            //return (query).GetEnumerator();
        }
        
        System.Collections.IEnumerator System.Collections.IEnumerable.GetEnumerator()
        {
            return GetEnumerator();
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
                return Source.Expression;
            }
        }

        public IQueryProvider Provider
        {
            get
            {
                if (parentQuery != null)
                    return parentQuery.Provider;
                return Source.Provider;
            }
        }
    }

}
