using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.Queryable.Wrappers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.TableStorageRepository.TableRepositories
{
    internal static class StringExtensions
    {
        public static string Base64Encode(this string plainText)
        {
            var plainTextBytes = Encoding.UTF8.GetBytes(plainText);
            return Convert.ToBase64String(plainTextBytes);
        }

        public static string Base64Decode(this string base64EncodedData)
        {
            var base64EncodedBytes = Convert.FromBase64String(base64EncodedData);
            return Encoding.UTF8.GetString(base64EncodedBytes);
        }
    }

    public abstract class TableRepository<TEntity> where TEntity : ITableEntity,new()
    {
        private List<EntityStateWrapper<TEntity>> _cache = new List<EntityStateWrapper<TEntity>>();
        protected readonly CloudTable table;
        protected readonly EntityTypeConfiguration configuration;
        protected readonly ITableStorageContext context;

        internal TableRepository(ITableStorageContext context, EntityTypeConfiguration configuration)
        {
            this.context = context;
            this.configuration = configuration;
            this.table = context.GetTable(configuration.TableName);
        }

        
     //   public abstract TableQuery<TEntity> Source { get; }



        public virtual void Add(TEntity entity)
        {
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Added, Entity = entity });
        }
        public virtual void Add(TEntity entity,string partitionkey, string rowkey)
        {
            entity.PartitionKey = partitionkey;
            entity.RowKey = rowkey;
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Added, Entity = entity, KeysLocked=true });
        }

        public void Delete(TEntity entity)
        {
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Deleted, Entity = entity });

        }
        public void Update(TEntity entity)
        {
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Updated, Entity = entity });

        }


        public virtual async Task<TEntity> FindByKeysAsync(string partitionkey, string rowkey)
        {
            var result = await table.ExecuteAsync(TableOperation.Retrieve<TEntity>(partitionkey, rowkey));
            return SetCollections((TEntity)result.Result);
        }
        public Task DeleteByKey(string partitionKey, string rowKey)
        {
            return table.ExecuteAsync(TableOperation.Delete(new TableEntity(partitionKey, rowKey) { ETag = "*" }));
        }
        public virtual async Task<TEntity> FindByIndexAsync(params object[] keys)
        {
            foreach (var index in configuration.Indexes.Values.GroupBy(idx => idx.TableName ?? configuration.TableName + "Index"))
            {
                var table = context.GetTable(index.Key);

                //TODO Optimize by executing all indexes at the same time and return the first found result.
                foreach (var idxConfig in index)
                {
                    var opr = TableOperation.Retrieve<IndexEntity>(idxConfig.GetIndexKeyFunc(keys), "");
                    var result = await table.ExecuteAsync(opr);
                    if (result.Result != null)
                    {
                        var idxEntity = result.Result as IndexEntity;
                        var entity = await this.table.ExecuteAsync(TableOperation.Retrieve<TEntity>(idxEntity.RefPartitionKey, idxEntity.RefRowKey));
                        return SetCollections((TEntity)entity.Result);
                    }
                }

            }
            return default(TEntity);
        }

       
        protected TEntity SetCollections(TEntity entity)
        {
            //if (typeof(T) != typeof(TEntity))
            //    return entity;

            if (entity == null)
                return entity;

            IEntityAdapter adapter = entity as IEntityAdapter;
            object obj = entity;
            if (adapter!=null)
               obj = adapter.GetInnerObject();

            foreach (var collectionInfo in configuration.Collections)
            {
                collectionInfo.SetCollection(obj,context);
            }
            return entity;
        }

        public async Task SaveChangesAsync()
        {
            if (!_cache.Any())
                return;
          //  await table.CreateIfNotExistsAsync();

            var indexes = new ConcurrentBag<EntityStateWrapper<IndexEntity>>();

            var actionblock = new ActionBlock<EntityStateWrapper<TEntity>[]>(async (batch) =>
            {
                var batchOpr = new TableBatchOperation();
                foreach (var item in batch)
                {
                    Trace.WriteLine(string.Format("Batch<{0}> item<{3}>: {1} {2}", 
                        batch.GetHashCode(),item.Entity.PartitionKey,item.Entity.RowKey,item.State));
                    
                    switch (item.State)
                    {
                        case EntityState.Added:

                            foreach (var index in configuration.Indexes.Values)
                            {
                                var indexkey = index.GetIndexKey(item.Entity);
                                if (indexkey == null)
                                    continue;
                                indexes.Add(new EntityStateWrapper<IndexEntity>
                                {
                                    State = EntityState.Added,
                                    Entity =
                                        new IndexEntity
                                        {
                                            Config = index,
                                            PartitionKey = indexkey,
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
                                var rep = collection.PropertyInfo.GetValue(GetEntity(item.Entity)) as ITableRepository;
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
                using (new TraceTimer(string.Format("Executing Batch length {0} to {1}", batchOpr.Count,table.Name)))
                {

                    //table.ExecuteBatch(batchOpr);
                    if (batchOpr.Count == 1)
                        await table.ExecuteAsync(batchOpr.First());
                    else
                        await table.ExecuteBatchAsync(batchOpr);
                }

            }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = ExecutionDataflowBlockOptions.Unbounded });

            using (new TraceTimer("Handling Entities"))
            {
                var batches = _cache.GroupBy(b => SetKeys(b.Entity,b.KeysLocked).PartitionKey);
                foreach (var group in batches)
                    foreach (var batch in group
                          .Select((x, i) => new { Group = x, Index = i })
                          .GroupBy(x => x.Index / 100)
                          .Select(x => x.Select(v => v.Group).ToArray())
                          .Select(t => t.ToArray()))
                    {
                        Trace.WriteLine(string.Format("Posting Batch of lenght: {0}", batch.Length));
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
                    var indexTable = context.GetTable(indexkey.Key);
                    foreach (var item in indexkey)
                        block.Post(new Tuple<CloudTable, EntityStateWrapper<IndexEntity>>(indexTable, item));

                }
                block.Complete();
                await block.Completion;
            }
            _cache = new List<EntityStateWrapper<TEntity>>();

        }

        private object GetEntity(object entity)
        {
            var ent = entity as IEntityAdapter;
            if (ent != null)
                return ent.GetInnerObject();
            return ent;
        }

        protected virtual TEntity SetKeys(TEntity entity,bool keysLocked)
        {
            if (keysLocked)
                return entity;

            var mapper = this.configuration.GetKeyMappers<TEntity>();
            entity.PartitionKey = mapper.PartitionKeyMapper(entity);
            entity.RowKey = mapper.RowKeyMapper(entity);
            return entity;
        }

        private TableOperation GetInsertionOperation<Entity>(Entity item) where Entity : ITableEntity
        {
            Trace.TraceInformation(string.Join(", ", item.WriteEntity(null).Select(d => string.Format("{0} {1}", d.Key, d.Value.PropertyType))));

            TableOperation opr;
            switch (this.context.InsertionMode)
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


        public async Task<Tuple<IEnumerable<TEntity>,TableContinuationToken>> ExecuteQuerySegmentedAsync(ITableQuery query, TableContinuationToken currentToken)
        {
            var q = new TableQuery<TEntity>{
                 FilterString = query.FilterString,
                 SelectColumns = query.SelectColumns,
                  TakeCount = query.TakeCount
            };
            var result = await table.ExecuteQuerySegmentedAsync<TEntity>(q, currentToken);
            return new Tuple<IEnumerable<TEntity>, TableContinuationToken>(result.Results, result.ContinuationToken);

        }
       
    }
}
