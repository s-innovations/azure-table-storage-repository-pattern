using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.TableStorageRepository.TableRepositories
{


    public abstract class TableRepository<TEntity> where TEntity : ITableEntity
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

        private static MethodInfo QueryFilterMethod = typeof(CollectionConfiguration).GetMethod("GetFilterQuery");
        protected T SetCollections<T>(T entity)
        {
            if (typeof(T) != typeof(TEntity))
                return entity;

            if (entity == null)
                return entity;

            foreach (var collectionInfo in configuration.Collections)
            {


                var repository = collectionInfo.Activator(context);

                QueryFilterMethod.MakeGenericMethod(collectionInfo.ParentEntityType, collectionInfo.EntityType)
                    .Invoke(collectionInfo, new object[] { repository, entity });

                collectionInfo.PropertyInfo.SetValue(entity, repository);
            }
            return entity;
        }

        public async Task SaveChangesAsync()
        {
            if (!_cache.Any())
                return;
            await table.CreateIfNotExistsAsync();

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
                                var rep = collection.PropertyInfo.GetValue(item.Entity) as ITableRepository;
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
                await table.ExecuteBatchAsync(batchOpr);

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
                    var indexTable = context.GetTable(indexkey.Key);
                    foreach (var item in indexkey)
                        block.Post(new Tuple<CloudTable, EntityStateWrapper<IndexEntity>>(indexTable, item));

                }
                block.Complete();
                await block.Completion;
            }
            _cache = new List<EntityStateWrapper<TEntity>>();

        }
        protected virtual TEntity SetKeys(TEntity entity)
        {
            var mapper = this.configuration.GetKeyMappers<TEntity>();
            entity.PartitionKey = mapper.PartitionKeyMapper(entity);
            entity.RowKey = mapper.RowKeyMapper(entity);
            return entity;
        }

        private TableOperation GetInsertionOperation<Entity>(Entity item) where Entity : ITableEntity
        {
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

    }
}
