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
using Microsoft.WindowsAzure.Storage;
using System.Linq.Expressions;
using SInnovations.Azure.TableStorageRepository.Queryable;
using Microsoft.Extensions.Logging;
using SInnovations.Azure.TableStorageRepository.Queryable.Expressions;
using System.Threading;

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



    public abstract class TableRepository<TEntity> where TEntity : class, ITableEntity, new()
    {
        protected readonly ILoggerFactory loggerFactory;
        private readonly ILogger Logger;
        private ConcurrentBag<EntityStateWrapper<TEntity>> _cache = new ConcurrentBag<EntityStateWrapper<TEntity>>();



        public CloudTable Table { get { return table.Value; } }
        protected Lazy<CloudTable> table;

        protected EntityTypeConfiguration Configuration { get { return configuration.Value; } }

        protected readonly Lazy<EntityTypeConfiguration> configuration;
        public ITableStorageContext Context { get; private set; }

        internal TableRepository(ILoggerFactory logFactory, ITableStorageContext context, Lazy<EntityTypeConfiguration> configuration)
        {
            this.Logger = logFactory.CreateLogger<TableRepository<TEntity>>();
            this.Context = context;
            this.configuration = configuration;
            this.table = new Lazy<CloudTable>(() => context.GetTable(configuration.Value.TableName(context)));
            this.loggerFactory = logFactory;
        }


        //   public abstract TableQuery<TEntity> Source { get; }



        public virtual void Add(TEntity entity)
        {
            if (Configuration.TraceOnAdd) {
                Logger.LogTrace($"Adding entity<{entity.GetHashCode()} to TableRepository");
            }

            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Added, Entity = entity });
        }
        public virtual void Add(TEntity entity, string partitionkey, string rowkey)
        {
            if (Configuration.TraceOnAdd)
            {
                Logger.LogTrace($"Adding entity<{entity.GetHashCode()} to TableRepository using partitionkey:{partitionkey},rowkey:{rowkey}");
                    }

            entity.PartitionKey = partitionkey;
            entity.RowKey = rowkey;
            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Added, Entity = entity, KeysLocked = true });
        }

        public void Delete(TEntity entity)
        {
            if (Configuration.TraceOnAdd) { Logger.LogTrace($"Deleting entity<{entity.GetHashCode()} to TableRepository"); }

            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Deleted, Entity = entity });

        }
        public void Update(TEntity entity)
        {
            if (Configuration.TraceOnAdd) { Logger.LogTrace($"Updating entity<{entity.GetHashCode()} to TableRepository"); }

            this._cache.Add(new EntityStateWrapper<TEntity>() { State = EntityState.Updated, Entity = entity });

        }


        public virtual async Task<TEntity> FindByKeysAsync(string partitionKey, string rowKey)
        {
            if (Configuration.TraceOnAdd) { Logger.LogTrace($"Finding entity by partitionkey:{partitionKey} and rowkey:{rowKey}"); }
            var op = TableOperation.Retrieve<TEntity>(partitionKey, rowKey);
            var result = await Table.ExecuteAsync(op);
            return SetCollections((TEntity)result.Result);
        }
        public async Task DeleteByKey(string partitionKey, string rowKey)
        {
            if (Configuration.TraceOnAdd) { Logger.LogTrace($"Deleting entity by partitionkey:{partitionKey} and rowkey:{rowKey}"); }

            var a= await Table.ExecuteAsync(TableOperation.Delete(new DynamicTableEntity(partitionKey, rowKey) { ETag = "*" }));
            var b = a.Result as TableEntity;

        }
        public virtual async Task<TEntity> FindByIndexAsync(params object[] keys)
        {
            if (Configuration.TraceOnAdd) { Logger.LogTrace($"Finding by index keys {string.Join(",", keys)}"); }

            foreach (var index in Configuration.Indexes.Values.GroupBy(idx => idx.TableName?.Invoke(this.Context) ?? Configuration.TableName(this.Context) + idx.TableNamePostFix))
            {
                var table = Context.GetTable(index.Key);

                //TODO Optimize by executing all indexes at the same time and return the first found result.
                foreach (var idxConfig in index)
                {

                    if (idxConfig.CopyAllProperties)
                    {
                        var opr = TableOperation.Retrieve<TEntity>(idxConfig.GetIndexKeyFunc(keys), "");
                        var entity = await table.ExecuteAsync(opr);
                        if (entity.Result != null)
                        {
                            return (TEntity)entity.Result;
                        }
                    }
                    else
                    {

                        var opr = TableOperation.Retrieve<IndexEntity>(idxConfig.GetIndexKeyFunc(keys), "");
                        var result = await table.ExecuteAsync(opr);
                        if (result.Result != null)
                        {
                            var idxEntity = result.Result as IndexEntity;


                            var entity = await Table.ExecuteAsync(TableOperation.Retrieve<TEntity>(idxEntity.RefPartitionKey, idxEntity.RefRowKey));
                            return SetCollections((TEntity)entity.Result);
                        }
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
            if (adapter != null)
                obj = adapter.GetInnerObject();

            foreach (var collectionInfo in Configuration.Collections)
            {
                collectionInfo.SetCollection(obj, Context);
            }
            return entity;
        }

        public static IPropagatorBlock<EntityStateWrapper<TEntity>, EntityStateWrapper<TEntity>[]>
        CreateGroupingBlock()
        {
            var dictionary = new Dictionary<string, List<EntityStateWrapper<TEntity>>>();
            var buffer = new BufferBlock<EntityStateWrapper<TEntity>[]>();

            var actionBlock = new ActionBlock<EntityStateWrapper<TEntity>>(async (state) =>
            {
                if (!dictionary.ContainsKey(state.Entity.PartitionKey))
                {
                    dictionary[state.Entity.PartitionKey] = new List<EntityStateWrapper<TEntity>>();
                }

                dictionary[state.Entity.PartitionKey].Add(state);

                if (dictionary[state.Entity.PartitionKey].Count >= 100)
                {
                    await buffer.SendAsync(dictionary[state.Entity.PartitionKey].ToArray());
                    dictionary[state.Entity.PartitionKey].Clear();
                }

            });
            actionBlock.Completion.ContinueWith((task) =>
            {
                foreach (var key in dictionary.Keys.ToArray())
                {
                    buffer.Post(dictionary[key].ToArray());
                    dictionary.Remove(key);
                }

                if (task.IsFaulted) ((IDataflowBlock)buffer).Fault(task.Exception);
                else buffer.Complete(); 
            });
            var prop = DataflowBlock.Encapsulate(actionBlock, buffer);
            //prop.Completion.ContinueWith(task =>
            //{
            //    foreach (var key in dictionary.Keys.ToArray())
            //    {
            //        buffer.Post(dictionary[key].ToArray());
            //        dictionary.Remove(key);
            //    }
            //    buffer.Complete();
            //});
            return prop;






        }

        public async Task SaveChangesAsync()
        {
            if (Configuration.TraceOnAdd) { Logger.LogTrace($"SaveChangesAsync Running"); }

            if (!_cache.Any())
            {
                if (Configuration.TraceOnAdd) { Logger.LogTrace($"SaveChangesAsync had empty cache"); }
                return;
            }

            try
            {
                var indexes = new ConcurrentBag<EntityStateWrapper<IndexEntity>>();

                var actionblock = new ActionBlock<EntityStateWrapper<TEntity>[]>(async (batch) =>
                {
                    if (Configuration.TraceOnAdd) { Logger.LogTrace($"Executing ActionBlock of batch size {batch.Length}"); }

                    var batchOpr = new TableBatchOperation();
                    foreach (var item in batch)
                    {
                       


                        switch (item.State)
                        {

                            case EntityState.Added:

                                foreach (var index in Configuration.Indexes.Values)
                                {
                                    try
                                    {
                                        var indexkeys = index.GetIndexKey(item.Entity);
                                        if (indexkeys == null)
                                            continue;

                                        foreach (var indexkey in indexkeys)
                                        {

                                            indexes.Add(new EntityStateWrapper<IndexEntity>
                                            {
                                                State = EntityState.Added,
                                                Entity =
                                                    new IndexEntity
                                                    {
                                                        Config = index,
                                                        PartitionKey = indexkey,
                                                        RowKey = index.GetIndexSecondKey(item.Entity),
                                                        RefRowKey = item.Entity.RowKey,
                                                        RefPartitionKey = item.Entity.PartitionKey,
                                                        Ref = item.Entity
                                                    }
                                            });

                                        }
                                    }
                                    catch (Exception ex)
                                    {

                                    }
                                }
                                batchOpr.Add(GetInsertionOperation(item.Entity));
                                break;
                            case EntityState.Updated:
                                foreach (var collection in Configuration.Collections)
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

                    using (new TraceTimer(Logger, string.Format("Executing operation {0} to {1}", batchOpr.Count, Table.Name)))
                    {
                        try
                        {
                        //table.ExecuteBatch(batchOpr);
                        if (batchOpr.Count == 1)
                                await Table.ExecuteAsync(batchOpr.First());
                            else if (batchOpr.Count > 0)
                                await Table.ExecuteBatchAsync(batchOpr);
                        }
                        catch (StorageException ex)
                        {
                            Logger.LogError(new EventId(), ex, "Storage Error execution on {tableName}, {ex}", Table.Name, ex);
                            throw;

                        }
                        catch (Exception ex)
                        {
                            Logger.LogError(new EventId(), ex, "Error execution on {tableName}, {ex}", Table.Name, ex);
                            throw;
                        }
                    }

                }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Context.MaxDegreeOfParallelism });

                using (new TraceTimer(Logger, "Handling Entities"))
                {
                    var buffer = new BufferBlock<EntityStateWrapper<TEntity>>();
                    var next = buffer;

                    var reversionOptions = Configuration.ReversionTracking;
                    if (reversionOptions.Enabled)
                    {
                        //    var store = this as ITableRepository<TEntity>;

                        next = new BufferBlock<EntityStateWrapper<TEntity>>();

                        var trackReversionBlock = new TransformManyBlock<EntityStateWrapper<TEntity>, EntityStateWrapper<TEntity>>(async state =>
                        {
                            var entity = state.Entity;




                            var oldQuery = reversionOptions.HeadWhereFilter.DynamicInvoke(this, (entity as IEntityAdapter)?.GetInnerObject() ?? entity) as ITableQuery;

                            var old = await this.Table.ExecuteQueryAsync(new TableQuery<TEntity> { FilterString = oldQuery.FilterString, TakeCount = 1 });

                            if (old.Any())
                            {

                            }

                            if (entity is IEntityAdapter)
                            {
                                var adapter = entity as IEntityAdapter;
                                entity = await adapter.MakeReversionCloneAsync(await PostReadEntityAsync(  old.FirstOrDefault(),null));


                            }


                            if (entity != null)
                            {
                                return new[] { state, new EntityStateWrapper<TEntity> { State = state.State, Entity = entity } };
                            }
                            return new[] { state };

                        }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Context.MaxDegreeOfParallelism });

                        buffer.LinkTo(trackReversionBlock, new DataflowLinkOptions { PropagateCompletion = true });
                        trackReversionBlock.LinkTo(next, new DataflowLinkOptions { PropagateCompletion = true });
                    }

                    var grouper = CreateGroupingBlock();
                    next.LinkTo(grouper, new DataflowLinkOptions { PropagateCompletion = true });
                    grouper.LinkTo(actionblock, new DataflowLinkOptions { PropagateCompletion = true });


                    foreach (var entityState in _cache)
                    {
                        var entity = SetKeys(entityState.Entity, entityState.KeysLocked);

                        buffer.Post(entityState);
                    }

                    buffer.Complete();
                    await actionblock.Completion;
                    //  var a = new System.Threading.Tasks.Dataflow.BatchBlock(100,new GroupingDataflowBlockOptions {})



                    //var batches = _cache.GroupBy(b => SetKeys(b.Entity, b.KeysLocked).PartitionKey);
                    //foreach (var group in batches)
                    //    foreach (var batch in group
                    //          .Select((x, i) => new { Group = x, Index = i })
                    //          .GroupBy(x => x.Index / 100)
                    //          .Select(x => x.Select(v => v.Group).ToArray())
                    //          .Select(t => t.ToArray()))
                    //    {
                    //        Logger.LogTrace($"Posting Batch of lenght: {batch.Length}");
                    //        await actionblock.SendAsync(batch);
                    //    }

                    //  actionblock.Complete();
                    //  await actionblock.Completion;
                }

                var block = new ActionBlock<Tuple<CloudTable, EntityStateWrapper<IndexEntity>[]>>(async (tuple) =>
                {
                    var batch = tuple.Item2;
                    var table = tuple.Item1;
                    if (Configuration.TraceOnAdd) { Logger.LogTrace($"Executing Index ActionBlock of batch size {batch.Length}"); }

                    var batchOpr = new TableBatchOperation();
                    foreach (var item in batch)
                    {

                        switch (item.State)
                        {
                            case EntityState.Added:
                                batchOpr.Add(GetInsertionOperation(item.Entity));
                            //await item.Item1.ExecuteAsync(GetInsertionOperation(item.Item2.Entity));
                            break;
                            case EntityState.Updated:
                                batchOpr.Add(TableOperation.Merge(item.Entity));
                            //await item.Item1.ExecuteAsync(TableOperation.Merge(item.Item2.Entity));
                            break;
                            case EntityState.Deleted:
                                batchOpr.Add(TableOperation.Delete(item.Entity));
                            //await item.Item1.ExecuteAsync(TableOperation.Delete(item.Item2.Entity));
                            break;
                            default:
                                break;
                        }
                    }

                    using (new TraceTimer(Logger, string.Format("Executing operation {0} to {1}", batchOpr.Count, table.Name)))
                    {
                        try
                        {
                        //table.ExecuteBatch(batchOpr);
                        if (batchOpr.Count == 1)
                                await table.ExecuteAsync(batchOpr.First());
                            else
                                await table.ExecuteBatchAsync(batchOpr);
                        }
                        catch (Exception ex)
                        {
                            Logger.LogError(new EventId(), ex, "Error execution {ex}", ex);
                            throw;
                        }
                    }

                }, new ExecutionDataflowBlockOptions { MaxDegreeOfParallelism = Context.MaxDegreeOfParallelism });

                using (new TraceTimer(Logger, "Handling Indexes"))
                {
                    foreach (var indexkey in indexes.GroupBy(idx => idx.Entity.Config.TableName?.Invoke(this.Context) ?? Configuration.TableName(this.Context) + idx.Entity.Config.TableNamePostFix))
                    {
                        var indexTable = Context.GetTable(indexkey.Key);
                        var batches = indexkey.GroupBy(k => k.Entity.PartitionKey);
                        foreach (var group in batches)
                            foreach (var batch in group
                              .Select((x, i) => new { Group = x, Index = i })
                              .GroupBy(x => x.Index / 100)
                              .Select(x => x.Select(v => v.Group).ToArray())
                              .Select(t => t.ToArray()))
                            {
                                Logger.LogTrace($"Posting Batch of lenght: {batch.Length}");
                                await block.SendAsync(new Tuple<CloudTable, EntityStateWrapper<IndexEntity>[]>(indexTable, batch));
                            }


                    }
                    block.Complete();
                    await block.Completion;
                }
            }
            finally
            {
                _cache = new ConcurrentBag<EntityStateWrapper<TEntity>>();
            }
           

        }

        public abstract Task<TEntity> PostReadEntityAsync(TEntity entity, IOverrides overrides);

        private object GetEntity(object entity)
        {
            var ent = entity as IEntityAdapter;
            if (ent != null)
                return ent.GetInnerObject();
            return ent;
        }

        protected virtual TEntity SetKeys(TEntity entity, bool keysLocked)
        {
            if (keysLocked)
                return entity;

            var mapper = Configuration.GetKeyMappers<TEntity>();
            if (mapper == null)
                throw new Exception("Mapper was not configured");

            entity.PartitionKey = mapper.PartitionKeyMapper(entity);
            entity.RowKey = mapper.RowKeyMapper(entity);
            if (Configuration.TraceOnAdd) { Logger.LogTrace($"Setting Keys for Entity<{entity.GetHashCode()}>[{entity.PartitionKey}|{entity.RowKey}]"); }
            return entity;
        }

        private TableOperation GetInsertionOperation<Entity>(Entity item) where Entity : ITableEntity
        {
            if (item.PartitionKey == null)
            {
                Logger.LogCritical(string.Join("\n", item.WriteEntity(null).Select(d => string.Format("{0} {1} {2}", d.Key, d.Value.PropertyType, d.Value.PropertyAsObject))));
                throw new NullReferenceException("The partionKey was not set");
            }



            TableOperation opr;
            switch (this.Context.InsertionMode)
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

      

        public async Task<Tuple<IEnumerable<TEntity>, TableContinuationToken>> ExecuteQuerySegmentedAsync(ITableQuery query, TableContinuationToken continueTOken, CancellationToken cancellationToken = default(CancellationToken))
        {
            var q = new TableQuery<TEntity>
            {
                FilterString = query.FilterString,
                SelectColumns = query.SelectColumns,
                TakeCount = query.TakeCount
            };
            var result = await Table.ExecuteQuerySegmentedAsync(q, continueTOken);
                

            return new Tuple<IEnumerable<TEntity>, TableContinuationToken>(result.Results, result.ContinuationToken);

        }


    }
}
