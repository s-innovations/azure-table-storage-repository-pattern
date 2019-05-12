using Microsoft.Extensions.Logging;
using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.Queryable;
using SInnovations.Azure.TableStorageRepository.Queryable.Expressions;
using SInnovations.Azure.TableStorageRepository.Queryable.Wrappers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.TableStorageRepository.TableRepositories
{

    public class ProjectedQuery<TProjected,TEntity> : IQueryable<TProjected>
    {

        public ProjectedQuery(TablePocoRepository<TEntity>  repository, ILoggerFactory logFactory, ITableStorageContext context, EntityTypeConfiguration<TEntity> configuration)
        {
           Provider = new TableQueryProvider<TEntity, TProjected>(logFactory, repository, configuration);
           Expression = Expression.Constant(this); 
        }
        public Type ElementType => typeof(TProjected);

        public Expression Expression { get;  }

        public IQueryProvider Provider { get; }

        public IEnumerator<TProjected> GetEnumerator()
        {
            return ((IEnumerable<TProjected>)Provider.Execute(Expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)Provider.Execute(Expression)).GetEnumerator();
        }
    }


    public class TablePocoRepository<TEntity> :
           TableRepository<EntityAdapter<TEntity>>,
           ITableRepository<TEntity>
    {


        public IQueryable<T> Project<T>()
        {
            return new ProjectedQuery<T, TEntity>(this,this.loggerFactory,this.Context,this.configuration.Value as EntityTypeConfiguration<TEntity>);
        }

    private readonly ILogger Logger;
        private readonly Expression _expression;
        public new EntityTypeConfiguration<TEntity> Configuration { get { return this.configuration.Value as EntityTypeConfiguration<TEntity>; } }
       
        public TablePocoRepository(ILoggerFactory logFactory, ITableStorageContext context, Lazy<EntityTypeConfiguration<TEntity>> configuration)
            : base(logFactory,context, new Lazy<EntityTypeConfiguration>(() => configuration.Value))
        {
            this.Logger = logFactory.CreateLogger<TablePocoRepository<TEntity>>();
            _expression = Expression.Constant(this);
            _provider = new Lazy<IQueryProvider>(() => new TableQueryProvider<TEntity>(logFactory,this, configuration.Value));
        }
        //public TableQuery<T> DynamicQuery<T>() where T : ITableEntity, new()
        //{
        //    return Table.que.CreateQuery<T>();            
        //}

        protected override EntityAdapter<TEntity> SetKeys(EntityAdapter<TEntity> entity, bool keysLocked)
        {
            if (entity == null)
                throw new ArgumentNullException("Entity is Null");

            if (keysLocked)
                return entity;

            if (this.configuration == null)
                throw new Exception("Configuration was not created");
            var mapper = Configuration.GetKeyMappers<TEntity>();
            if(mapper == null)
            {
                throw new Exception("Key Mapper was not created");
            }

            if (mapper.PartitionKeyMapper == null)
                throw new Exception("PartitionKeyMapper is Null");
            if (mapper.RowKeyMapper == null)
                throw new Exception("RowKeyMapper is Null");
            if (entity.InnerObject == null)
                throw new Exception("Inner Object is null");
            try
            {
                if (Configuration.ReversionTracking.Enabled)
                {
                    if(entity.ReversionBase != null)
                    {

                    }
                }

                entity.PartitionKey = mapper.PartitionKeyMapper(entity.InnerObject);
                entity.RowKey = mapper.RowKeyMapper(entity.InnerObject);

            }catch(Exception ex)
            {
                Logger.LogError(new EventId(),ex, entity.PartitionKey +" " + entity.RowKey);
                
                throw;
            }
            return entity;
        }

        /// <summary>
        ///     Executes a query on a table.
        /// </summary>
        /// <param name="tableQuery">
        ///     A <see cref="ITableQuery" /> representing the query to execute.
        /// </param>
        /// <returns>
        ///     An enumerable collection of <see cref="T:Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity" /> objects, representing table entities returned by the query.
        /// </returns>
        internal IEnumerable<T> ExecuteQuery<T>(ITableQuery tableQuery) where T : ITableEntity,new()
        {
            var query = new TableQuery<T>
            {
                FilterString = tableQuery.FilterString,
                SelectColumns = tableQuery.SelectColumns,
                TakeCount = tableQuery.TakeCount
            };

          
            return Table.ExecuteQueryAsync(query).GetAwaiter().GetResult();
        }

        /// <summary>
        ///     Executes a query on a table asynchronously.
        /// </summary>
        /// <param name="tableQuery">Table query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        ///     An enumerable collection of <see cref="T:Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity" /> objects, representing table entities returned by the query.
        /// </returns>
        internal Task<IEnumerable<T>> ExecuteQueryAsync<T>(ITableQuery tableQuery, CancellationToken cancellationToken) where T : ITableEntity,new()
        {
          
                var query = new TableQuery<T>
                {
                    FilterString = tableQuery.FilterString,
                    SelectColumns = tableQuery.SelectColumns,
                    TakeCount = tableQuery.TakeCount
                };
            try
            {
                return Table.ExecuteQueryAsync(query, cancellationToken);
            }catch(Exception ex)
            {
                Logger.LogWarning(ex,"Exception on {@Query}", query);
                throw;
            }
        }

        /// <summary>
        ///     Executes the operation on a table.
        /// </summary>
        /// <param name="operation">
        ///     A <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableOperation" /> object that represents the operation to perform.
        /// </param>
        /// <returns>
        ///     A <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableResult" /> containing the result of executing the operation on the table.
        /// </returns>
        public TableResult Execute(TableOperation operation)
        {
            return Table.ExecuteAsync(operation).GetAwaiter().GetResult();
        }

        /// <summary>
        ///     Executes table operation asynchronously.
        /// </summary>
        /// <param name="operation">
        ///     A <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableOperation" /> object that represents the operation to perform.
        /// </param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        ///     A <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableResult" /> containing the result of executing the operation on the table.
        /// </returns>
        public Task<TableResult> ExecuteAsync(TableOperation operation, CancellationToken cancellationToken)
        {
            return Table.ExecuteAsync(operation, null,null,cancellationToken);
        }

        /// <summary>
        ///     Executes a batch operation on a table as an atomic operation.
        /// </summary>
        /// <param name="batch">
        ///     The <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableBatchOperation" /> object representing the operations to execute on the table.
        /// </param>
        /// <returns>
        ///     An enumerable collection of <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableResult" /> objects that contains the results, in order, of each operation in the
        ///     <see
        ///         cref="T:Microsoft.WindowsAzure.Storage.Table.TableBatchOperation" />
        ///     on the table.
        /// </returns>
        public IList<TableResult> ExecuteBatch(TableBatchOperation batch)
        {
            return Table.ExecuteBatchAsync(batch).GetAwaiter().GetResult();
        }

        /// <summary>
        ///     Executes a batch of operations on a table asynchronously.
        /// </summary>
        /// <param name="tableBatchOperation">
        ///     The <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableBatchOperation" /> object representing the operations to execute on the table.
        /// </param>
        /// <param name="cancellationToken">Cancalltion token.</param>
        /// <returns>
        ///     An enumerable collection of <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableResult" /> objects that contains the results, in order, of each operation in the
        ///     <see cref="T:Microsoft.WindowsAzure.Storage.Table.TableBatchOperation" />
        ///     on the table.
        /// </returns>
        public Task<IList<TableResult>> ExecuteBatchAsync(TableBatchOperation tableBatchOperation, CancellationToken cancellationToken)
        {
            return Table.ExecuteBatchAsync(tableBatchOperation,null,null, cancellationToken);
        }


   //     public override TableQuery<EntityAdapter<TEntity>> Source { get { throw new NotSupportedException(); } }

        public void Add(TEntity entity)
        {
            base.Add(new EntityAdapter<TEntity>(Context,Configuration, entity));
        }

        public void AddRevision(TEntity entity, EntityAdapter<TEntity>.OnEntityChanged onEntityChanged)
        {
            if (!Configuration.ReversionTracking.Enabled)
            {
                throw new Exception("RevisionTracking not enabled");
            }

            base.Add(new EntityAdapter<TEntity>(Context, Configuration, entity, onEntityChanged));
        }

        public void Add(TEntity entity, string partitionKey, string rowKey)
        {
            base.Add(new EntityAdapter<TEntity>(Context, Configuration, entity), partitionKey, rowKey);
        }
        public void Add(TEntity entity, IDictionary<string,EntityProperty> additionalProperties){
            base.Add(new EntityAdapter<TEntity>(Context, Configuration, entity) { Properties = additionalProperties });
        }

        public void Delete(TEntity entity)
        {
            Tuple<DateTimeOffset, string> _state;
            if (Configuration.EntityStates.TryGetValue(entity.GetHashCode(), out _state))
                base.Delete(new EntityAdapter<TEntity>(Context, Configuration, entity, _state.Item1, _state.Item2));
            else
                base.Delete(new EntityAdapter<TEntity>(Context, Configuration, entity, null, "*"));

        }
        public void Update(TEntity entity, IDictionary<string, EntityProperty> additionalProperties)
        {
            Tuple<DateTimeOffset, string> _state;
            if (Configuration.EntityStates.TryGetValue(entity.GetHashCode(), out _state))
                base.Update(new EntityAdapter<TEntity>(Context, Configuration, entity, _state.Item1, _state.Item2) { Properties = additionalProperties });
            else
                base.Update(new EntityAdapter<TEntity>(Context, Configuration, entity, null, "*") { Properties = additionalProperties });
        }
        public void Update(TEntity entity)
        {
            Tuple<DateTimeOffset, string> _state;
            if (Configuration.EntityStates.TryGetValue(entity.GetHashCode(), out _state))
                base.Update(new EntityAdapter<TEntity>(Context, Configuration, entity, _state.Item1, _state.Item2));
            else
                base.Update(new EntityAdapter<TEntity>(Context, Configuration, entity, null, "*"));

        }

        public  IEnumerable<TEntity> FluentQuery(string filter)
        {
            throw new NotImplementedException();
            //  return base.FluentQuery(filter).Select(o => o.InnerObject);
        }


        public new async Task<TEntity> FindByIndexAsync(params Object[] keys)
        {
            var result =  (await base.FindByIndexAsync(keys));
            if(result==null)
                return default(TEntity);

            await PostReadEntityAsync(result,null).ConfigureAwait(false);
      
            return result.InnerObject;
        }
        

        public new async Task<TEntity> FindByKeysAsync(string partitionKey, string rowKey)
        {
            var result = (await base.FindByKeysAsync(partitionKey, rowKey));
            if (result == null)
                return default(TEntity);


            await PostReadEntityAsync(result,null).ConfigureAwait(false);

            return SetCollections(result).InnerObject;
        }
        public async Task<IDictionary<string,EntityProperty>> FindPropertiesByKeysAsync(string partitionKey, string rowKey)
        {
            var result = (await base.FindByKeysAsync(partitionKey, rowKey));
            if (result == null)
                return new Dictionary<string, EntityProperty>();

            return result.Properties;
        }

        public async Task<IDictionary<string,EntityProperty>> FindPropertiesByKeysAsync(string partitionKey, string rowKey, params string[] properties)
        {
            var tableQuery = new TableQuery<EntityAdapter<TEntity>>();
            tableQuery.SelectColumns = new List<string>(properties);
            tableQuery.FilterString = TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.Equal, partitionKey);
            tableQuery.FilterString = TableQuery.CombineFilters(tableQuery.FilterString, TableOperators.And,
            TableQuery.GenerateFilterCondition("RowKey", QueryComparisons.Equal, rowKey));

            var entity = await Table.ExecuteQueryAsync(tableQuery);
            if (!entity.Any())
                return new Dictionary<string,EntityProperty>();

            return entity.First().Properties;

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


        public void Clear()
        {
            throw new NotImplementedException();
        }

        public int Count
        {
            get { throw new NotImplementedException(); }
        }

        public bool IsReadOnly
        {
            get { throw new NotImplementedException(); }
        }

        public Expression Expression
        {
            get
            {
                if (BaseQuery != null)
                    return BaseQuery.Expression;
                return _expression;
            }
        }

        Type IQueryable.ElementType
        {
            get { return typeof(TEntity); }
        }

        private Lazy<IQueryProvider> _provider;
        public IQueryProvider Provider {
            get
            {
                if (BaseQuery != null)
                    return BaseQuery.Provider;
                return _provider.Value;
            }
        }

        public IEnumerator<TEntity> GetEnumerator()
        {
            return ((IEnumerable<TEntity>)Provider.Execute(Expression)).GetEnumerator();
        }

        IEnumerator IEnumerable.GetEnumerator()
        {
            return ((IEnumerable)Provider.Execute(Expression)).GetEnumerator();
        }

        public IQueryable<TEntity> BaseQuery { get; set; }






        public new async Task<Tuple<IEnumerable<TEntity>, TableContinuationToken>> ExecuteQuerySegmentedAsync(ITableQuery query, TableContinuationToken currentToken, CancellationToken cancellationToken = default(CancellationToken))
        {
            var result = await base.ExecuteQuerySegmentedAsync(query, currentToken)
                 .Then(async p => new Tuple<IEnumerable<TEntity>, TableContinuationToken>( await GetProcessedResultAsync(p.Item1, new TranslationResult(),null).ConfigureAwait(false), p.Item2), cancellationToken);
            return result;
          //  return new Tuple<IEnumerable<TEntity>, TableContinuationToken>(result.Item1.Select(e => e), result.Item2);

             
        }




        private async Task<TEntity> KeepState(EntityAdapter<TEntity> entity, IOverrides overrides)
        {
            //foreach (var entity in enumerable)
            //  {
            //TODO: key should properly be row+part key
            //   _entityConfiguration.EntityStates.AddOrUpdate(entity.InnerObject.GetHashCode(),
            //        (key) => new Tuple<DateTimeOffset, string>(entity.Timestamp, entity.ETag), (key,v) => new Tuple<DateTimeOffset, string>(entity.Timestamp, entity.ETag));

            //Set properties that infact are the part/row key
            //   _entityConfiguration.ReverseKeyMapping<TEntity>(entity);
            //    }
            //        return enumerable;
         //   await entity.PostReadEntityAsync(Configuration);
            await PostReadEntityAsync(entity,overrides);

            return entity.InnerObject;
        }

        /// <summary>
        ///     Executes post processing of retrieved entities.
        /// </summary>
        /// <param name="tableEntities">Table entities.</param>
        /// <param name="translation">translation result.</param>
        /// <returns>Collection of entities.</returns>
        internal async Task<IEnumerable<TEntity>> GetProcessedResultAsync(IEnumerable<EntityAdapter<TEntity>> tableEntities, TranslationResult translation, IOverrides overrides)
        {
            if (!tableEntities.Any())
                return Enumerable.Empty<TEntity>();
            var buffer = new BufferBlock<TEntity>();
            var block = new TransformBlock<EntityAdapter<TEntity>, TEntity>(async (adapter) =>
            {
                var a = await KeepState(adapter,overrides).ConfigureAwait(false);

                return a;

            });
            block.LinkTo(buffer, new DataflowLinkOptions { PropagateCompletion = true });
            foreach (var adapter in tableEntities) { await block.SendAsync(adapter); }


            block.Complete();
            await block.Completion;


            buffer.TryReceiveAll(out IList<TEntity> result);

            //IEnumerable<TEntity> result = tableEntities.Select(q => KeepState(q));

            if (translation.PostProcessing == null)
            {
                return result;
            }

            try
            {
                return translation.PostProcessing.DynamicInvoke(result.AsQueryable()) as IEnumerable<TEntity>;
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException;
            }
        }

        public override async Task<EntityAdapter<TEntity>> PostReadEntityAsync(EntityAdapter<TEntity> entity, IOverrides overrides)
        {
            if (entity != null)
            {
                await entity.PostReadEntityAsync(Configuration,overrides);
            }
            return entity;
        }
    }
}
