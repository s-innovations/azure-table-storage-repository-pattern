using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.Queryable;

using SInnovations.Azure.TableStorageRepository.Queryable.Wrappers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.TableRepositories
{



    public class TablePocoRepository<TEntity> :
           TableRepository<EntityAdapter<TEntity>>,
           ITableRepository<TEntity>
    {
        private readonly Expression _expression;
        private new readonly EntityTypeConfiguration<TEntity> configuration;
       
        public TablePocoRepository(ITableStorageContext context, EntityTypeConfiguration<TEntity> configuration)
            : base(context, configuration)
        {
            this.configuration = configuration;
            _expression = Expression.Constant(this);
            //var t = new CloudTableWrapper(table);
            _provider = new TableQueryProvider<TEntity>(this, configuration);
        }

        public TableQuery<T> DynamicQuery<T>() where T : ITableEntity, new()
        {
            return table.CreateQuery<T>();            
        }

        //protected override EntityAdapter<TEntity> SetKeys(EntityAdapter<TEntity> entity)
        protected override EntityAdapter<TEntity> SetKeys(EntityAdapter<TEntity> entity, bool keysLocked)
        {
            if (keysLocked)
                return entity;

            var mapper = this.configuration.GetKeyMappers<TEntity>();
            entity.PartitionKey = mapper.PartitionKeyMapper(entity.InnerObject);
            entity.RowKey = mapper.RowKeyMapper(entity.InnerObject);
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

          
            return table.ExecuteQuery(query);
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

            return table.ExecuteQueryAsync(query, cancellationToken);
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
            return table.Execute(operation);
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
            return table.ExecuteAsync(operation, cancellationToken);
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
            return table.ExecuteBatch(batch);
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
            return table.ExecuteBatchAsync(tableBatchOperation, cancellationToken);
        }


   //     public override TableQuery<EntityAdapter<TEntity>> Source { get { throw new NotSupportedException(); } }

        public void Add(TEntity entity)
        {
            base.Add(new EntityAdapter<TEntity>(configuration,entity));
        }
        public void Add(TEntity entity, string partitionKey, string rowKey)
        {
            base.Add(new EntityAdapter<TEntity>(configuration, entity),partitionKey,rowKey);
        }
        public void Add(TEntity entity, IDictionary<string,EntityProperty> additionalProperties){
            base.Add(new EntityAdapter<TEntity>(configuration, entity) { Properties = additionalProperties });
        }

        public void Delete(TEntity entity)
        {
            Tuple<DateTimeOffset, string> _state;
            if (configuration.EntityStates.TryGetValue(entity.GetHashCode(), out _state))
                base.Delete(new EntityAdapter<TEntity>(configuration,entity, _state.Item1, _state.Item2));
            else
                base.Delete(new EntityAdapter<TEntity>(configuration,entity, null, "*"));

        }
        public void Update(TEntity entity)
        {
            Tuple<DateTimeOffset, string> _state;
            if (configuration.EntityStates.TryGetValue(entity.GetHashCode(), out _state))
                base.Update(new EntityAdapter<TEntity>(configuration,entity, _state.Item1, _state.Item2));
            else
                base.Update(new EntityAdapter<TEntity>(configuration,entity, null, "*"));

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
            return result.InnerObject;
        }






        public new async Task<TEntity> FindByKeysAsync(string partitionKey, string rowKey)
        {
            var result = (await base.FindByKeysAsync(partitionKey, rowKey));
            if (result == null)
                return default(TEntity);
            return SetCollections(result).InnerObject;
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

        private IQueryProvider _provider;
        public IQueryProvider Provider {
            get
            {
                if (BaseQuery != null)
                    return BaseQuery.Provider;
                return _provider;
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






        public new async Task<Tuple<IEnumerable<TEntity>, TableContinuationToken>> ExecuteQuerySegmentedAsync(ITableQuery query, TableContinuationToken currentToken)
        {
            var result = await base.ExecuteQuerySegmentedAsync(query, currentToken);
            return new Tuple<IEnumerable<TEntity>, TableContinuationToken>(result.Item1.Select(e => e.InnerObject), result.Item2);

             
        }
    }
}
