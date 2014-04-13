using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.Queryable.Base;
using SInnovations.Azure.TableStorageRepository.Queryable.Expressions;
using SInnovations.Azure.TableStorageRepository.Queryable.Wrappers;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable
{
    /// <summary>
    ///     LINQ to Windows Azure Storage Table query provider.
    ///     http://msdn.microsoft.com/en-us/library/windowsazure/dd894031.aspx
    /// </summary>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    internal class TableQueryProvider<TEntity> : QueryProviderBase, IAsyncQueryProvider where TEntity : new()
    {
        private readonly TablePocoRepository<TEntity> _repository;
        private readonly EntityTypeConfiguration<TEntity> _entityConfiguration;
        private readonly QueryTranslator _queryTranslator;

        /// <summary>
        ///     Constructor.
        /// </summary>
        /// <param name="cloudTable">Cloud table.</param>
        /// <param name="entityConverter"></param>
        internal TableQueryProvider(TablePocoRepository<TEntity> cloudTable, EntityTypeConfiguration<TEntity> entityConverter)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException("cloudTable");
            }

            if (entityConverter == null)
            {
                throw new ArgumentNullException("entityConverter");
            }

            _repository = cloudTable;
            _entityConfiguration = entityConverter;
            _queryTranslator = new QueryTranslator(entityConverter.NamePairs);
        }

        /// <summary>
        ///     Executes expression query.
        /// </summary>
        /// <param name="expression">Expression.</param>
        /// <returns>Result.</returns>
        public override object Execute(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }

            var result = new TranslationResult();

            _queryTranslator.Translate(expression, result);

            IEnumerable<EntityAdapter<TEntity>> tableEntities = _repository.ExecuteQuery<EntityAdapter<TEntity>>(result.TableQuery);

            return GetProcessedResult(tableEntities, result);
        }

        /// <summary>
        ///     Executes expression query asynchronously.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public Task<object> ExecuteAsync(
            Expression expression,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }

            var result = new TranslationResult();

            _queryTranslator.Translate(expression, result);

            return _repository
                .ExecuteQueryAsync<EntityAdapter<TEntity>>(result.TableQuery, cancellationToken)
                .Then(p => GetProcessedResult(p, result), cancellationToken);
        }

        /// <summary>
        ///     Executes post processing of retrieved entities.
        /// </summary>
        /// <param name="tableEntities">Table entities.</param>
        /// <param name="translation">translation result.</param>
        /// <returns>Collection of entities.</returns>
        private object GetProcessedResult(IEnumerable<EntityAdapter<TEntity>> tableEntities, TranslationResult translation)
        {
            IEnumerable<TEntity> result = tableEntities.Select(q => KeepState(q));

            if (translation.PostProcessing == null)
            {
                return result;
            }

            try
            {
                return translation.PostProcessing.DynamicInvoke(result.AsQueryable());
            }
            catch (TargetInvocationException e)
            {
                throw e.InnerException;
            }
        }

        private TEntity KeepState(EntityAdapter<TEntity> entity)
        {
            //foreach (var entity in enumerable)
          //  {
                //TODO: key should properly be row+part key
                _entityConfiguration.EntityStates.AddOrUpdate(entity.InnerObject.GetHashCode(),
                    (key) => new Tuple<DateTimeOffset, string>(entity.Timestamp, entity.ETag), (key,v) => new Tuple<DateTimeOffset, string>(entity.Timestamp, entity.ETag));
                _entityConfiguration.ReverseKeyMapping<TEntity>(entity);
        //    }
        //        return enumerable;
                return entity.InnerObject;
        }
    }
}
