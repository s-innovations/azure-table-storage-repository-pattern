using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Extensions.Logging;
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
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.TableStorageRepository.Queryable
{

    public static class QueryableExtensions
    {
        public static ITableQuery AsTableQuery<T>(this IQueryable<T> query)
        {
            try
            {
                var provider = query.Provider as TableQueryProvider<T>;
                if (provider.GetType() == typeof(TableQueryProvider<T>))
                {
                    var result = ((TableQueryProvider<T>)provider).GetTranslationResult(query.Expression);

                    return result.TableQuery;


                }
            }
            catch (Exception ex)
            {

                throw;
            }
            throw new Exception("not supported");
        }
    }


    /// <summary>
    ///     LINQ to Windows Azure Storage Table query provider.
    ///     http://msdn.microsoft.com/en-us/library/windowsazure/dd894031.aspx
    /// </summary>
    /// <typeparam name="TEntity">Entity type.</typeparam>
    internal class TableQueryProvider<TEntity> : QueryProviderBase, IAsyncQueryProvider
    {
        /// <summary>
        /// The logger
        /// </summary>
        private readonly ILogger Logger;


        private readonly TablePocoRepository<TEntity> _repository;
        private readonly EntityTypeConfiguration<TEntity> _entityConfiguration;
        private readonly QueryTranslator _queryTranslator;
        private readonly List<string> _prefixes = new List<string>();
        /// <summary>
        ///     Constructor.
        /// </summary>
        /// <param name="cloudTable">Cloud table.</param>
        /// <param name="entityConverter"></param>
        internal TableQueryProvider(ILoggerFactory logfactory, TablePocoRepository<TEntity> cloudTable, EntityTypeConfiguration<TEntity> entityConverter)
        {
            if (cloudTable == null)
            {
                throw new ArgumentNullException("cloudTable");
            }

            if (entityConverter == null)
            {
                throw new ArgumentNullException("entityConverter");
            }

            Logger = logfactory.CreateLogger<TableQueryProvider<TEntity>>();

            _repository = cloudTable;
            _entityConfiguration = entityConverter;
            _queryTranslator = new QueryTranslator(logfactory, entityConverter);
        }

        public void AddPrefix(string prefix)
        {
            _prefixes.Add(prefix);
        }

        /// <summary>
        ///     Executes expression query.
        /// </summary>
        /// <param name="expression">Expression.</param>
        /// <returns>Result.</returns>
        public override object Execute(Expression expression)
        {
            //var result = GetTranslationResult(expression);

            //            IEnumerable<EntityAdapter<TEntity>> tableEntities = _repository.ExecuteQuery<EntityAdapter<TEntity>>(result.TableQuery);

            //          return _repository.GetProcessedResultAsync(tableEntities, result).GetAwaiter().GetResult();

            return ExecuteAsync(expression).GetAwaiter().GetResult();

        }
        internal TranslationResult GetTranslationResult(Expression expression)
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }

            var result = new TranslationResult();

            _queryTranslator.Translate(expression, result);

            if (result.TableQuery.FilterString == "()")
                result.TableQuery.FilterString = null;

            AddCollectionPropertiesFilters(result);

            foreach (var prefix in _prefixes)
            {
                var length = prefix.Length - 1;
                var nextChar = prefix[length] + 1;
                var startWithEnd = prefix.Substring(0, length) + (char)nextChar;

                var filter = TableQuery.CombineFilters(
                   TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.GreaterThanOrEqual, prefix),
                   TableOperators.And,
                   TableQuery.GenerateFilterCondition("PartitionKey", QueryComparisons.LessThan, startWithEnd));


                if (string.IsNullOrEmpty(result.TableQuery.FilterString))
                {
                    result.TableQuery.FilterString = filter;
                }
                else
                {
                    result.TableQuery.FilterString = TableQuery.CombineFilters(
                      filter,
                       TableOperators.And,
                        result.TableQuery.FilterString);
                }
            }

            return result;
        }

        internal void AddCollectionPropertiesFilters(TranslationResult result)
        {
            //TODO Really want to wrap nested data collections into lazy properties on the model itself.
            if (result.TableQuery.SelectColumns != null)
            {
                foreach (var colInfo in _entityConfiguration.Collections)
                {
                    //Only set it if the activator is null, otherwise its set by a query lookup.
                    if (colInfo.Activator == null)
                        result.AddColumn(colInfo.PropertyInfo.Name);
                }
            }
        }






        /// <summary>
        ///     Executes expression query asynchronously.
        /// </summary>
        /// <param name="expression"></param>
        /// <param name="cancellationToken"></param>
        /// <returns></returns>
        public async Task<object> ExecuteAsync(
            Expression expression,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            if (expression == null)
            {
                throw new ArgumentNullException("expression");
            }
            Logger.LogDebug("Translating Async Expression");

            //var result = new TranslationResult();

            //_queryTranslator.Translate(expression, result);

            //  AddCollectionPropertiesFilters(result);
            var result = GetTranslationResult(expression);
            Logger.LogDebug("Executing Async Expression : {0}, {1}, {2}",
                result.TableQuery.FilterString, result.TableQuery.TakeCount, string.Join(", ", result.TableQuery.SelectColumns ?? Enumerable.Empty<string>()));


            return await _repository
                .ExecuteQueryAsync<EntityAdapter<TEntity>>(result.TableQuery, cancellationToken)
                .Then(async p => await _repository.GetProcessedResultAsync(p, result).ConfigureAwait(false), cancellationToken)
                .ConfigureAwait(false);

        }




    }
}
