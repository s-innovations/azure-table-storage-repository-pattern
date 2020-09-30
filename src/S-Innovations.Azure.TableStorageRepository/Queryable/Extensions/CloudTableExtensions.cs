
using Microsoft.Azure.Cosmos.Table;
using SInnovations.Azure.TableStorageRepository;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace Microsoft.Azure.Cosmos.Table
{
    public static class CloudTableExtensions
    {
        /// <summary>
        ///     Executes a query on a table asynchronously.
        /// </summary>
        /// <param name="cloudTable">Cloud table.</param>
        /// <param name="tableQuery">Table query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        ///     An enumerable collection of <see cref="T:Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity" /> objects, representing table entities returned by the query.
        /// </returns>
        public static Task<IEnumerable<T>> ExecuteQueryAsync<T>(
            this CloudTable cloudTable,
            TableQuery<T> tableQuery,
            CancellationToken cancellationToken = default(CancellationToken)) where T : ITableEntity,new()
        {
            return ExecuteQuerySegmentedImplAsync(
                cloudTable, new List<T>(), tableQuery, null, cancellationToken)
                .Then(results => (IEnumerable<T>)results);
        }
        /// <summary>
        ///     Aggregates query execution segments.
        /// </summary>
        /// <param name="cloudTable">Cloud table.</param>
        /// <param name="tableEntities">Table entities.</param>
        /// <param name="tableQuery">Table query.</param>
        /// <param name="continuationToken">Continuation token.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>
        ///     An enumerable collection of <see cref="T:Microsoft.WindowsAzure.Storage.Table.DynamicTableEntity" /> objects, representing table entities returned by the query.
        /// </returns>
        private static Task<List<T>> ExecuteQuerySegmentedImplAsync<T>(
            this CloudTable cloudTable,
            List<T> tableEntities,
            TableQuery<T> tableQuery,
            TableContinuationToken continuationToken,
            CancellationToken cancellationToken = default (CancellationToken)) where T : ITableEntity,new()
        {
            return cloudTable
                .ExecuteQuerySegmentedAsync(tableQuery, continuationToken,null,null, cancellationToken)
                .Then(result =>
                {
                    cancellationToken.ThrowIfCancellationRequested();

                    tableEntities.AddRange(result.Results);

                    // Checks whether TakeCount entities has been received
                    if (tableQuery.TakeCount.HasValue && tableEntities.Count >= tableQuery.TakeCount.Value)
                    {
                        return TaskHelpers.FromResult(tableEntities.Take(tableQuery.TakeCount.Value).ToList());
                    }

                    // Checks whether enumeration has been completed
                    if (result.ContinuationToken != null)
                    {
                        return ExecuteQuerySegmentedImplAsync(cloudTable, tableEntities, tableQuery, result.ContinuationToken, cancellationToken);
                    }

                    return TaskHelpers.FromResult(tableEntities);
                });
        }
    }
}
