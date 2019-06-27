using SInnovations.Azure.TableStorageRepository.Queryable.Wrappers;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable
{
    public interface IWrappingQueryable<T>
    {
        IQueryable<T> Parent { get; }
    }
    public class PrefixWrappingQueryableWrapper<TEntity> : IQueryable<TEntity>, IWrappingQueryable<TEntity>
    {
        public IQueryable<TEntity> Parent { get; }
        public string Prefix { get; }

        public PrefixWrappingQueryableWrapper(IQueryable<TEntity> parent, string prefix)
        {
            this.Parent = parent;
            this.Prefix = prefix;
            this.Provider = (parent.Provider as TableQueryProvider<TEntity>).Clone(this);
        }
        public Type ElementType => this.Parent.ElementType;

        public Expression Expression => this.Parent.Expression;

        public IQueryProvider Provider { get; }

        public IEnumerator<TEntity> GetEnumerator() => this.Parent.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => this.Parent.GetEnumerator();

    }
    public class FromTableQueryableWrapper<TEntity> : IQueryable<TEntity>, IWrappingQueryable<TEntity>
    {
        public IQueryable<TEntity> Parent { get; }
        public string TableName { get; }

        public FromTableQueryableWrapper(IQueryable<TEntity> parent, string tableName)
        {
            this.Parent = parent;
            this.TableName = tableName;
            this.Provider = (parent.Provider as TableQueryProvider<TEntity>).Clone(this);
        }
        public Type ElementType => this.Parent.ElementType;

        public Expression Expression => this.Parent.Expression;

        public IQueryProvider Provider { get; }

        public IEnumerator<TEntity> GetEnumerator() => this.Parent.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => this.Parent.GetEnumerator();

    }
    public class FilterWrappingQueryableWrapper<TEntity> : IQueryable<TEntity>, IWrappingQueryable<TEntity>
    {
        public IQueryable<TEntity> Parent { get; }
        public string Filter { get; }

        public FilterWrappingQueryableWrapper(IQueryable<TEntity> parent, string filter)
        {
            this.Parent = parent;
            this.Filter = filter;
            this.Provider = (parent.Provider as TableQueryProvider<TEntity>).Clone(this);
        }
        public Type ElementType => this.Parent.ElementType;

        public Expression Expression => this.Parent.Expression;

        public IQueryProvider Provider { get; }

        public IEnumerator<TEntity> GetEnumerator() => this.Parent.GetEnumerator();

        IEnumerator IEnumerable.GetEnumerator() => this.Parent.GetEnumerator();

    }
    /// <summary>
    ///     LINQ extensions for a asynchronous query execution.
    /// </summary>
    public static class AsyncQueryExtensions
    {

        public static ITableQuery TranslateQuery<T>(this IQueryable<T> source)
        {
            var result = (source.Provider as TableQueryProvider<T>).GetTranslationResult(source.Expression);

            return result.TableQuery;
        }

        public static IQueryable<T> WithPrefix<T>(this IQueryable<T> source, string prefix)
        {
            return new PrefixWrappingQueryableWrapper<T>(source,prefix);

          //  (source.Provider as TableQueryProvider<T>).AddPrefix(prefix);

          //  return source;
        }
        public static IQueryable<T> FromTable<T>(this IQueryable<T> source, string tableName)
        {
            return new FromTableQueryableWrapper<T>(source, tableName);

            //  (source.Provider as TableQueryProvider<T>).AddPrefix(prefix);

            //  return source;
        }
        public static IQueryable<T> WithODataFilter<T>(this IQueryable<T> source, string filter)
        {
            if (string.IsNullOrEmpty(filter))
                return source;

            return new FilterWrappingQueryableWrapper<T>(source, filter);

            //(source.Provider as TableQueryProvider<T>).WithODataFilter(filter);

            //return source;
        }

        /// <summary>
        ///     Executes a query ToList method asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>List of entities.</returns>
        public static Task<List<T>> ToListAsync<T>(
            this IQueryable<T> source,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.ToList());
            }

            return tableQueryProvider.ExecuteAsync(source.Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).ToList(), cancellationToken);
        }


      


        /// <summary>
        ///     Executes a query ToList method asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="predicate">Predicate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>List of entities.</returns>
        public static Task<List<T>> ToListAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.ToList());
            }

            return tableQueryProvider.ExecuteAsync(source.Where(predicate).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).ToList(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query Take method asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="count">Entities count.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entities.</returns>
        public static Task<List<T>> TakeAsync<T>(
            this IQueryable<T> source,
            int count,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.Take(count).ToList());
            }

            return tableQueryProvider.ExecuteAsync(source.Take(count).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).ToList(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query First method asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> FirstAsync<T>(
            this IQueryable<T> source,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.First());
            }

            return tableQueryProvider.ExecuteAsync(source.Take(1).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).First(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query First method asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="predicate">Predicate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> FirstAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.First());
            }

            return tableQueryProvider.ExecuteAsync(source.Where(predicate).Take(1).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).First(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query FirstOrDefault asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> FirstOrDefaultAsync<T>(
            this IQueryable<T> source,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.FirstOrDefault());
            }

            return tableQueryProvider.ExecuteAsync(source.Take(1).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).FirstOrDefault(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query FirstOrDefault asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="predicate">Predicate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> FirstOrDefaultAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.FirstOrDefault());
            }

            return tableQueryProvider.ExecuteAsync(source.Where(predicate).Take(1).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).FirstOrDefault(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query Single asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> SingleAsync<T>(
            this IQueryable<T> source,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.Single());
            }

            return tableQueryProvider.ExecuteAsync(source.Take(2).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).Single(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query Single asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="predicate">Predicate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> SingleAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default(CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.Single());
            }

            return tableQueryProvider.ExecuteAsync(source.Where(predicate).Take(2).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).Single(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query SingleOrDefault method asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> SingleOrDefaultAsync<T>(
            this IQueryable<T> source,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.SingleOrDefault());
            }

            return tableQueryProvider.ExecuteAsync(source.Take(2).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).SingleOrDefault(), cancellationToken);
        }

        /// <summary>
        ///     Executes a query SingleOrDefault method asynchronously.
        /// </summary>
        /// <typeparam name="T">The entity type of the query.</typeparam>
        /// <param name="source">Query.</param>
        /// <param name="predicate">Predicate.</param>
        /// <param name="cancellationToken">Cancellation token.</param>
        /// <returns>Entity.</returns>
        public static Task<T> SingleOrDefaultAsync<T>(
            this IQueryable<T> source,
            Expression<Func<T, bool>> predicate,
            CancellationToken cancellationToken = default (CancellationToken))
        {
            var tableQueryProvider = source.Provider as IAsyncQueryProvider;

            if (tableQueryProvider == null)
            {
                return TaskHelpers.FromResult(source.SingleOrDefault());
            }

            return tableQueryProvider.ExecuteAsync(source.Where(predicate).Take(2).Expression, cancellationToken)
                                     .Then(result => ((IEnumerable<T>)result).SingleOrDefault(), cancellationToken);
        }
    }
}
