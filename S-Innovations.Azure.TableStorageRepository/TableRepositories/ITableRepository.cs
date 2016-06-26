using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.Queryable.Wrappers;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.TableRepositories
{
    public interface ITableRepository
    {
        Task SaveChangesAsync();


    }

    public interface ITableRepository<TEntity> : ITableRepository,
        ICollection<TEntity>, 
        IQueryable<TEntity>
    {

    
        EntityTypeConfiguration<TEntity> Configuration { get; }
        ITableStorageContext Context { get; }
        /// <summary>
        /// Overrides the modelbuild key selectors.
        /// </summary>
        /// <param name="entity"></param>
        /// <param name="partitionkey"></param>
        /// <param name="rowkey"></param>
        void Add(TEntity entity,string partitionkey,string rowkey);

        void AddRevision(TEntity entity, EntityAdapter<TEntity>.OnEntityChanged onEntityChanged);

        void Add(TEntity entity, IDictionary<string, EntityProperty> additionalProperties);
        void Delete(TEntity entity);
        void Update(TEntity entity);
        void Update(TEntity entity, IDictionary<string, EntityProperty> additionalProperties);

        IQueryable<TEntity> BaseQuery { get; set; }

        IEnumerable<TEntity> FluentQuery(string filter);
        Task<TEntity> FindByIndexAsync(params object[] keys);
        Task<TEntity> FindByKeysAsync(string partitionKey, string rowKey);
        Task DeleteByKey(string partitionKey, string rowKey);

        Task<Tuple<IEnumerable<TEntity>, TableContinuationToken>> ExecuteQuerySegmentedAsync(ITableQuery query, TableContinuationToken currentToken);
        CloudTable Table
        {
            get;
        }
        Task<IDictionary<string, EntityProperty>> FindPropertiesByKeysAsync(string partitionKey, string rowKey);

        Task<IDictionary<string, EntityProperty>> FindPropertiesByKeysAsync(string partitionKey, string rowKey, params string[] properties);

    }
}
