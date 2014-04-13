using Microsoft.WindowsAzure.Storage.Table;
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
        //void Add(TEntity entity);
        void Delete(TEntity entity);
        void Update(TEntity entity);

        IQueryable<TEntity> BaseQuery { get; set; }

        IEnumerable<TEntity> FluentQuery(string filter);
        Task<TEntity> FindByIndexAsync(params object[] keys);
        Task<TEntity> FindByKeysAsync(string partitionKey, string rowKey);
    }
}
