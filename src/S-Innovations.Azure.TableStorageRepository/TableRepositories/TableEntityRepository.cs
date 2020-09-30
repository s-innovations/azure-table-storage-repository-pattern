
using System;
using System.Collections;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.TableRepositories
{
    //[Obsolete("Dont use this, there is fine support for ITableEntity build in")]
    //public class TableEntityRepository<TEntity> :
    //       TableRepository<TEntity>,
    //       ITableRepository<TEntity>
    //       where TEntity : ITableEntity,new()
    //{       
    

    //    public IQueryable<TEntity> BaseQuery { get; set; }


    //    public TableEntityRepository(ITableStorageContext context, EntityTypeConfiguration configuration)
    //        : base(context, configuration)
    //    {


    //    }

    //    public IEnumerable<TEntity> FluentQuery(string filter)
    //    {
    //        return table.ExecuteQuery(new TableQuery<TEntity>().Where(filter));
    //    }
        
    //    public void Clear()
    //    {
    //        //   _cache.Clear();
    //    }

    //    public bool Contains(TEntity item)
    //    {
    //        return false;// _cache.Any(t => t.Entity.Equals(item));
    //    }

    //    public void CopyTo(TEntity[] array, int arrayIndex)
    //    {
    //        throw new NotImplementedException();
    //    }

    //    public int Count
    //    {
    //        get
    //        {
    //            return 0;// _cache.Count; 
    //        }
    //    }

    //    public bool IsReadOnly
    //    {
    //        get { return true; }
    //    }

    //    public bool Remove(TEntity item)
    //    {
    //        this.Delete(item);
    //        return true;
    //    }
    //    public IEnumerator<TEntity> GetEnumerator()
    //    {
    //        if (BaseQuery != null)
    //            return BaseQuery.GetEnumerator();
    //        return Table.CreateQuery<TEntity>().GetEnumerator(); 
    //       // return ((IEnumerable<TEntity>)Provider.Execute(Expression)).GetEnumerator();
    //    }

    //    IEnumerator IEnumerable.GetEnumerator()
    //    {
    //        return GetEnumerator();
    //    //    return ((IEnumerable)Provider.Execute(Expression)).GetEnumerator();
    //    }

    //    public Type ElementType
    //    {
    //        get { return typeof(TEntity); }
    //    }

    //    public System.Linq.Expressions.Expression Expression
    //    {
    //        get
    //        {
    //            if (BaseQuery != null)
    //                return BaseQuery.Expression;
    //            return table.CreateQuery<TEntity>().Expression;
    //        }
    //    }

    //    public IQueryProvider Provider
    //    {
    //        get
    //        {
    //            if (BaseQuery != null)
    //                return BaseQuery.Provider;
    //            return table.CreateQuery<TEntity>().Provider;
    //        }
    //    }


    //    public void Add(TEntity entity, IDictionary<string, EntityProperty> additionalProperties)
    //    {
    //        throw new NotImplementedException();
    //    }



    //    public Task<IDictionary<string, EntityProperty>> FindPropertiesByKeysAsync(string partitionKey, string rowKey)
    //    {
    //        throw new NotImplementedException();
    //    }


    //    public CloudTable Table
    //    {
    //        get { return table; }
    //    }


    //    public void Update(TEntity entity, IDictionary<string, EntityProperty> additionalProperties)
    //    {
    //        throw new NotImplementedException();
    //    }


    //    public Task<IDictionary<string, EntityProperty>> FindPropertiesByKeysAsync(string partitionKey, string rowKey, params string[] properties)
    //    {
    //        throw new NotImplementedException();
    //    }
    //}
}
