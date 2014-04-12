using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository
{
    public struct KeysMapper<TEntity>
    {
        public Func<TEntity, String> PartitionKeyMapper { get; set; }
        public Func<TEntity, String> RowKeyMapper { get; set; }
    }
    public struct IndexConfiguration
    {
        public string TableName { get; set; }
        public object Finder { get; set; }
        public string GetIndexKey<TEntity>(TEntity obj)
        {
            return ((Func<TEntity, string>)Finder)(obj);
        }
    }
    public struct CollectionConfiguration
    {
        
        public PropertyInfo PropertyInfo { get; set; }
        public Type EntityType { get; set; }
        public Type ParentEntityType { get; set; }

        public Func<ITableStorageContext, object> Activator { get; set; }

        public object Filter { get; set; }

        public Func<TableQuery<TChild>, TEntity, IQueryable<TChild>> GetFilter<TEntity, TChild>()
        {
            return (Func<TableQuery<TChild>, TEntity, IQueryable<TChild>>)Filter;
        }
        public void GetFilterQuery<TEntity, TChild>(TableRepository<TChild> source, TEntity entity)
        {
            var filterFunc = (Func<TableQuery<TChild>, TEntity, IQueryable<TChild>>)Filter;
            source.parentQuery= filterFunc(source.Source, entity);

        }
         
    }
    public class EntityTypeConfiguration
    {
        protected readonly TableStorageModelBuilder builder;
        public EntityTypeConfiguration(TableStorageModelBuilder builder)
        {
            this.builder = builder;
            Indexes = new Dictionary<string, IndexConfiguration>();
            Collections = new List<CollectionConfiguration>();
          
        }
        public object KeyMapper { get; set; }
        public Dictionary<string, IndexConfiguration> Indexes { get; set; }
        public List<CollectionConfiguration> Collections { get; set; }

        public string TableName { get; protected set; }

        public KeysMapper<TEntity> GetKeyMappers<TEntity>() { 
            return (KeysMapper<TEntity>)KeyMapper;
        }

    }

    public class EntityTypeConfiguration<TEntityType> : EntityTypeConfiguration
    {
        public EntityTypeConfiguration(TableStorageModelBuilder builder) :base(builder)
        {
    
        }
        public EntityTypeConfiguration<TEntityType> HasKeys<TPartitionKey, TRowKey>(Expression<Func<TEntityType, TPartitionKey>> PartitionKeyExpression, Expression<Func<TEntityType, TRowKey>> RowKeyExpression)
        {
            string v="";
            KeyMapper = new KeysMapper<TEntityType> { PartitionKeyMapper = ConvertToStringKey(PartitionKeyExpression, out v), RowKeyMapper = ConvertToStringKey(RowKeyExpression, out v) };
            return this;
        }
        public EntityTypeConfiguration<TEntityType> WithIndex<T>(Expression<Func<TEntityType,T>> index, string TableName = null )
        {
            string key="";
            var finder = ConvertToStringKey(index,out key);
            Indexes.Add(key, new IndexConfiguration { Finder = finder, TableName = TableName });
            return this;
        }

        public EntityTypeConfiguration<TEntityType> WithCollectionOf<T>(
            Expression<Func<TEntityType, ICollection<T>>> expression,
            Func<TableQuery<T>,TEntityType,IQueryable<T>> filter )
            where T : new()
        {
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;

                Func<ITableStorageContext, ITableRepository<T>> activator =
                    (ctx) => Factory.RepositoryFactory<T>(ctx, this.builder.Entity<T>());

                this.builder.Entity<TEntityType>()
                .Collections.Add(new CollectionConfiguration{ 
                    PropertyInfo =memberEx.Member as PropertyInfo,
                    EntityType = typeof(T),
                    ParentEntityType = typeof(TEntityType),
                    Activator = activator,
                    Filter = filter,
                });

            }


            return this;
        }
        public EntityTypeConfiguration<TEntityType> ToTable(string tableName)
        {

            this.TableName = tableName;

            return this;
        }
        
        public static Func<TEntityType, string> ConvertToStringKey<T>(Expression<Func<TEntityType, T>> expression, out string key)
        {
            
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;
                key = memberEx.Member.Name;
            }
            else if (expression.Body is NewExpression)
            {
                var newEx = expression.Body as NewExpression;
                key = string.Join("__", newEx.Members.Select(m => m.Name));

            }
            else
            {
                key = "";
            }

            var func = expression.Compile();
       
            var oType = typeof(T);
            if (IsStringConvertable(oType))
                return (o) => func(o).ToString();
            else
            {
                var properties = oType.GetProperties().Where(p=> IsStringConvertable(p.PropertyType)).ToArray();
                return (o) => {
                    var obj = func(o);
                    return string.Join("__", properties.Select(p => p.GetValue(obj).ToString()));
                
                };
            }
        }

        public static bool IsStringConvertable(Type type)
        {
            return type.IsPrimitive || type == typeof(string) || type == typeof(Guid);
        }

    }


}
