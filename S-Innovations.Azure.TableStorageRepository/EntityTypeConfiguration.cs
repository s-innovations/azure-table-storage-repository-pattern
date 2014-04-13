using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System;
using System.Collections.Concurrent;
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
        public Action<TEntity,string,string> ReverseKeysMapper { get; set; }
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

        public Func<IQueryable<TChild>, TEntity, IQueryable<TChild>> GetFilter<TEntity, TChild>()
        {
            return (Func<IQueryable<TChild>, TEntity, IQueryable<TChild>>)Filter;
        }
        public void GetFilterQuery<TEntity, TChild>(ITableRepository<TChild> source, TEntity entity) 
        {
            var filterFunc = (Func<ITableRepository<TChild>, TEntity, IQueryable<TChild>>)Filter;
            source.parentQuery= filterFunc(source, entity);

        }
         
    }
    public class EntityTypeConfiguration
    {
        protected readonly TableStorageModelBuilder builder;
        public ConcurrentDictionary<long, Tuple<DateTimeOffset, string>> EntityStates { get; set; }

        public EntityTypeConfiguration(TableStorageModelBuilder builder)
        {
            this.builder = builder;
            Indexes = new Dictionary<string, IndexConfiguration>();
            Collections = new List<CollectionConfiguration>();
            NamePairs = new Dictionary<string, string>();
            EntityStates = new ConcurrentDictionary<long, Tuple<DateTimeOffset, string>>();
          
        }
        public object KeyMapper { get; set; }

        public Dictionary<string, string> NamePairs { get; set; }
        public Dictionary<string, IndexConfiguration> Indexes { get; set; }
        public List<CollectionConfiguration> Collections { get; set; }

        public string TableName { get; protected set; }

        public KeysMapper<TEntity> GetKeyMappers<TEntity>() { 
            return (KeysMapper<TEntity>)KeyMapper;
        }

        public void ReverseKeyMapping<TEntity>(EntityAdapter<TEntity> entity) where TEntity : new()
        {
            ((KeysMapper<TEntity>)KeyMapper).ReverseKeysMapper(entity.InnerObject, entity.PartitionKey, entity.RowKey);
        }

    }

    public class EntityTypeConfiguration<TEntityType> : EntityTypeConfiguration
    {

       

        public EntityTypeConfiguration(TableStorageModelBuilder builder) :base(builder)
        {
    
        }
        public EntityTypeConfiguration<TEntityType> HasKeys<TPartitionKey, TRowKey>(
            Expression<Func<TEntityType, TPartitionKey>> PartitionKeyExpression, 
            Expression<Func<TEntityType, TRowKey>> RowKeyExpression)
        {
            string partitionKey="";
            string rowKey = "";
           
            var keyMapper = new KeysMapper<TEntityType> { 
                PartitionKeyMapper = ConvertToStringKey(PartitionKeyExpression, out partitionKey),
                RowKeyMapper = ConvertToStringKey(RowKeyExpression, out rowKey) };
            this.NamePairs.Add(partitionKey, "PartitionKey");
            this.NamePairs.Add(rowKey, "RowKey");

            Action<TEntityType,string> partitionAction = GetReverseActionFrom<TPartitionKey>(PartitionKeyExpression);
            Action<TEntityType, string> rowAction = GetReverseActionFrom<TRowKey>(RowKeyExpression);

            keyMapper.ReverseKeysMapper = (a, part, row) =>
            {
                partitionAction(a, part);
                rowAction(a, row);
            };

            KeyMapper = keyMapper;
            return this;
        }

        private Action<TEntityType, string> GetReverseActionFrom<TPartitionKey>(Expression<Func<TEntityType, TPartitionKey>> PartitionKeyExpression)
        {
            if (PartitionKeyExpression.Body is MemberExpression)
            {
                return GetReverseActionFrom<TPartitionKey>(PartitionKeyExpression.Body as MemberExpression);
            }
            else if (PartitionKeyExpression.Body is NewExpression)
            {
                return GetReverseActionFrom(PartitionKeyExpression.Body as NewExpression);
            }
            throw new NotImplementedException("Expression not known");
        }

        private Action<TEntityType, string> GetReverseActionFrom<TPartitionKey>(MemberExpression memberEx)
        {
            var property = memberEx.Member as PropertyInfo;
            return (a, partitionkey) => property.SetValue(a, StringTo(typeof(TPartitionKey), partitionkey));
        }

        private Action<TEntityType, string> GetReverseActionFrom(NewExpression newEx)
        {
            Action<TEntityType, string> partitionAction = (a, partitionkey) =>
            {
                var parts = partitionkey.Split(new[] { "__" }, StringSplitOptions.RemoveEmptyEntries);

                for (int i = 0; i < newEx.Members.Count && i < parts.Length; ++i)
                {
                    var prop = newEx.Members[i] as PropertyInfo;
                    prop.SetValue(a, StringTo(prop.PropertyType, partitionkey));
                }

            };
            return partitionAction;
        }

        private object StringTo(Type type, string key)
        {
      
            if (type == typeof(string))
                return key;
            else if (type == typeof(int))
                return int.Parse(key);
            else if (type == typeof(long))
                return long.Parse(key);
            else if (type == typeof(Guid))
                return Guid.Parse(key);
               
            throw new Exception("not supported type");
        }
        public EntityTypeConfiguration<TEntityType> WithIndex<T>(Expression<Func<TEntityType,T>> index, string TableName = null )
        {
            string key="";
            var finder = ConvertToStringKey(index,out key);
            Indexes.Add(key, new IndexConfiguration { Finder = finder, TableName = TableName });
            return this;
        }




        public EntityTypeConfiguration<TEntityType> WithCollectionOf<T>(
            Expression<Func<TEntityType, IEnumerable<T>>> expression,
            Func<IQueryable<T>, TEntityType, IQueryable<T>> filter)
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
                return (o) => ConvertToString(func(o));
            else
            {
                var properties = oType.GetProperties().Where(p=> IsStringConvertable(p.PropertyType)).ToArray();
                return (o) => {
                    var obj = func(o);
                    var objs = properties.Select(p => p.GetValue(obj));
                    if(objs.Any(p=>p==null))
                        return null;
                    return string.Join("__", objs);
                
                };
            }
        }
        private static string ConvertToString(object obj)
        {
            if(obj==null)
                return null;
            return obj.ToString();
        }
        public static bool IsStringConvertable(Type type)
        {
            return type.IsPrimitive || type == typeof(string) || type == typeof(Guid);
        }

    }


}
