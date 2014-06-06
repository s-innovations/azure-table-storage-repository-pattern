using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository
{
    public struct KeysMapper<TEntity>
    {
        public Func<TEntity, String> PartitionKeyMapper { get; set; }
        public Func<TEntity, String> RowKeyMapper { get; set; }
        public Action<TEntity, IDictionary<string, EntityProperty>, string, string> ReverseKeysMapper { get; set; }
    }
    public abstract class IndexConfiguration
    {
        public string TableName { get; set; }


        public abstract string GetIndexKey(object entity);

        public Func<object[], string> GetIndexKeyFunc { get; set; }
    }

    public class IndexConfiguration<TEntity> : IndexConfiguration
    {
        public Func<TEntity, string> Finder { get; set; }
        public override string GetIndexKey(object entity)
        {
            if (entity is IEntityAdapter)
                return Finder((TEntity)((IEntityAdapter)entity).GetInnerObject());
            return Finder((TEntity)entity);
        }
    }

    public class CollectionConfiguration
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
            source.BaseQuery = filterFunc(source, entity);

        }

        private static MethodInfo QueryFilterMethod = typeof(CollectionConfiguration).GetMethod("GetFilterQuery");
        internal void SetCollection(object obj, ITableStorageContext context)
        {

            //The collection is a differnt table
            var repository = Activator(context);

            QueryFilterMethod.MakeGenericMethod(ParentEntityType, EntityType)
                .Invoke(this, new object[] { repository, obj });

            PropertyInfo.SetValue(obj, repository);

        }


    }
    public abstract class PropertyConfiguration
    {
        public PropertyInfo PropertyInfo { get; set; }
        public object Deserializer { get; set; }
        public object Serializer { get; set; }
        //public Func<EntityProperty, TProperty> Deserializer { get; set; }
        //public Func<TProperty, EntityProperty> Serializer { get; set; }

        public abstract Task SetPropertyAsync(object obj, EntityProperty prop);
        public abstract Task<EntityProperty> GetPropertyAsync(object p);
    }
    public class PropertyConfiguration<PropertyType> : PropertyConfiguration
    {
        public override async Task SetPropertyAsync(object obj, EntityProperty prop)
        {
            PropertyInfo.SetValue(obj, await ((Func<EntityProperty, Task<PropertyType>>)Deserializer)(prop));
        }


        public override Task<EntityProperty> GetPropertyAsync(object p)
        {
            var obj = PropertyInfo.GetValue(p);
            if (obj != null)
                return ((Func<PropertyType, Task<EntityProperty>>)Serializer)((PropertyType)PropertyInfo.GetValue(p));
            return Task.FromResult<EntityProperty>(null);
        }
    }


    public abstract class EntityTypeConfiguration
    {
        // protected readonly TableStorageModelBuilder builder;
        public ConcurrentDictionary<long, Tuple<DateTimeOffset, string>> EntityStates { get; set; }

        public EntityTypeConfiguration()
        {
            // this.builder = builder;
            Indexes = new Dictionary<string, IndexConfiguration>();
            Collections = new List<CollectionConfiguration>();
            NamePairs = new Dictionary<string, string>();
            EntityStates = new ConcurrentDictionary<long, Tuple<DateTimeOffset, string>>();
            PropertiesToEncode = new List<string>();
            Properties = new List<PropertyConfiguration>();
        }
        public object KeyMapper { get; set; }

        public Dictionary<string, string> NamePairs { get; set; }
        public Dictionary<string, IndexConfiguration> Indexes { get; set; }
        public List<string> PropertiesToEncode { get; set; }
        public List<CollectionConfiguration> Collections { get; set; }
        public List<PropertyConfiguration> Properties { get; set; }

        public string TableName { get; protected set; }

        public KeysMapper<TEntity> GetKeyMappers<TEntity>()
        {
            return (KeysMapper<TEntity>)KeyMapper;
        }

        public void ReverseKeyMapping<TEntity>(EntityAdapter<TEntity> entity)
        {
            ((KeysMapper<TEntity>)KeyMapper).ReverseKeysMapper(entity.InnerObject, entity.Properties, entity.PartitionKey, entity.RowKey);
        }


    }

    public class EntityTypeConfiguration<TEntityType> : EntityTypeConfiguration
    {

        private static Action<TEntityType, IDictionary<string, EntityProperty>, string> EmptyReverseAction = (_, __, ___) => { };
        Func<IDictionary<string, EntityProperty>, Object[]> ArgumentsExpression;
        Func<IDictionary<string, EntityProperty>, TEntityType> CtorExpression;
        public EntityTypeConfiguration()
        {

        }






        public TEntityType CreateEntity(IDictionary<string, EntityProperty> properties)
        {
            if (CtorExpression != null)
                return CtorExpression(properties);

            if (ArgumentsExpression == null)
                return Activator.CreateInstance<TEntityType>();
            return (TEntityType)Activator.CreateInstance(typeof(TEntityType), ArgumentsExpression(properties));
        }
        public EntityTypeConfiguration<TEntityType> WithNoneDefaultConstructor(
            Func<IDictionary<string, EntityProperty>, Object[]> ArgumentsExpression)
        {
            this.ArgumentsExpression = ArgumentsExpression;
            return this;
        }
        public EntityTypeConfiguration<TEntityType> WithNoneDefaultConstructor(
            Func<IDictionary<string, EntityProperty>, TEntityType> CtorExpression)
        {
            this.CtorExpression = CtorExpression;
            return this;
        }

        public EntityTypeConfiguration<TEntityType> HasKeys<TPartitionKey, TRowKey>(
            Expression<Func<TEntityType, TPartitionKey>> PartitionKeyExpression,
            Expression<Func<TEntityType, TRowKey>> RowKeyExpression)
        {
            string partitionKey = "";
            string rowKey = "";

            var keyMapper = new KeysMapper<TEntityType>
            {
                PartitionKeyMapper = ConvertToStringKey(PartitionKeyExpression, out partitionKey, PropertiesToEncode.ToArray()),
                RowKeyMapper = ConvertToStringKey(RowKeyExpression, out rowKey, PropertiesToEncode.ToArray())
            };
            if (!string.IsNullOrEmpty(partitionKey))
                this.NamePairs.Add(partitionKey, "PartitionKey");
            if (!string.IsNullOrEmpty(rowKey))
                this.NamePairs.Add(rowKey, "RowKey");

            Trace.TraceInformation("Created Key Mapper: PartionKey: {0}, RowKey: {1}", partitionKey, rowKey);

            Action<TEntityType, IDictionary<string, EntityProperty>, string> partitionAction = GetReverseActionFrom<TPartitionKey>(PartitionKeyExpression);
            Action<TEntityType, IDictionary<string, EntityProperty>, string> rowAction = GetReverseActionFrom<TRowKey>(RowKeyExpression);

            keyMapper.ReverseKeysMapper = (a, dict, part, row) =>
            {
                partitionAction(a, dict, part);
                rowAction(a, dict, row);
            };

            KeyMapper = keyMapper;
            return this;
        }

        private Action<TEntityType, IDictionary<string, EntityProperty>, string> GetReverseActionFrom<TKey>(Expression<Func<TEntityType, TKey>> KeyExpression)
        {

            // When a key selector is used pointing to a property
            if (KeyExpression.Body is MemberExpression)
            {
                return GetReverseActionFrom<TKey>(KeyExpression.Body as MemberExpression);
            }
            // For composite keys a=>new{a.KeyPart1, A.KeyPart2} is used
            else if (KeyExpression.Body is NewExpression)
            {
                return GetReverseActionFrom(KeyExpression.Body as NewExpression);
            }
            return EmptyReverseAction;

        }

        private Action<TEntityType, IDictionary<string, EntityProperty>, string> GetReverseActionFrom<TPartitionKey>(MemberExpression memberEx)
        {
            var property = memberEx.Member as PropertyInfo;
            if (PropertiesToEncode.Contains(property.Name))
                return (a, dict, partitionkey) =>
                {
                    EntityProperty prop = null;
                    var key = StringTo(typeof(TPartitionKey), partitionkey.Base64Decode(), out prop);
                    if (prop != null)
                        dict[property.Name] = prop;
                    property.SetValue(a, key);
                };
            return (a, dict, partitionkey) =>
            {
                EntityProperty prop = null;
                var key = StringTo(typeof(TPartitionKey), partitionkey, out prop);
                if (prop != null)
                    dict[property.Name] = prop;
                property.SetValue(a, key);
            };
        }

        private Action<TEntityType, IDictionary<string, EntityProperty>, string> GetReverseActionFrom(NewExpression newEx)
        {
            Action<TEntityType, IDictionary<string, EntityProperty>, string> partitionAction = (obj, dict, partitionkey) =>
            {
                var parts = partitionkey.Split(new[] { TableStorageContext.KeySeparator }, StringSplitOptions.None);
                var type = typeof(TEntityType);
                for (int i = 0; i < newEx.Members.Count && i < parts.Length; ++i)
                {
                    //  PropertyInfo property = newEx.Members[i] as PropertyInfo;
                    PropertyInfo property = type.GetProperty(newEx.Members[i].Name);

                    if (PropertiesToEncode.Contains(newEx.Members[i].Name))
                        parts[i] = parts[i].Base64Decode();

                    EntityProperty prop = null;
                    var value = StringTo(property.PropertyType, parts[i], out prop);


                    if (property.SetMethod == null)
                        throw new Exception(string.Format("SetMethod was null: {1} {0} {{get;set;}}\n {2} \n {3}\n\n When using Composite Keys, do m => new {m.PropertyName0,m.PropertyName1}, and only int,long,guid,string properties are supported at this point.", property.Name, property.PropertyType, partitionkey, newEx));

                  
                    if (property.SetMethod != null)
                        property.SetValue(obj, value);

                    if (prop != null)
                        dict[property.Name] = prop;
                }

            };
            return partitionAction;
        }

        private object StringTo(Type type, string key, out EntityProperty prop)
        {
            prop = null;
            if (string.IsNullOrEmpty(key))
                return null;

            if (type == typeof(string))
            { prop = new EntityProperty((string)key); return key; }
            else if (type == typeof(int))
            { int value = int.Parse(key); prop = new EntityProperty(value); return value; }
            else if (type == typeof(long))
            { long value = long.Parse(key); prop = new EntityProperty(value); return value; }
            else if (type == typeof(Guid))
            { Guid value = Guid.Parse(key); prop = new EntityProperty(value); return value; }

            throw new Exception("not supported type");
        }
        public EntityTypeConfiguration<TEntityType> WithIndex<IndexKeyType>(Expression<Func<TEntityType, IndexKeyType>> IndexKeyExpression, string TableName = null)
        {
            string key = "";

            var entityToKeyProperty = ConvertToStringKey(IndexKeyExpression, out key, PropertiesToEncode.ToArray());
            Indexes.Add(key, new IndexConfiguration<TEntityType>
            {
                Finder = entityToKeyProperty,
                TableName = TableName,
                GetIndexKeyFunc = (objs) =>
                {
                    var propNames = key.Split(new[] { TableStorageContext.KeySeparator }, StringSplitOptions.RemoveEmptyEntries);
                    var idxKey = string.Join(TableStorageContext.KeySeparator, objs.Select((obj, idx) => ConvertToString(obj, PropertiesToEncode.Contains(propNames[idx]))));
                    return idxKey;
                }
            });

            //Action<TEntityType, string> partitionAction = GetReverseActionFrom<IndexKeyType>(IndexKeyExpression);

            return this;
        }


        public EntityTypeConfiguration<TEntityType> UseBase64EncodingFor<T>(Expression<Func<TEntityType, T>> expression)
        {
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;
                PropertiesToEncode.Add(memberEx.Member.Name);
            }


            return this;
        }
        public EntityTypeConfiguration<TEntityType> WithEnumProperties()
        {
            //      var type = typeof(PropertyConfiguration<>);
            var fact = this.GetType().GetMethod("PropertyConfigurationFactory", BindingFlags.Static | BindingFlags.NonPublic);

            foreach (var prop in typeof(TEntityType).GetProperties().Where(p => p.PropertyType.IsEnum))
            {
                PropertyConfiguration config = (PropertyConfiguration)fact.MakeGenericMethod(prop.PropertyType).Invoke(null, null);
                config.PropertyInfo = prop;
                this.Properties.Add(config);
            }
            return this;
        }

        #region UriProperties

        public EntityTypeConfiguration<TEntityType> WithUriProperties()
        {
            foreach (var prop in typeof(TEntityType).GetProperties().Where(p => p.PropertyType == typeof(Uri)))
            {
                this.Properties.Add(new PropertyConfiguration<Uri>
                {
                    PropertyInfo = prop,
                    Serializer = (Func<Uri, Task<EntityProperty>>)UriSerializer,
                    Deserializer = (Func<EntityProperty, Task<Uri>>)UriDeserializer

                });
            }
            return this;
        }
        private static Task<Uri> UriDeserializer(EntityProperty property)
        {
            return Task.FromResult(new Uri(property.StringValue));
        }
        private static Task<EntityProperty> UriSerializer(Uri uri)
        {
            return Task.FromResult(new EntityProperty(uri.AbsoluteUri));
        }
        #endregion

        private static PropertyConfiguration PropertyConfigurationFactory<T>()
        {
            return new PropertyConfiguration<T>()
                {
                    Deserializer = (Func<EntityProperty, Task<T>>)((property) => Task.FromResult(JsonConvert.DeserializeObject<T>(property.StringValue))),
                    Serializer = (Func<T, Task<EntityProperty>>)(p => Task.FromResult(new EntityProperty(JsonConvert.SerializeObject(p))))
                };
        }


        /// <summary>
        /// Configure a custom property with custom serializer/deserializer
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="expression"></param>
        /// <param name="deserializer"></param>
        /// <param name="serializer"></param>
        /// <returns></returns>
        public EntityTypeConfiguration<TEntityType> WithPropertyOf<T>(
           Expression<Func<TEntityType, T>> expression,
           Func<EntityProperty, Task<T>> deserializer = null,
           Func<T, Task<EntityProperty>> serializer = null)
        {
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;

                this.Properties.Add(new PropertyConfiguration<T>
                {
                    PropertyInfo = memberEx.Member as PropertyInfo,
                    // EntityType = typeof(T),
                    // ParentEntityType = typeof(TEntityType),
                    Deserializer = deserializer ?? (p => Task.FromResult(JsonConvert.DeserializeObject<T>(p.StringValue))),
                    Serializer = serializer ?? (p => Task.FromResult(new EntityProperty(JsonConvert.SerializeObject(p)))),
                });
            }

            return this;

        }

        //public EntityTypeConfiguration<TEntityType> WithPropertyOf<T>(
        //    Expression<Func<TEntityType, T>> expression,
        //    Func<EntityProperty, T> deserializer,
        //    Func<T,EntityProperty> serializer)
        //{
        //    return WithPropertyOf(expression, p => Task.FromResult(deserializer(p)), p => Task.FromResult(serializer(p)));

        //}

        public EntityTypeConfiguration<TEntityType> WithCollectionOf<T>(
            Expression<Func<TEntityType, IEnumerable<T>>> expression,
            Func<IQueryable<T>, TEntityType, IQueryable<T>> filter)
        {
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;

                Func<ITableStorageContext, ITableRepository<T>> activator =
                    (ctx) => Factory.RepositoryFactory<T>(ctx, EntityTypeConfigurationsContainer.Entity<T>());

                //this.builder.Entity<TEntityType>()                 
                this.Collections.Add(new CollectionConfiguration
                {
                    PropertyInfo = memberEx.Member as PropertyInfo,
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

        public static Func<TEntityType, string> ConvertToStringKey<T>(Expression<Func<TEntityType, T>> expression, out string key, string[] encodedProperties)
        {
            var func = expression.Compile();
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;
                var propertyName = memberEx.Member.Name;
                key = propertyName;
                return (o) => ConvertToString(func(o), encodedProperties.Contains(propertyName));
            }
            else if (expression.Body is NewExpression)
            {
                Trace.TraceInformation("Using NewExpressino for KeyMapping");
                var newEx = expression.Body as NewExpression;
                key = string.Join(TableStorageContext.KeySeparator, newEx.Members.Select(m => m.Name));

                return (o) =>
                {
                    if (o == null)
                        throw new ArgumentNullException("Object cannot be null");

                    var obj = func(o);

                    var properties = newEx.Members.OfType<PropertyInfo>().ToArray();
                    var objs = properties.Select((p, i) => ConvertToString(p.GetValue(obj), encodedProperties.Contains(properties[i].Name)));

                    //If any nulls, then the key becomes a enmpty string.
                    //   if (objs.Any(p => p == null))
                    //       return "";

                    return string.Join(TableStorageContext.KeySeparator, objs.Select(t => t == null ? "" : t));

                };
            }
            else
            {
                key = "";
            }

            return (a) => "";

        }
        private static string ConvertToString(object obj, bool encode)
        {
            if (obj == null)
                return null;
            var str = obj.ToString();
            if (encode)
                return str.Base64Encode();
            return str;
        }
        public static bool IsStringConvertable(Type type)
        {
            return type.IsPrimitive || type == typeof(string) || type == typeof(Guid);
        }





    }


}
