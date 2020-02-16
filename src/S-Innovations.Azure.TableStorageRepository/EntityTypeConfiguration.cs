using Microsoft.WindowsAzure.Storage.Table;
using Newtonsoft.Json;
using Microsoft.Extensions.Logging;
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
using SInnovations.Azure.TableStorageRepository.Queryable.Wrappers;
using System.IO;

namespace SInnovations.Azure.TableStorageRepository
{
    public class KeysMapper<TEntity>
    {
        public Func<TEntity, String> PartitionKeyMapper { get; set; }
        public Func<TEntity, String> RowKeyMapper { get; set; }
        public Action<TEntity, IDictionary<string, EntityProperty>, string, string> ReverseKeysMapper { get; set; }
    }
    public abstract class IndexConfiguration
    {
        public Func<ITableStorageContext,string> TableName { get; set; }
        public string TableNamePostFix { get; set; } = "Index";

        public abstract string[] GetIndexKey(object entity);
        public abstract string GetIndexSecondKey(object entity);
        public bool CopyAllProperties { get; set; }

        public Func<object[], (string,string)> GetIndexKeyFunc { get; set; }
    }

    public class IndexConfiguration<TEntity> : IndexConfiguration
    {
        public Func<TEntity, string[]> PartitionSplitKeyProvider { get; set; }
        public Func<TEntity, string> PartitionKeyProvider { get; set; }
        public Func<TEntity, string> RowKeyProvider { get; set; }

        public override string[] GetIndexKey(object entity)
        {
           if (entity is IEntityAdapter adapter && adapter.IsReversionClone)
           {
                //Dont index reversion clones
                return null;
           }


            TEntity data = entity is IEntityAdapter ? (TEntity)((IEntityAdapter)entity).GetInnerObject() : (TEntity)entity;

            if(PartitionSplitKeyProvider != null)
            {
                return PartitionSplitKeyProvider(data);
            }
           
            return new[] { PartitionKeyProvider(data) };
        }
        public override string GetIndexSecondKey(object entity)
        {
            if (RowKeyProvider == null)
            {
                return "";
            }

            if (entity is IEntityAdapter)
                return RowKeyProvider((TEntity)((IEntityAdapter)entity).GetInnerObject());
            return RowKeyProvider((TEntity)entity);
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

        private static MethodInfo QueryFilterMethod = typeof(CollectionConfiguration).GetTypeInfo().GetDeclaredMethod("GetFilterQuery");
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

        public object Composer { get; set; }

        public object Decomposer { get; set; }
        public bool IsComposite { get; set; }

        //public Func<EntityProperty, TProperty> Deserializer { get; set; }
        //public Func<TProperty, EntityProperty> Serializer { get; set; }

        public abstract Task SetPropertyAsync(object obj, EntityProperty prop);
        public abstract Task SetCompositePropertyAsync(object innerObject, IDictionary<string, EntityProperty> properties);

        public abstract Task<EntityProperty> GetPropertyAsync(object p);
        public abstract Task<IDictionary<string,EntityProperty>> GetPropertiesAsync(object p);
    }
    public class PropertyConfiguration<TEntity,PropertyType> : PropertyConfiguration
    {
        public bool IsEntityComposed { get; internal set; }

        public override async Task SetPropertyAsync(object obj, EntityProperty prop)
        {
            PropertyInfo.SetValue(obj, await ((Func<EntityProperty, Task<PropertyType>>)Deserializer)(prop));
        }


        public override Task<EntityProperty> GetPropertyAsync(object p)
        {
            var obj = (PropertyType)PropertyInfo.GetValue(p);
         
            if(EqualityComparer<PropertyType>.Default.Equals(obj, default) || obj == null)
                return Task.FromResult<EntityProperty>(null);
         
              
            return ((Func<PropertyType, Task<EntityProperty>>)Serializer)(obj);
          
        }
        public override Task<IDictionary<string, EntityProperty>> GetPropertiesAsync(object p)
        {
            if (IsComposite)
            {
                var obj = PropertyInfo.GetValue(p);
                if (obj != null)
                    return ((Func<PropertyType, Task<IDictionary<string, EntityProperty>>>)Decomposer)((PropertyType)PropertyInfo.GetValue(p));

            }
            return Task.FromResult<IDictionary<string, EntityProperty>>(null);
        }

        public override async Task SetCompositePropertyAsync(object innerObject, IDictionary<string, EntityProperty> properties)
        {
            if (IsEntityComposed)
            {
         
             
               var method = (Func <TEntity, IDictionary< string, EntityProperty >, Task < PropertyType >> )Composer;
                PropertyInfo.SetValue(innerObject, await method((TEntity)innerObject, properties));
            }
            else
            {
                PropertyInfo.SetValue(innerObject, await ((Func<IDictionary<string, EntityProperty>, Task<PropertyType>>)Composer)(properties));
            }
        }
       

    }

    public class EncoderPair
    {
        //public string Key { get; set; }
        public Func<string, string> Encoder { get; set; }
        public Func<string, string> Decoder { get; set; }
    }
    public abstract class EntityTypeConfiguration
    {
        // protected readonly TableStorageModelBuilder builder;
        public ConcurrentDictionary<long, Tuple<DateTimeOffset, string>> EntityStates { get; set; }

        public ReversionTrackingOptions ReversionTracking { get; set; } = new ReversionTrackingOptions();

        public bool TraceOnAdd { get; set; }

        public EntityTypeConfiguration()
        {
            // this.builder = builder;
            Indexes = new Dictionary<string, IndexConfiguration>();
            Collections = new List<CollectionConfiguration>();
            KeyMappings = new Dictionary<string, string>();
            EntityStates = new ConcurrentDictionary<long, Tuple<DateTimeOffset, string>>();
            PropertiesToEncode = new Dictionary<string, EncoderPair>(); // new List<string>();
            Properties = new List<PropertyConfiguration>();

             IgnorePartitionKeyPropertyRemovables = new Dictionary<string, object>();
            IgnoreRowKeyPropertyRemovables = new Dictionary<string, object>();
        }
        public object KeyMapper { get; set; }

        public Dictionary<string, string> KeyMappings { get; set; }
        public Dictionary<string, IndexConfiguration> Indexes { get; set; }
        public Dictionary<string, EncoderPair> PropertiesToEncode { get; set; }
        public Dictionary<string,object> IgnorePartitionKeyPropertyRemovables { get; set; }
        public Dictionary<string, object> IgnoreRowKeyPropertyRemovables { get; set; }

        public bool IgnoredKey(string key) => this.IgnoreRowKeyPropertyRemovables.ContainsKey(key) || this.IgnorePartitionKeyPropertyRemovables.ContainsKey(key);

        public List<CollectionConfiguration> Collections { get; set; }
        public List<PropertyConfiguration> Properties { get; set; }

        public Func<ITableStorageContext,string> TableName { get; protected set; }

        public KeysMapper<TEntity> GetKeyMappers<TEntity>()
        {
            return (KeysMapper<TEntity>)KeyMapper;
        }

        public void ReverseKeyMapping<TEntity>(EntityAdapter<TEntity> entity)
        {
            ((KeysMapper<TEntity>)KeyMapper).ReverseKeysMapper(entity.InnerObject, entity.Properties, entity.Properties.ContainsKey("RefPartitionKey")? entity.Properties["RefPartitionKey"]?.StringValue : entity.PartitionKey, entity.Properties.ContainsKey("RefRowKey") ? entity.Properties["RefRowKey"]?.StringValue : entity.RowKey);
        }


    }
    public enum PaddingDirection
    {
        Left,
        Right,
    }
    public struct LengthPadding
    {
        public int Length { get; set; }
        public PaddingDirection Direction { get; set; }
    }

    public class ReversionTrackingOptions
    {
        public bool Enabled { get; set; }
     //   public delegate ITableQuery FilterDelegate<T>(ITableRepository<T> table, T entity);
        public Delegate HeadWhereFilter { get; set; }
   //     public Delegate UpdateReversion { get; set; }

      //  public object OnEntityChanged { get; set; }
         
        //    public object CreateReversion { get; set; }

    }
    public class EntityTypeConfiguration<TEntityType> : EntityTypeConfiguration
    {
        private readonly ILogger Logger;
        private readonly ILoggerFactory _loggerFactory;
        private readonly IEntityTypeConfigurationsContainer _container;

        private static Action<TEntityType, IDictionary<string, EntityProperty>, string> EmptyReverseAction = (_, __, ___) => { };
        Func<IDictionary<string, EntityProperty>, Object[]> ArgumentsExpression;
        Func<IDictionary<string, EntityProperty>,string,DateTimeOffset, TEntityType> CtorExpression;
        public EntityTypeConfiguration(ILoggerFactory factory, IEntityTypeConfigurationsContainer container)
        {
            _loggerFactory = factory;
            _container = container;
            this.Logger = factory.CreateLogger<EntityTypeConfiguration<TEntityType>>();
        }






        public TEntityType CreateEntity(IDictionary<string, EntityProperty> properties,string etag, DateTimeOffset timestamp)
        {
            if (CtorExpression != null)
                return CtorExpression(properties,etag, timestamp);

            if (ArgumentsExpression == null)
                return Activator.CreateInstance<TEntityType>();
            return (TEntityType)Activator.CreateInstance(typeof(TEntityType), ArgumentsExpression(properties));
        }
        public EntityTypeConfiguration<TEntityType> WithConstructorArguments(
            Func<IDictionary<string, EntityProperty>, Object[]> ArgumentsExpression)
        {
            this.ArgumentsExpression = ArgumentsExpression;
            return this;
        }
        public EntityTypeConfiguration<TEntityType> WithNoneDefaultConstructor(
            Func<IDictionary<string, EntityProperty>, string, DateTimeOffset, TEntityType> CtorExpression)
        {
            this.CtorExpression = CtorExpression;
            return this;
        }


        public EntityTypeConfiguration<TEntityType> HasKeys<TPartitionKey>(
             Expression<Func<TEntityType, TPartitionKey>> PartitionKeyExpression,
             params LengthPadding?[] keylenghts)
        {
            return HasKeys<TPartitionKey, String>(PartitionKeyExpression, null, keylenghts);
        }

        public EntityTypeConfiguration<TEntityType> WithKeyTransformation<PropertyType>(
            Expression<Func<TEntityType, PropertyType>> propertyExpression, Func<PropertyType,string> encoder, string KeyType=null)
        {
            if (propertyExpression.Body is MemberExpression)
            {
                var memberEx = propertyExpression.Body as MemberExpression;
                if (KeyType == "PartitionKey")
                {
                    IgnorePartitionKeyPropertyRemovables.Add(memberEx.Member.Name, encoder);
                }else
                if(KeyType == "RowKey")
                {
                    IgnoreRowKeyPropertyRemovables.Add(memberEx.Member.Name, encoder);
                }
                else
                {
                    IgnorePartitionKeyPropertyRemovables.Add(memberEx.Member.Name, encoder);
                    IgnoreRowKeyPropertyRemovables.Add(memberEx.Member.Name, encoder);
                }
            }

            return this;
        }

        public EntityTypeConfiguration<TEntityType> HasKeys<TPartitionKey, TRowKey>(
            Expression<Func<TEntityType, TPartitionKey>> PartitionKeyExpression,
            Expression<Func<TEntityType, TRowKey>> RowKeyExpression, params LengthPadding?[] keylenghts)
        {
            string partitionKey = "";
            string rowKey = "";
            var lengthQueue = new Queue<LengthPadding?>(keylenghts);

            var keyMapper = new KeysMapper<TEntityType>
            {
                PartitionKeyMapper = ConvertToStringKey(PartitionKeyExpression,IgnorePartitionKeyPropertyRemovables, out partitionKey, lengthQueue),
                RowKeyMapper = RowKeyExpression == null ? (e) => "" : ConvertToStringKey(RowKeyExpression, IgnoreRowKeyPropertyRemovables, out rowKey, lengthQueue)
            };
            if (!string.IsNullOrEmpty(partitionKey))
                this.KeyMappings.Add(partitionKey, "PartitionKey");
            if (!string.IsNullOrEmpty(rowKey))
                this.KeyMappings.Add(rowKey, "RowKey");

            Logger.LogDebug("Created Key Mapper: PartionKey: {0}, RowKey: {1}", partitionKey, rowKey);

            Action<TEntityType, IDictionary<string, EntityProperty>, string> partitionAction = GetReverseActionFrom<TPartitionKey>(PartitionKeyExpression);
            Action<TEntityType, IDictionary<string, EntityProperty>, string> rowAction = RowKeyExpression == null ? (e, d, s) => { } : GetReverseActionFrom<TRowKey>(RowKeyExpression);

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
            if (PropertiesToEncode.Keys.Contains(property.Name))
                return (a, dict, partitionkey) =>
                {
                    EntityProperty prop = null;
                    var key = StringTo(typeof(TPartitionKey), PropertiesToEncode[property.Name].Decoder(partitionkey), out prop);
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
                    PropertyInfo property = type.GetRuntimeProperty(newEx.Members[i].Name);
                    if (IgnoredKey(newEx.Members[i].Name))
                        property = null;

                    if (i + 1 == newEx.Members.Count && i + 1 < parts.Length)
                        parts[i] = string.Join(TableStorageContext.KeySeparator, parts.Skip(i));

                    if (PropertiesToEncode.Keys.Contains(newEx.Members[i].Name))
                        parts[i] = PropertiesToEncode[newEx.Members[i].Name].Decoder(parts[i]);//.Base64Decode();

                    if (property != null && property.SetMethod != null)
                    {
                        EntityProperty prop = null;
                        var value = StringTo(property.PropertyType, parts[i], out prop);


                       // if ( property.SetMethod == null)
                       //     throw new Exception(string.Format("SetMethod was null: {1} {0} {{get;set;}}\n {2} \n {3}\n\n When using Composite Keys, do m => new {{m.PropertyName0,m.PropertyName1}}, and only int,long,guid,string properties are supported at this point.", property.Name, property.PropertyType, partitionkey, newEx));


                        if (property.SetMethod != null)
                            property.SetValue(obj, value);

                      //  if (prop != null)
                      //      dict[property.Name] = prop;
                    }
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
        public EntityTypeConfiguration<TEntityType> WithIndex<IndexKeyType>(Expression<Func<TEntityType, IndexKeyType>> IndexKeyExpression, bool CopyAllProperties=false, string TableName = null, string partitionPrefix = null)
        {
            string key = "";

            var entityToKeyProperty = ConvertToStringKey(IndexKeyExpression,IgnorePartitionKeyPropertyRemovables, out key,null,partitionPrefix);
            Indexes.Add(key, new IndexConfiguration<TEntityType>
            {
                PartitionKeyProvider = entityToKeyProperty,
                TableName = (ctx)=> TableName ?? (string.IsNullOrWhiteSpace(this.TableName?.Invoke(ctx)) ? null : this.TableName(ctx) + "Index"),            
                GetIndexKeyFunc = (objs) =>
                {
                    var propNames = key.Split(new[] { TableStorageContext.KeySeparator }, StringSplitOptions.RemoveEmptyEntries);
                    var idxKey = string.Join(TableStorageContext.KeySeparator, objs.Select((obj, idx) => ConvertToString(
                        IgnorePartitionKeyPropertyRemovables.ContainsKey(propNames[idx]) ?
                            TypeConvert(IgnorePartitionKeyPropertyRemovables[propNames[idx]], obj) : obj, GetEncoder(propNames[idx]))));

                    if(string.IsNullOrEmpty(partitionPrefix))
                        return (idxKey,"");
                    return ($"{partitionPrefix}{TableStorageContext.KeySeparator}{idxKey}","");
                },
                CopyAllProperties = CopyAllProperties,
            });

            //Action<TEntityType, string> partitionAction = GetReverseActionFrom<IndexKeyType>(IndexKeyExpression);

            return this;
        }
        public EntityTypeConfiguration<TEntityType> WithIndex<IndexKeyType, SecondaryIndexKeyType>(Expression<Func<TEntityType, IndexKeyType>> IndexKeyExpression, Expression<Func<TEntityType, SecondaryIndexKeyType>> SecondaryIndexKeyExpression,bool CopyAllProperties = false, string TableName = null, string partitionPrefix = null)
        {
            string key = "";
            string row = "";

            var entityToKeyProperty = ConvertToStringKey(IndexKeyExpression, IgnorePartitionKeyPropertyRemovables, out key, null, partitionPrefix);
            var secondaryProp = ConvertToStringKey(SecondaryIndexKeyExpression, IgnorePartitionKeyPropertyRemovables, out row, null);
            Indexes.Add(key, new IndexConfiguration<TEntityType>
            {
                PartitionKeyProvider = entityToKeyProperty,
                RowKeyProvider = secondaryProp,
                TableName = (ctx) => TableName ?? (string.IsNullOrWhiteSpace(this.TableName?.Invoke(ctx)) ? null : this.TableName(ctx) + "Index"),
                GetIndexKeyFunc = (objs) =>
                {
                    var propNames = key.Split(new[] { TableStorageContext.KeySeparator }, StringSplitOptions.RemoveEmptyEntries);
                    var idxKey = string.Join(TableStorageContext.KeySeparator, objs.Select((obj, idx) => ConvertToString(
                        IgnorePartitionKeyPropertyRemovables.ContainsKey(propNames[idx]) ?
                            TypeConvert(IgnorePartitionKeyPropertyRemovables[propNames[idx]], obj) : obj, GetEncoder(propNames[idx]))));

                    var propNames1 = row.Split(new[] { TableStorageContext.KeySeparator }, StringSplitOptions.RemoveEmptyEntries);
                    var idxKey1 = string.Join(TableStorageContext.KeySeparator, objs.Select((obj, idx) => ConvertToString(
                        IgnorePartitionKeyPropertyRemovables.ContainsKey(propNames1[idx]) ?
                            TypeConvert(IgnorePartitionKeyPropertyRemovables[propNames1[idx]], obj) : obj, GetEncoder(propNames1[idx]))));

                    if (string.IsNullOrEmpty(partitionPrefix))
                        return (idxKey, idxKey1);
                    return ($"{partitionPrefix}{TableStorageContext.KeySeparator}{idxKey}", idxKey1);
                },
                CopyAllProperties = CopyAllProperties,
            });

            //Action<TEntityType, string> partitionAction = GetReverseActionFrom<IndexKeyType>(IndexKeyExpression);

            return this;
        }
        public Func<string, string> GetEncoder(string name)
        {
            if (PropertiesToEncode.ContainsKey(name))
                return PropertiesToEncode[name].Encoder;
            return null;
        }

        public EntityTypeConfiguration<TEntityType> UseEncodingFor<T>(Expression<Func<TEntityType, T>> expression, Func<string, string> encoder, Func<string, string> decoder)
        {
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;
                PropertiesToEncode.Add(memberEx.Member.Name, new EncoderPair { Decoder = decoder, Encoder = encoder });
            }


            return this;
        }
        public EntityTypeConfiguration<TEntityType> UseBase64EncodingFor<T>(Expression<Func<TEntityType, T>> expression)
        {
            return UseEncodingFor<T>(expression, StringExtensions.Base64Encode, StringExtensions.Base64Decode);
            //if (expression.Body is MemberExpression)
            //{
            //    var memberEx = expression.Body as MemberExpression;
            //    PropertiesToEncode.Add(memberEx.Member.Name);
            //}


            //  return this;
        }
        public EntityTypeConfiguration<TEntityType> WithEnumProperties()
        {
            //      var type = typeof(PropertyConfiguration<>);
            //var fact = this.GetType().GetMethod("PropertyConfigurationFactory", BindingFlags.Static | BindingFlags.NonPublic);
            var fact = this.GetType().GetTypeInfo().GetDeclaredMethod("PropertyConfigurationFactoryV2");


            foreach (var prop in typeof(TEntityType).GetRuntimeProperties().Where(p => p.PropertyType.GetTypeInfo().IsEnum))
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
            foreach (var prop in typeof(TEntityType).GetRuntimeProperties().Where(p => p.PropertyType == typeof(Uri)))
            {
                this.Properties.Add(new PropertyConfiguration<TEntityType,Uri>
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
            return new PropertyConfiguration<TEntityType, T>()
                {
                    Deserializer = (Func<EntityProperty, Task<T>>)((property) => Task.FromResult(JsonConvert.DeserializeObject<T>(property.StringValue))),
                    Serializer = (Func<T, Task<EntityProperty>>)(p => Task.FromResult(new EntityProperty(JsonConvert.SerializeObject(p))))
                };
        }
        private static PropertyConfiguration PropertyConfigurationFactoryV2<T>()
        {
            return new PropertyConfiguration<TEntityType, T>()
            {
                Deserializer = (Func<EntityProperty, Task<T>>)(Deserializer<T>),
                Serializer = (Func<T, Task<EntityProperty>>)(Serializer<T>)
            };
        }

        private static Task<T> Deserializer<T>(EntityProperty property)
        {
            var obj = property.PropertyAsObject;
            if(obj is int intValue)
                return Task.FromResult((T)obj);

           if(obj is byte[] bytes)
                return Task.FromResult(JsonConvert.DeserializeObject<T>(Encoding.UTF8.GetString(bytes)));

            return   Task.FromResult(JsonConvert.DeserializeObject<T>(property.StringValue));
        }

        private static Task<EntityProperty> Serializer<T>(T obj)
        {
            if (typeof(T).IsEnum)
                return Task.FromResult(new EntityProperty((int)(object)obj));
             

            return Task.FromResult(new EntityProperty(Encoding.UTF8.GetBytes( JsonConvert.SerializeObject(obj))));
        }

        public EntityTypeConfiguration<TEntityType> WithReversionTracking(Func<ITableRepository<TEntityType>,TEntityType,ITableQuery> headFilter)
        {
            this.ReversionTracking.Enabled = true;
            ReversionTracking.HeadWhereFilter = headFilter;
          //  ReversionTracking.OnEntityChanged = onEntityChanged;
 
            return this;
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
                if (deserializer == null && serializer == null)
                {
                    var fact = this.GetType().GetTypeInfo().GetDeclaredMethod("PropertyConfigurationFactoryV2");
                    var prop = memberEx.Member as PropertyInfo;
                    PropertyConfiguration config = (PropertyConfiguration)fact.MakeGenericMethod(prop.PropertyType).Invoke(null, null);
                    config.PropertyInfo = prop;
                    config.Deserializer = deserializer ?? config.Deserializer;
                    config.Serializer = serializer ?? config.Serializer;
                    this.Properties.Add(config);
                }
                else
                {
                    this.Properties.Add(new PropertyConfiguration<TEntityType, T>
                    {
                        PropertyInfo = memberEx.Member as PropertyInfo,
                        // EntityType = typeof(T),
                        // ParentEntityType = typeof(TEntityType),
                        Deserializer = deserializer ?? (p => Task.FromResult(JsonConvert.DeserializeObject<T>(p.StringValue))),
                        Serializer = serializer ?? (p => Task.FromResult(new EntityProperty(JsonConvert.SerializeObject(p)))),
                    });
                }
            }

            return this;

        }

        public EntityTypeConfiguration<TEntityType> WithPropertyOf<T>(
          Expression<Func<TEntityType, T>> expression,
          Func<IDictionary<string, EntityProperty>, Task<T>> composer,
          Func<T, Task<IDictionary<string, EntityProperty>>> decomposer)
        {
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;

                this.Properties.Add(new PropertyConfiguration<TEntityType,T>
                {
                    PropertyInfo = memberEx.Member as PropertyInfo,
                    IsComposite = true,
                    Composer = composer,
                    Decomposer = decomposer,

                });
            }

            return this;

        }
        public EntityTypeConfiguration<TEntityType> WithPropertyOf<T>(
        Expression<Func<TEntityType, T>> expression,
        Func<TEntityType,IDictionary<string, EntityProperty>, Task<T>> composer,
        Func<T, Task<IDictionary<string, EntityProperty>>> decomposer)
        {
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;

                this.Properties.Add(new PropertyConfiguration<TEntityType,T>
                {
                    PropertyInfo = memberEx.Member as PropertyInfo,
                    IsComposite = true,
                    IsEntityComposed = true,
                    Composer = composer,
                    Decomposer = decomposer,

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
                    (ctx) => Factory.RepositoryFactory<T>(_loggerFactory,_container,ctx);

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
        public EntityTypeConfiguration<TEntityType> ToTable(Func<ITableStorageContext,string> tableName)
        {
            this.TableName = tableName;
            foreach (var index in Indexes.Where(i => null == (i.Value.TableName)))
                index.Value.TableName = (ctx)=> this.TableName(ctx) + index.Value.TableNamePostFix;

            return this;
        }

        public EntityTypeConfiguration<TEntityType> ToTable<TContext>(Func<TContext, string> tableName) where TContext : class,ITableStorageContext
        {
            return ToTable((ctx) => tableName(ctx as TContext));
        }

        public EntityTypeConfiguration<TEntityType> ToTable(string tableName)
        {
            this.TableName = (ctx)=>tableName;
            foreach (var index in Indexes.Where(i => null == (i.Value.TableName)))
                index.Value.TableName = (ctx) => this.TableName(ctx) + index.Value.TableNamePostFix;

            return this;
        }

        public Func<TEntityType, string> ConvertToStringKey<T>(Expression<Func<TEntityType, T>> expression, IDictionary<string,object> ignores, out string key, Queue<LengthPadding?> lenghts = null, string prefix = null)
        {
            var func = expression.Compile();
            if (expression.Body is MemberExpression)
            {
                var memberEx = expression.Body as MemberExpression;
                var propertyName = memberEx.Member.Name;
                key = propertyName;
                var length = (lenghts == null || lenghts.Count == 0) ? (LengthPadding?)null : lenghts.Dequeue();

                //var ignores = keyType == "PartitionKey" ? IgnorePartitionKeyPropertyRemovables : IgnoreRowKeyPropertyRemovables;

                string GetKey(TEntityType o)
                {

                    var str= ConvertToString(ignores.ContainsKey(propertyName) ?
                        TypeConvert(ignores[propertyName], func(o)) : func(o) as object,
                        GetEncoder(propertyName), length);

                    if (!string.IsNullOrEmpty(prefix))
                    {
                        return $"{prefix}{TableStorageContext.KeySeparator}{str}";
                    }

                    return str;
                }


                return GetKey;
            }
            else if (expression.Body is NewExpression)
            {
                Logger.LogDebug("Using NewExpressino for KeyMapping");
                var newEx = expression.Body as NewExpression;
                key = string.Join(TableStorageContext.KeySeparator, newEx.Members.Select(m => m.Name));
                var properties = newEx.Members.OfType<PropertyInfo>().ToArray();
                List<LengthPadding?> lenghtsList = new List<LengthPadding?>();
                int mi = properties.Length;
                while (mi-- > 0)
                    lenghtsList.Add(lenghts == null || lenghts.Count == 0 ? (LengthPadding?)null : lenghts.Dequeue());



                string GetKey(TEntityType o)
                {
                    
                    if (o == null)
                        throw new ArgumentNullException("Object cannot be null");

                    var obj = func(o);

                   

                    var objs = properties.Select((p, i) => ConvertToString(ignores.ContainsKey(p.Name) ? TypeConvert(ignores[p.Name],p.GetValue(obj)) :  p.GetValue(obj),                        
                        GetEncoder(properties[i].Name), lenghtsList[i])
                    ).ToArray();


                    //If any nulls, then the key becomes a enmpty string.
                    //   if (objs.Any(p => p == null))
                    //       return "";
                    if (!string.IsNullOrEmpty(prefix))
                    {
                        return $"{prefix}{TableStorageContext.KeySeparator}{string.Join(TableStorageContext.KeySeparator, objs.Select(t => t == null ? "" : t))}";
                    }
                    return string.Join(TableStorageContext.KeySeparator, objs.Select(t => t == null ? "" : t));

                };
                return GetKey;
            }
            else
            {
                key = "";
            }
            return (a)=> func(a).ToString();
           // return (a) => "";

        }
       
        private string TypeConvert(object p1, object p2)
        {
         
           return (string) ((Delegate)p1).DynamicInvoke(p2);
        }
        private static string ConvertToString(object obj, Func<string, string> encoder = null, LengthPadding? fixedLength = null)
        {
            if (obj == null)
                return null;

            if (fixedLength.HasValue)
            {
                // if (obj.GetType() == typeof(int))
                //     return ((int)obj).ToString("D" + fixedLength.Value.Lenght);
                // if (obj.GetType() == typeof(string))
                if (fixedLength.Value.Direction == PaddingDirection.Left)
                    return (obj.ToString()).PadLeft(fixedLength.Value.Length, TableStorageContext.KeySeparator.First());
                else
                    return (obj.ToString()).PadRight(fixedLength.Value.Length, TableStorageContext.KeySeparator.First());

            }
            var str = obj.ToString();
            if (encoder != null)
                return encoder(str);// str.Base64Encode();
            return str;
        }
        public static bool IsStringConvertable(Type type)
        {
            return type.GetTypeInfo().IsPrimitive || type == typeof(string) || type == typeof(Guid);
        }

        Func<ITableStorageContext, string, EntityProperty, IDictionary<string, EntityProperty>, Task<SizeReductionResult>> reducer;

        internal Task<SizeReductionResult> SizeReducerAsync(ITableStorageContext context, string key, EntityProperty value, IDictionary<string, EntityProperty> properties)
        {
            if (reducer == null)
            {
                Logger.LogWarning("SizeReducer was called for {0} but no reducer was set.", key);
                return Task.FromResult(new SizeReductionResult { Key = key, Value = value });
            }
            return reducer(context, key, value, properties);
        }
        public EntityTypeConfiguration<TEntityType> SetColumnSizeReducer(Func<ITableStorageContext, string, EntityProperty, IDictionary<string, EntityProperty>, Task<SizeReductionResult>> reducer)
        {
            this.reducer = reducer;
            return this;
        }
    }
    public class SizeReductionResult
    {
        public string Key { get; set; }
        public EntityProperty Value { get; set; }
    }

}
