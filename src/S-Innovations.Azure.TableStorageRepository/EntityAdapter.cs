using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Collections;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using Microsoft.Extensions.Logging;

namespace SInnovations.Azure.TableStorageRepository
{
    public class IndexEntity : TableEntity
    {
        public string RefRowKey { get; set; }
        public string RefPartitionKey { get; set; }

        [IgnoreProperty]
        public IndexConfiguration Config { get; set; }

        [IgnoreProperty]
        public ITableEntity Ref { get; set; }

        public override void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            base.ReadEntity(properties, operationContext);
        }
        public override IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            if (Config.CopyAllProperties && Ref is IEntityAdapter)
            {
                var adapter = Ref as IEntityAdapter;
                var baseProps = base.WriteEntity(operationContext);
                var props = baseProps
                    .Concat(adapter.WrittenProperties.Where(k => !(k.Key == "PartitionKey" || k.Key == "RowKey")))
                    .Concat(adapter.RemovedProperties ?? Enumerable.Empty<KeyValuePair<string,EntityProperty>>())
                    .ToDictionary(k => k.Key, v => v.Value);
                return props;
            }

            return base.WriteEntity(operationContext);
        }

    }
    public interface IEntityAdapter
    {
        object GetInnerObject();
        IDictionary<string, EntityProperty> Properties { get; }
        IDictionary<string, EntityProperty> WrittenProperties { get; }
        IDictionary<string, EntityProperty> RemovedProperties { get; }

        Task<TTableEntity> MakeReversionCloneAsync<TTableEntity>(TTableEntity old) where TTableEntity : class, ITableEntity;
    }
    public interface IEntityAdapter<T> : IEntityAdapter
    {
        T InnerObject { get; }
    }
    public class EntityAdapter<TEntity> : IEntityAdapter, ITableEntity
    {
        public delegate Task<bool> OnEntityChanged(TEntity current, TEntity old, IDictionary<string, EntityProperty> currentProps, IDictionary<string, EntityProperty> oldProps);

        OnEntityChanged onEntityCHanged;

        ITableStorageContext context;
        EntityTypeConfiguration<TEntity> config;
        public EntityAdapter()
        {
            // If you would like to work with objects that do not have a default Ctor you can use (T)Activator.CreateInstance(typeof(T));
            //   this.InnerObject = new T();
           // this.config = EntityTypeConfigurationsContainer.Entity<TEntity>();
        }

        internal EntityAdapter(ITableStorageContext context, EntityTypeConfiguration<TEntity> config, TEntity innerObject, DateTimeOffset? timestamp = null, string Etag = null)
        {
            this.context = context;
            this.InnerObject = innerObject;
            if (timestamp.HasValue)
                this.Timestamp = timestamp.Value;
            this.ETag = Etag;
            this.config = config;
        }

        internal EntityAdapter(ITableStorageContext context, EntityTypeConfiguration<TEntity> config, TEntity innerObject, OnEntityChanged onEntityCHanged, DateTimeOffset? timestamp = null, string Etag = null)
            : this(context,config,innerObject,timestamp,Etag)
        {
            this.onEntityCHanged = onEntityCHanged;
        }

        public EntityAdapter<TEntity> ReversionBase { get; private set; }

        public object GetInnerObject() { return InnerObject; }
        public TEntity InnerObject { get; set; }


        public EntityState State { get; set; }
        /// <summary>
        /// Gets or sets the entity's partition key.
        /// </summary>
        /// <value>The partition key of the entity.</value>
        public string PartitionKey { get; set; }

        /// <summary>
        /// Gets or sets the entity's row key.
        /// </summary>
        /// <value>The row key of the entity.</value>
        public string RowKey { get; set; }

        /// <summary>
        /// Gets or sets the entity's timestamp.
        /// </summary>
        /// <value>The timestamp of the entity.</value>
        public DateTimeOffset Timestamp { get; set; }

        /// <summary>
        /// Gets or sets the entity's current ETag. Set this value to '*' in order to blindly overwrite an entity as part of an update operation.
        /// </summary>
        /// <value>The ETag of the entity.</value>
        public string ETag { get; set; }

        private OperationContext operationContext;
        public virtual async Task PostReadEntityAsync(EntityTypeConfiguration<TEntity> config)
        {
            this.config = config;
            //Create the Entity Object Type
            this.InnerObject = config.CreateEntity(Properties);
            //Read all default supported types from table entity
            TableEntity.ReadUserObject(this.InnerObject, Properties, operationContext);

            //Read all custom properties configured.
            var tasks = new List<Task>();
            foreach (var propInfo in config.Properties)
            {
                if (Properties.ContainsKey(propInfo.PropertyInfo.Name))
                {
                    var prop = Properties[propInfo.PropertyInfo.Name];
                    tasks.Add(propInfo.SetPropertyAsync(this.InnerObject, prop));
                }
                else if (propInfo.IsComposite)
                {
                    tasks.Add(propInfo.SetCompositePropertyAsync(this.InnerObject, Properties.Where(k => k.Key.StartsWith(propInfo.PropertyInfo.Name)).ToDictionary(k => k.Key.Substring(propInfo.PropertyInfo.Name.Length + 2), v => v.Value)));
                }
            }

            await Task.WhenAll(tasks.ToArray());
            //Reverse Part and RowKeys  to its InnerObject properties and add them to the property dict also.
            config.ReverseKeyMapping(this);
        }
        public virtual void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {

            //Set the properties
            Properties = properties;
            this.operationContext = operationContext;

        }

        public virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {

            if (this.InnerObject == null)
            {
                return new Dictionary<string, EntityProperty>();
            }

            if (WrittenProperties != null)
            {
                return WrittenProperties;
            }


            WrittenProperties = TableEntity.WriteUserObject(this.InnerObject, operationContext);

            var all = Task.WhenAll(config.Properties
            .Select(async propInfo =>
                new
                {
                    Key = propInfo.PropertyInfo.Name,
                    Property = propInfo.IsComposite ? null : await propInfo.GetPropertyAsync(this.InnerObject),
                    Properties = propInfo.IsComposite ? await propInfo.GetPropertiesAsync(this.InnerObject) : null
                })).Result;

            foreach (var propInfo in all.Where(p => p.Property != null))
            {
                WrittenProperties.Add(propInfo.Key, propInfo.Property);
            }

            foreach (var propInfo in all.Where(p => p.Properties != null))
            {
                foreach (var prop in propInfo.Properties)
                {
                    WrittenProperties.Add($"{propInfo.Key}__{prop.Key}", prop.Value);
                }
            }

            if (Properties != null)
                foreach (var propInfo in Properties)
                {
                    if (!WrittenProperties.ContainsKey(propInfo.Key))
                        WrittenProperties.Add(propInfo.Key, propInfo.Value);
                }

            //Remove those parts that is used for partition/row keys. (redundant data)
            var keyprops = config.KeyMappings.Keys.SelectMany(k => k.Split(new string[] { TableStorageContext.KeySeparator }, StringSplitOptions.RemoveEmptyEntries));
            foreach (var key in keyprops.Where(n => !config.IgnoreKeyPropertyRemovables.ContainsKey(n)))
            {
                if (RemovedProperties == null)
                    RemovedProperties = new Dictionary<string, EntityProperty>();
                RemovedProperties.Add(key, WrittenProperties[key]);
                WrittenProperties.Remove(key);
            }


            EnsureSizeLimites(WrittenProperties);




            return WrittenProperties;
        }

        private void EnsureSizeLimites(IDictionary<string, EntityProperty> properties)
        {
            //Now Ensure Sizes
            List<Task<SizeReductionResult>> tasks = new List<Task<SizeReductionResult>>();
            foreach (var key in properties.Keys)
            {
                var value = properties[key];
                if (value.PropertyType == EdmType.Binary)
                {
                    if (value.BinaryValue.Length > 64000)
                    {

                        tasks.Add(config.SizeReducerAsync(context, key, value, properties));
                    }
                }
            }

            Task.WaitAll(tasks.ToArray());
            foreach (var task in tasks.Select(t => t.Result))
            {
                properties[task.Key] = task.Value;
            }
        }

        public async Task<TTableEntity> MakeReversionCloneAsync<TTableEntity>(TTableEntity old) where TTableEntity : class, ITableEntity
        {
            var rowKey = RowKey;
               RowKey = "";
            var copy = new EntityAdapter<TEntity>(context, config, InnerObject)
            {
                ReversionBase = old as EntityAdapter<TEntity>,
                PartitionKey = PartitionKey.Replace("HEAD", "REV"),
                RowKey = rowKey,
               
            };

            if (copy.ReversionBase != null)
            {
                var oldProps = copy.ReversionBase.Properties;
                var newProps = copy.WriteEntity(null);
                foreach (var key in oldProps.Keys)
                {
                    if (newProps.ContainsKey(key))
                    {
                        var oldValue = oldProps[key];
                        var newValue = newProps[key];
                        if (oldValue.PropertyType == newValue.PropertyType)
                        {
                            if (oldValue.PropertyType == EdmType.Binary)
                            {
                                if(StructuralComparisons.StructuralEqualityComparer.Equals(oldValue.BinaryValue, newValue.BinaryValue))
                                {
                                    newProps.Remove(key);
                                }
                            }
                            else
                            {
                                if (oldValue.PropertyAsObject.Equals(newValue.PropertyAsObject))
                                {
                                    newProps.Remove(key);
                                }
                            } 

                        }
                    }
                }
                foreach( var key in config.IgnoreKeyPropertyRemovables.Keys)
                {
                    if(newProps.ContainsKey(key))
                        newProps.Remove(key);
                }

                if (!newProps.Any())
                {
                    return null;
                }else
                {
                    if(onEntityCHanged != null)
                    {
                        if(!await onEntityCHanged(copy.InnerObject, copy.ReversionBase.InnerObject, newProps, oldProps))
                        {
                            return null;
                        }
                    }

                    //if (config.ReversionTracking.OnEntityChanged != null)
                    //{//T innerObject1, T innerObject2, IDictionary<string, EntityProperty> newProps
                    //    var func = config.ReversionTracking.OnEntityChanged as Func<T, T, IDictionary<string, EntityProperty>, IDictionary<string, EntityProperty>, bool>;
                    //    if(!func(copy.InnerObject, copy.ReversionBase.InnerObject, newProps, oldProps))
                    //    {
                    //        return null;
                    //    }
                    //}
                }
            }

           

            return copy as TTableEntity;
        }

        public virtual IDictionary<string, EntityProperty> Properties { get; set; }

        public IDictionary<string, EntityProperty> WrittenProperties { get; set; }
        public IDictionary<string, EntityProperty> RemovedProperties { get; set; }

    }
}
