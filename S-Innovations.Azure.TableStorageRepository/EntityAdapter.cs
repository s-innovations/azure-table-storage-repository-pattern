using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

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
            if(Config.CopyAllProperties && Ref is IEntityAdapter)
            {
                var adapter = Ref as IEntityAdapter;
                var baseProps = base.WriteEntity(operationContext);
                var props= baseProps.Concat(adapter.WrittenProperties.Where(k => !(k.Key == "PartitionKey" || k.Key == "RowKey")))
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
        IDictionary<string,EntityProperty> WrittenProperties { get; }
    }
    public interface IEntityAdapter<T> : IEntityAdapter
    {
        T InnerObject { get; }
    }
    public class EntityAdapter<T> : IEntityAdapter, ITableEntity
    {
        ITableStorageContext context;
        EntityTypeConfiguration<T> config;
        public EntityAdapter()
        {
            // If you would like to work with objects that do not have a default Ctor you can use (T)Activator.CreateInstance(typeof(T));
         //   this.InnerObject = new T();
            this.config = EntityTypeConfigurationsContainer.Entity<T>();
        }

        internal EntityAdapter(ITableStorageContext context , EntityTypeConfiguration<T> config, T innerObject, DateTimeOffset? timestamp = null, string Etag = null)
        {
            this.context = context;
            this.InnerObject = innerObject;
            if (timestamp.HasValue)
                this.Timestamp = timestamp.Value;
            this.ETag = Etag;
            this.config = config;
        }
        public object GetInnerObject() { return InnerObject; }
        public T InnerObject { get; set; }


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

        public virtual void ReadEntity(IDictionary<string, EntityProperty> properties, OperationContext operationContext)
        {
            //Create the Entity Object Type
            this.InnerObject = config.CreateEntity(properties);
            //Read all default supported types from table entity
            TableEntity.ReadUserObject(this.InnerObject, properties, operationContext);
            
            //Read all custom properties configured.
            var tasks = new List<Task>();
            foreach (var propInfo in config.Properties)
            {
                if (properties.ContainsKey(propInfo.PropertyInfo.Name))
                {
                    var prop = properties[propInfo.PropertyInfo.Name];
                    tasks.Add(propInfo.SetPropertyAsync(this.InnerObject, prop));
                }else if (propInfo.IsComposite)
                {
                    tasks.Add(propInfo.SetCompositePropertyAsync(this.InnerObject, properties.Where(k=>k.Key.StartsWith(propInfo.PropertyInfo.Name)).ToDictionary(k=>k.Key.Substring(propInfo.PropertyInfo.Name.Length+2), v=>v.Value)));
                }
            }
            Task.WaitAll(tasks.ToArray());

            //Set the properties
            Properties = properties;
            
            //Reverse Part and RowKeys  to its InnerObject properties and add them to the property dict also.
            config.ReverseKeyMapping(this);


        }

        public virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {

            if (this.InnerObject == null){                
                return new Dictionary<string, EntityProperty>();
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
                foreach (var prop in propInfo.Properties) {
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
            foreach (var key in keyprops.Where(n=>!config.IgnoreKeyPropertyRemovables.ContainsKey(n)))
                WrittenProperties.Remove(key);


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

        public virtual IDictionary<string, EntityProperty> Properties { get; set; }

        public IDictionary<string, EntityProperty> WrittenProperties { get; set; }
    }
}
