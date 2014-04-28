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
    }
    internal interface IEntityAdapter
    {
        object GetInnerObject();
        IDictionary<string, EntityProperty> Properties { get; }
    }
    internal interface IEntityAdapter<T> : IEntityAdapter
    {
        T InnerObject { get; }
    }
    public class EntityAdapter<T> : IEntityAdapter, ITableEntity
    {
        EntityTypeConfiguration<T> config;
        public EntityAdapter()
        {
            // If you would like to work with objects that do not have a default Ctor you can use (T)Activator.CreateInstance(typeof(T));
         //   this.InnerObject = new T();
            this.config = EntityTypeConfigurationsContainer.Entity<T>();
        }

        internal EntityAdapter(EntityTypeConfiguration<T> config, T innerObject, DateTimeOffset? timestamp = null, string Etag = null)
        {
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
                }
            }
            Task.WaitAll(tasks.ToArray());

            //Set the properties
            Properties = properties;
        }

        public virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {

            if (this.InnerObject == null){                
                return new Dictionary<string, EntityProperty>();
            }

            
            IDictionary<string, EntityProperty>  properties = TableEntity.WriteUserObject(this.InnerObject, operationContext);
            var all = Task.WhenAll(config.Properties
            .Select(async propInfo =>
                new
                {
                    Key = propInfo.PropertyInfo.Name,
                    Property = await propInfo.GetPropertyAsync(this.InnerObject)
                })).Result;

            foreach (var propInfo in all.Where(p => p.Property != null))
            {
                properties.Add(propInfo.Key, propInfo.Property);
            }
            foreach (var propInfo in Properties)
            {
                if (!properties.ContainsKey(propInfo.Key))
                    properties.Add(propInfo.Key, propInfo.Value);
            }

            return properties;
        }

        public virtual IDictionary<string, EntityProperty> Properties { get; set; }

    }
}
