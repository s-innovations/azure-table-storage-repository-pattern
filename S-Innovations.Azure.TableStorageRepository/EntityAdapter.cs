using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Table;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository
{
    public class IndexEntity : TableEntity
    {
        public string RefRowKey { get; set; }
        public string RefPartitionKey { get; set; }

        [IgnoreProperty]
        public IndexConfiguration Config { get; set; }
    }
    public interface EntityAdapter
    {
        object GetInnerObject();
    }
    public class EntityAdapter<T> : ITableEntity where T : new()
    {

        public EntityAdapter()
        {
            // If you would like to work with objects that do not have a default Ctor you can use (T)Activator.CreateInstance(typeof(T));
            this.InnerObject = new T();
        }

        public EntityAdapter(T innerObject,DateTimeOffset? timestamp=null, string Etag=null)
        {
            this.InnerObject = innerObject;
            if (timestamp.HasValue)
                this.Timestamp = timestamp.Value;
            this.ETag = Etag;
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
            TableEntity.ReadUserObject(this.InnerObject, properties, operationContext);
        }

        public virtual IDictionary<string, EntityProperty> WriteEntity(OperationContext operationContext)
        {
            return TableEntity.WriteUserObject(this.InnerObject, operationContext);
        }
    }
}
