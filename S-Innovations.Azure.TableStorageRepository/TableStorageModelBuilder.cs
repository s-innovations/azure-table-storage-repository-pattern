using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository
{
    public class TableStorageModelBuilder
    {
       
        public TableStorageModelBuilder()
        {

        }
        internal ConcurrentDictionary<Type, EntityTypeConfiguration> _configurations = new ConcurrentDictionary<Type, EntityTypeConfiguration>();
        public EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {

            if (!_configurations.ContainsKey(typeof(TEntityType)))
                _configurations.TryAdd(typeof(TEntityType), new EntityTypeConfiguration<TEntityType>(this));
            return (EntityTypeConfiguration<TEntityType>)_configurations[typeof(TEntityType)];
        }
    }
}
