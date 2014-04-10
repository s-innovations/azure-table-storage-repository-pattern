using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository
{
    public class TableStorageModelBuilder
    {
        internal ConcurrentDictionary<Type, EntityTypeConfiguration> _configurations = new ConcurrentDictionary<Type, EntityTypeConfiguration>();
        public EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {
            var config = new EntityTypeConfiguration<TEntityType>();
            _configurations.AddOrUpdate(typeof(TEntityType), (type) => config, (type, old) => config);
            return config;
        }
    }
}
