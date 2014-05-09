using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository
{
    public static class EntityTypeConfigurationsContainer
    {
        static internal ConcurrentDictionary<Type, EntityTypeConfiguration> Configurations = new ConcurrentDictionary<Type, EntityTypeConfiguration>();
        static internal ConcurrentDictionary<Type, Lazy<TableStorageModelBuilder>> ModelBuilders = new ConcurrentDictionary<Type, Lazy<TableStorageModelBuilder>>(); 

        public static  EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {

            if (!Configurations.ContainsKey(typeof(TEntityType)))
                Configurations.TryAdd(typeof(TEntityType), new EntityTypeConfiguration<TEntityType>());
            return (EntityTypeConfiguration<TEntityType>)Configurations[typeof(TEntityType)];
        }
    }

    public class TableStorageModelBuilder
    {
        internal List<Type> entities;
        public TableStorageModelBuilder()
        {
            entities = new List<Type>();
        }
        public EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {
            entities.Add(typeof(TEntityType));
            return EntityTypeConfigurationsContainer.Entity<TEntityType>();           
        }
        
    }
}
