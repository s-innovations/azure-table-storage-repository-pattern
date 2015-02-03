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

       
        public static EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {


            if (!Configurations.ContainsKey(typeof(TEntityType)))
            {
                Configurations.TryAdd(typeof(TEntityType), new EntityTypeConfiguration<TEntityType>());
                foreach (var value in ModelBuilders.Values.Where(v=>v.Value.Entities == null))
                    value.Value.Builder();
            }
            return (EntityTypeConfiguration<TEntityType>)Configurations[typeof(TEntityType)];
          

            throw new InvalidOperationException("Something vent wrong");
        }
    }

    public class TableStorageModelBuilder
    {



        internal List<Type> Entities;
        
        public TableStorageModelBuilder()
        {
           
           
        }
        public EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {
            if (Entities == null)
                Entities = new List<Type>();
           
            Entities.Add(typeof(TEntityType));
            return EntityTypeConfigurationsContainer.Entity<TEntityType>();           
        }

        public Action Builder { get; set; }
    }
}
