using Microsoft.Extensions.Logging;
using SInnovations.Azure.TableStorageRepository.DataInitializers;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading;

namespace SInnovations.Azure.TableStorageRepository
{
    public interface IEntityTypeConfigurationsContainer
    {
        EntityTypeConfiguration<TEntityType> Entity<TEntityType>();
        TableStorageModelBuilder GetModelBuilder(ITableStorageContext context, Action<TableStorageModelBuilder> OnModelCreating);
        EntityTypeConfiguration GetConfiguration(Type type);

    }
    public  class EntityTypeConfigurationsContainer : IEntityTypeConfigurationsContainer
    {
        internal ConcurrentDictionary<Type, EntityTypeConfiguration> Configurations = new ConcurrentDictionary<Type, EntityTypeConfiguration>();
        internal ConcurrentDictionary<Type, Lazy<TableStorageModelBuilder>> ModelBuilders = new ConcurrentDictionary<Type, Lazy<TableStorageModelBuilder>>();

        static object locker = new object();
        private readonly ILoggerFactory _factory;
        public EntityTypeConfigurationsContainer(ILoggerFactory factory)
        {
            this._factory = factory;
        }
        public EntityTypeConfiguration GetConfiguration(Type type)
        {
            return Configurations[type];
        }
        public TableStorageModelBuilder GetModelBuilder(ITableStorageContext context, Action<TableStorageModelBuilder> OnModelCreating)
        {
            return ModelBuilders.GetOrAdd(
                    context.GetType(), (key) => new Lazy<TableStorageModelBuilder>(() =>
                    {
                        var abuilder = new TableStorageModelBuilder(_factory,this);
                        abuilder.Builder = () =>
                        {
                            OnModelCreating(abuilder);
                            if (Table.inits.ContainsKey(key))
                            {
                                var init = Table.inits[key];
                                init.Initialize(context, abuilder);
                                Table.inits.Remove(key);
                            }
                        };
                        return abuilder;
                    }, LazyThreadSafetyMode.ExecutionAndPublication)).Value;
        }
        public EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {


            if (!Configurations.ContainsKey(typeof(TEntityType)))
            {
                lock (locker)
                {
                    if (!Configurations.ContainsKey(typeof(TEntityType)))
                    {
                        Configurations.TryAdd(typeof(TEntityType), new EntityTypeConfiguration<TEntityType>(_factory,this));
                        foreach (var value in ModelBuilders.Values.Where(v => v.Value.Entities == null))
                            value.Value.Builder();
                    }
                }
            }
            return (EntityTypeConfiguration<TEntityType>)Configurations[typeof(TEntityType)];
          

            throw new InvalidOperationException("Something vent wrong");
        }
    }

    public class TableStorageModelBuilder
    {


        private readonly ILoggerFactory logFactory;
        private readonly IEntityTypeConfigurationsContainer _container;
        internal List<Type> Entities;
        
        public TableStorageModelBuilder(ILoggerFactory logFactory, IEntityTypeConfigurationsContainer container)
        {
            this.logFactory = logFactory;
            this._container = container;
           
        }
        public EntityTypeConfiguration<TEntityType> Entity<TEntityType>()
        {
            if (Entities == null)
                Entities = new List<Type>();
           
            Entities.Add(typeof(TEntityType));
            return _container.Entity<TEntityType>();           
        }

        public Action Builder { get; set; }

       
    }
}
