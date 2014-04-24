﻿using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.DataInitializers
{
    public class CreateTablesIfNotExists<TableContext> : Initializer<TableContext> where TableContext : ITableStorageContext
    {

        public void Initialize(ITableStorageContext context, TableStorageModelBuilder modelbuilder)
        {
            foreach (var table in modelbuilder.entities)
            {
                context.GetTable(EntityTypeConfigurationsContainer.Configurations[table].TableName).CreateIfNotExists();
            }

        }
    }
    public interface Initializer
    {
       

        void Initialize(ITableStorageContext tableStorageContext, TableStorageModelBuilder modelbuilder);
    }
    public interface Initializer<T> :Initializer
    {

    }
    public static class Table
    {
        internal static Dictionary<Type, Initializer> inits = new Dictionary<Type, Initializer>();
        public static void SetInitializer<T>(Initializer<T> initializer)
        {
            inits[typeof(T)] = initializer;
        }
    }
}
