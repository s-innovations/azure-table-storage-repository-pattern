using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.DataInitializers
{
    public class CreateTablesIfNotExists<TableContext> : Initializer<TableContext> where TableContext : ITableStorageContext
    {
        private Type[] ignored;
        public CreateTablesIfNotExists(params Type[] ignored)
        {
            this.ignored = ignored;
        }
        public void Initialize(ITableStorageContext context, TableStorageModelBuilder modelbuilder)
        {
           
            foreach (var table in modelbuilder.entities.Where(t=>!ignored.Any(tt=>t==tt)))
            {
                var configuration = EntityTypeConfigurationsContainer.Configurations[table];
                context.GetTable(configuration.TableName).CreateIfNotExists();
                var tasks = configuration.Indexes.Select(index => context.GetTable(index.Value.TableName ?? configuration.TableName + "Index").CreateIfNotExistsAsync()).ToArray();
                Task.WaitAll(tasks);
                    
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
