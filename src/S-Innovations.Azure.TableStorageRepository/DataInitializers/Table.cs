using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.DataInitializers
{
    public class DropTablesAndCreateIfExist<TableContext> : Initializer<TableContext> where TableContext : ITableStorageContext
    {
        private readonly ILogger Logger;
        private Type[] ignored;
        private readonly IEntityTypeConfigurationsContainer container;
        public DropTablesAndCreateIfExist(ILoggerFactory factory, IEntityTypeConfigurationsContainer container, params Type[] ignored)
        {
            this.ignored = ignored;
            this.Logger = factory.CreateLogger<DropTablesAndCreateIfExist<TableContext>>();
            this.container = container;
        }
        public void Initialize(ITableStorageContext context, TableStorageModelBuilder modelbuilder)
        {
           
            foreach (var tableType in modelbuilder.Entities.Where(t=>!ignored.Any(tt=>t==tt)))
            {
                var configuration = container.GetConfiguration(tableType);
                //DROP
                var table = context.GetTable(configuration.TableName(context));
                {
                    table.DeleteIfExistsAsync().GetAwaiter().GetResult();
                    var tasks = configuration.Indexes.Select(index => context.GetTable(index.Value.TableName?.Invoke(context) ?? configuration.TableName(context) + index.Value.TableNamePostFix).DeleteIfExistsAsync()).ToArray();
                    Task.WaitAll(tasks);
                }

                //Now wait for it to be deleted.
                while(true)
                {
                    try
                    {
                        context.GetTable(configuration.TableName(context)).CreateIfNotExistsAsync().GetAwaiter().GetResult();
                        var tasks = configuration.Indexes.Select(index => context.GetTable(index.Value.TableName?.Invoke(context) ?? configuration.TableName(context) + index.Value.TableNamePostFix).CreateIfNotExistsAsync()).ToArray();
                        Task.WaitAll(tasks);
                        
                        Logger.LogInformation("Creating was successfull");
                        break;
                    }catch(Exception ex)
                    {
                        Logger.LogInformation(new EventId(),ex,"Creating caused exception {ex}, retrying", ex);
                        Task.Delay(5000).Wait();
                    }

                }

            }

        }
    }
    public class CreateTablesIfNotExists<TableContext> : Initializer<TableContext> where TableContext : ITableStorageContext
    {
        private Type[] ignored;
        private readonly IEntityTypeConfigurationsContainer container;
        public CreateTablesIfNotExists(IEntityTypeConfigurationsContainer container, params Type[] ignored)
        {
            this.ignored = ignored;
            this.container = container;
        }
        public void Initialize(ITableStorageContext context, TableStorageModelBuilder modelbuilder)
        {
           
            foreach (var table in modelbuilder.Entities.Where(t=>!ignored.Any(tt=>t==tt)))
            {
                var configuration = container.GetConfiguration(table);
                var tableName = configuration.TableName(context);
                context.GetTable(tableName).CreateIfNotExistsAsync().GetAwaiter().GetResult();
                var tasks = configuration.Indexes.Select(index => context.GetTable(index.Value.TableName?.Invoke(context) ?? configuration.TableName(context) + index.Value.TableNamePostFix).CreateIfNotExistsAsync()).ToArray();
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
        public static void ClearInitializer<T>()
        {
            if(inits.ContainsKey(typeof(T)))
                inits.Remove(typeof(T));
        }
    }
}
