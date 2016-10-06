using SInnovations.Azure.TableStorageRepository.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.DataInitializers
{
    public class DropTablesAndCreateIfExist<TableContext> : Initializer<TableContext> where TableContext : ITableStorageContext
    {
        static readonly ILog Logger = LogProvider.GetCurrentClassLogger();
        private Type[] ignored;
        public DropTablesAndCreateIfExist(params Type[] ignored)
        {
            this.ignored = ignored;
        }
        public void Initialize(ITableStorageContext context, TableStorageModelBuilder modelbuilder)
        {
           
            foreach (var tableType in modelbuilder.Entities.Where(t=>!ignored.Any(tt=>t==tt)))
            {
                var configuration = EntityTypeConfigurationsContainer.Configurations[tableType];
                //DROP
                var table = context.GetTable(configuration.TableName());
                {
                    table.DeleteIfExists();
                    var tasks = configuration.Indexes.Select(index => context.GetTable(index.Value.TableName?.Invoke() ?? configuration.TableName() + index.Value.TableNamePostFix).DeleteIfExistsAsync()).ToArray();
                    Task.WaitAll(tasks);
                }

                //Now wait for it to be deleted.
                while(true)
                {
                    try
                    {
                        context.GetTable(configuration.TableName()).CreateIfNotExists();
                        var tasks = configuration.Indexes.Select(index => context.GetTable(index.Value.TableName?.Invoke() ?? configuration.TableName() + index.Value.TableNamePostFix).CreateIfNotExistsAsync()).ToArray();
                        Task.WaitAll(tasks);
                        
                        Logger.Info("Creating was successfull");
                        break;
                    }catch(Exception ex)
                    {
                        Logger.InfoException("Creating caused exception, retrying", ex);
                        Task.Delay(5000).Wait();
                    }

                }

            }

        }
    }
    public class CreateTablesIfNotExists<TableContext> : Initializer<TableContext> where TableContext : ITableStorageContext
    {
        private Type[] ignored;
        public CreateTablesIfNotExists(params Type[] ignored)
        {
            this.ignored = ignored;
        }
        public void Initialize(ITableStorageContext context, TableStorageModelBuilder modelbuilder)
        {
           
            foreach (var table in modelbuilder.Entities.Where(t=>!ignored.Any(tt=>t==tt)))
            {
                var configuration = EntityTypeConfigurationsContainer.Configurations[table];
                context.GetTable(configuration.TableName()).CreateIfNotExists();
                var tasks = configuration.Indexes.Select(index => context.GetTable(index.Value.TableName?.Invoke() ?? configuration.TableName() + index.Value.TableNamePostFix).CreateIfNotExistsAsync()).ToArray();
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
