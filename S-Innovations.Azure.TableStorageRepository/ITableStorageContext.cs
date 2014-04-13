using Microsoft.WindowsAzure.Storage.Table;
using SInnovations.Azure.TableStorageRepository.TableRepositories;
using System;
using System.Collections;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;
using System.Threading.Tasks.Dataflow;

namespace SInnovations.Azure.TableStorageRepository
{
    public interface ITableStorageContext: IDisposable
    {
        InsertionMode InsertionMode { get; set; }
        CloudTable GetTable(string name);
    }
    public enum EntityState
    {
        Added,
        Updated,
        Deleted,
        Unmodified,
    }
    public static class ITableEntityExtensions
    {
        public static EntityState GetState(this ITableEntity entity)
        {
            if (string.IsNullOrEmpty(entity.ETag))
                return EntityState.Added;
            else
                return EntityState.Updated;
        }
    }

    public struct EntityStateWrapper<T> where T : ITableEntity
    {
        public EntityState State { get; set; }
        public T Entity { get; set; }
    }

    
   

}
