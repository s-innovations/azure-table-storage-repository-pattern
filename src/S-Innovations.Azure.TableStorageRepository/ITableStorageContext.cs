﻿using Microsoft.Azure.Cosmos.Table;
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
        IRetryPolicy RetryPolicy { get; set; }
        InsertionMode InsertionMode { get; set; }
        CloudTable GetTable(string name);
        CloudStorageAccount StorageAccount { get; }

        int MaxDegreeOfParallelism { get; }
    }
    public enum EntityState
    {
        Added,
        Updated,
    //    UpdatedWithReversionTracking,
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
        public bool KeysLocked{get;set;}

      //  internal string PartitionKey { get { return State == EntityState.UpdatedWithReversionTracking ? Entity.PartitionKey.Replace("HEAD","REV") :  Entity.PartitionKey; } }
    }

    
   

}
