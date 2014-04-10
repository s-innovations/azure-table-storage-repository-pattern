using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository
{
    public struct KeysMapper<TEntity>
    {
        public Func<TEntity, String> PartitionKeyMapper { get; set; }
        public Func<TEntity, String> RowKeyMapper { get; set; }
    }
    public class EntityTypeConfiguration
    {
        public object KeyMapper { get; set; }
        public string TableName { get; protected set; }
    }
    public class EntityTypeConfiguration<TEntityType> : EntityTypeConfiguration
    {
        public EntityTypeConfiguration<TEntityType> HasKeys(Func<TEntityType, string> PartitionKeyExpression, Func<TEntityType, string> RowKeyExpression)
        {
            KeyMapper = new KeysMapper<TEntityType> { PartitionKeyMapper = PartitionKeyExpression, RowKeyMapper = RowKeyExpression };
            return this;
        }

        public EntityTypeConfiguration<TEntityType> ToTable(string tableName)
        {

            this.TableName = tableName;

            return this;
        }
    }
}
