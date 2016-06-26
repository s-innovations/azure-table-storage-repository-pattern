using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;
using Microsoft.WindowsAzure.Storage.Table;

namespace SInnovations.Azure.TableStorageRepository.PropertyEncoders
{
    public static class TagsProperty
    {
        public static Task<IDictionary<string, EntityProperty>> TagDecomposer(IDictionary<string, string> tags)
        {
            return Task.FromResult<IDictionary<string, EntityProperty>>(tags.ToDictionary(k => k.Key, v => new EntityProperty(v.Value)));
        }

        public static Task<IDictionary<string, string>> TagComposer(IDictionary<string, EntityProperty> tags)
        {
            return Task.FromResult<IDictionary<string, string>>(tags.ToDictionary(k => k.Key, v => v.Value.StringValue));
        }

        public static EntityTypeConfiguration<TEntityType> WithTagsProperty<TEntityType>(this EntityTypeConfiguration<TEntityType> config, Expression<Func<TEntityType, IDictionary<string,string>>> expression)
        {
            return config.WithPropertyOf(expression, TagComposer, TagDecomposer);
        }
    }
}
