using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Base
{
    internal static class TypeSystem
    {
        private static readonly Type StringType = typeof(string);
        private static readonly Type ObjectType = typeof(object);
        private static readonly Type EnumerableType = typeof(IEnumerable<>);

        internal static Type GetElementType(Type seqType)
        {
            Type ienum = FindIEnumerable(seqType);
            if (ienum == null) return seqType;
            return ienum.GetTypeInfo().GenericTypeParameters[0];
        }

        private static Type FindIEnumerable(Type seqType)
        {
            if (seqType == null || seqType == StringType)
            {
                return null;
            }

            if (seqType.IsArray)
            {
                return EnumerableType.MakeGenericType(seqType.GetElementType());
            }

            if (seqType.GetTypeInfo().IsGenericType)
            {
                foreach (Type arg in seqType.GetTypeInfo().GenericTypeParameters)
                {
                    Type ienum = EnumerableType.MakeGenericType(arg);
                    if (ienum.GetTypeInfo().IsAssignableFrom(seqType.GetTypeInfo()))
                    {
                        return ienum;
                    }
                }
            }

            Type[] ifaces = seqType.GetTypeInfo().ImplementedInterfaces.ToArray();
            if (ifaces.Length > 0)
            {
                foreach (Type iface in ifaces)
                {
                    Type ienum = FindIEnumerable(iface);
                    if (ienum != null)
                    {
                        return ienum;
                    }
                }
            }

            if (seqType.GetTypeInfo().BaseType != null && seqType.GetTypeInfo().BaseType != ObjectType)
            {
                return FindIEnumerable(seqType.GetTypeInfo().BaseType);
            }

            return null;
        }
    }
}
