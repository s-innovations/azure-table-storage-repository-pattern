using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Methods
{
    /// <summary>
    ///     LINQ FirstOrDefault method translator.
    /// </summary>
    internal sealed class FirstOrDefaultTranslator : MethodTranslatorBase
    {
        public FirstOrDefaultTranslator(EntityTypeConfiguration configuration)
            : base(configuration, "FirstOrDefault")
        {
        }

        public override void Translate(MethodCallExpression method, ITranslationResult result)
        {
            base.Translate(method, result);
            result.AddTop(1);
        }
    }
}
