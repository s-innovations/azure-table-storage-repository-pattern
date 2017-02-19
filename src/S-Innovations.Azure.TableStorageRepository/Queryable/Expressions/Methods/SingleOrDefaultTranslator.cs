using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Methods
{
    /// <summary>
    ///     LINQ SingleOrDefault method translator.
    /// </summary>
    internal sealed class SingleOrDefaultTranslator : MethodTranslatorBase
    {
        public SingleOrDefaultTranslator(ILoggerFactory factory,EntityTypeConfiguration configuration)
            : base(factory,configuration, "SingleOrDefault")
        {
        }

        public override void Translate(MethodCallExpression method, ITranslationResult result)
        {
            base.Translate(method, result);
            result.AddTop(2);
        }
    }
}
