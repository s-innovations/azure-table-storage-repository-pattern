using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Methods
{
    /// <summary>
    ///     LINQ Take method translator.
    /// </summary>
    internal sealed class TakeTranslator : IMethodTranslator
    {
        private const string MethodName = "Take";

        public string Name
        {
            get { return MethodName; }
        }

        public void Translate(MethodCallExpression method, ITranslationResult result)
        {
            if (method.Method.Name != MethodName || method.Arguments.Count != 2)
            {
                var message = string.Format("Resources.TranslatorMemberNotSupported {0}", method.NodeType);
                throw new ArgumentOutOfRangeException("method", message);
            }

            var constant = (ConstantExpression)method.Arguments[1];

            result.AddTop((int)constant.Value);
        }
    }
}
