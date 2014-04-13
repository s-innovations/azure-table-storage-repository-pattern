using SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Methods
{
    /// <summary>
    ///     LINQ Where method translator.
    /// </summary>
    internal sealed class WhereTranslator : IMethodTranslator
    {
        private const string MethodName = "Where";
        private readonly IDictionary<string, string> _nameChanges;

        public WhereTranslator(IDictionary<string, string> nameChanges)
        {
            _nameChanges = nameChanges;
        }

        public string Name
        {
            get { return MethodName; }
        }

        public void Translate(MethodCallExpression method, ITranslationResult result)
        {
            if (method.Method.Name != MethodName)
            {
                var message = string.Format("Resources.TranslatorMemberNotSupported {0}", method.NodeType);
                throw new ArgumentOutOfRangeException("method", message);
            }

            var expressionTranslator = new ExpressionTranslator(_nameChanges);
            expressionTranslator.Translate(result, method);
        }
    }
}
