using SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Infrastructure;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Methods
{
    internal abstract class MethodTranslatorBase : IMethodTranslator
    {
        private readonly string _methodName;
        //private readonly IDictionary<string, string> _nameChanges;
        EntityTypeConfiguration _configuration;

        protected MethodTranslatorBase(EntityTypeConfiguration configuration, string methodName)
        {
            //_nameChanges = nameChanges;
            _configuration = configuration;
            _methodName = methodName;
        }

        public string Name
        {
            get { return _methodName; }
        }

        public virtual void Translate(MethodCallExpression method, ITranslationResult result)
        {
            if (method.Method.Name != _methodName)
            {
                string message = string.Format("Resources.TranslatorMethodNotSupported {0}", method.Method.Name);
                throw new ArgumentOutOfRangeException("method", message);
            }

            var expressionTranslator = new ExpressionTranslator(_configuration);

            MethodCallExpression targetMethod = method;

            if (method.Arguments.Count == 1 && method.Arguments[0].NodeType == ExpressionType.Call)
            {
                targetMethod = (MethodCallExpression)method.Arguments[0];
            }

            expressionTranslator.Translate(result, targetMethod);
            expressionTranslator.AddPostProcessing(method);
        }
    }
}
