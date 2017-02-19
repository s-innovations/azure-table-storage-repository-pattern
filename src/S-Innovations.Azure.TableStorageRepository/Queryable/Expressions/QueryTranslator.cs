using Microsoft.Extensions.Logging;
using SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Methods;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions
{
    /// <summary>
    ///     Manages translation of the LINQ expressions.
    /// </summary>
    internal class QueryTranslator : IQueryTranslator
    {
        private readonly IDictionary<string, IMethodTranslator> _methodTranslators;

        /// <summary>
        ///     Constructor.
        /// </summary>
        internal QueryTranslator(ILoggerFactory factory,EntityTypeConfiguration configuration)
            : this(GetTranslators(factory,configuration))
        {
        }

        /// <summary>
        ///     Constructor.
        /// </summary>
        /// <param name="methodTranslators">LINQ Expression methods translators.</param>
        internal QueryTranslator(IEnumerable<IMethodTranslator> methodTranslators)
        {
            _methodTranslators = methodTranslators.ToDictionary(translator => translator.Name);
        }

        /// <summary>
        ///     Translates a LINQ expression into collection of query segments.
        /// </summary>
        /// <param name="expression">LINQ expression.</param>
        /// <param name="result">Translation result.</param>
        /// <returns>Collection of query segments.</returns>
        public void Translate(Expression expression, ITranslationResult result)
        {
            if (expression.NodeType != ExpressionType.Call)
            {
                return;
            }

            var methodCall = (MethodCallExpression)expression;

            // Visit method
            VisitMethodCall(methodCall, result);

            // ReSharper disable ForCanBeConvertedToForeach

            // Visit arguments
            for (int i = 0; i < methodCall.Arguments.Count; i++)
                {
                Expression argument = methodCall.Arguments[i];
                if (argument.NodeType == ExpressionType.Call)
                {
                    Translate(argument, result);
                }
            }

            // ReSharper restore ForCanBeConvertedToForeach
        }

        private static IEnumerable<IMethodTranslator> GetTranslators(ILoggerFactory factory, EntityTypeConfiguration configuration)
        {
            return new List<IMethodTranslator>
                {
                    new WhereTranslator(factory,configuration),
                    new FirstTranslator(factory,configuration),
                    new FirstOrDefaultTranslator(factory,configuration),
                    new SingleTranslator(factory,configuration),
                    new SingleOrDefaultTranslator(factory,configuration),
                    new SelectTranslator(configuration.KeyMappings),
                    new TakeTranslator()
                };
        }

        private void VisitMethodCall(MethodCallExpression methodCall, ITranslationResult result)
        {
            if (methodCall.Method.DeclaringType != typeof(System.Linq.Queryable))
            {
                throw new NotSupportedException(string.Format("Resources.TranslatorMethodNotSupported {0}", methodCall.Method.Name));
            }

            // Get a method translator
            IMethodTranslator translator;

            if (!_methodTranslators.TryGetValue(methodCall.Method.Name, out translator))
            {
                string message = String.Format("Resources.TranslatorMethodNotSupported {0}", methodCall.Method.Name);
                throw new NotSupportedException(message);
            }

            translator.Translate(methodCall, result);
        }
    }
}
