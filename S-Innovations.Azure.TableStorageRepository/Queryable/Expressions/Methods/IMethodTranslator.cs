using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Methods
{
    /// <summary>
    ///     Expression method translator.
    /// </summary>
    internal interface IMethodTranslator
    {
        /// <summary>
        ///     Gets a method name.
        /// </summary>
        string Name { get; }

        /// <summary>
        ///     Provides evaluated query information.
        /// </summary>
        /// <param name="method">Expression method.</param>
        /// <param name="result">Translation result.</param>
        void Translate(MethodCallExpression method, ITranslationResult result);
    }
}
