using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Infrastructure
{
    /// <summary>
    ///     Performs evaluation of the LINQ Expression.
    /// </summary>
    internal sealed class ExpressionEvaluator : ExpressionVisitor
    {
        /// <summary>
        ///     Evaluates an expression.
        /// </summary>
        /// <param name="expression">Source expression.</param>
        /// <returns>Evaluated expression.</returns>
        public Expression Evaluate(Expression expression)
        {
            return Visit(expression);
        }

        protected override Expression VisitUnary(UnaryExpression node)
        {
            switch (node.NodeType)
            {
                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    {
                        ConstantExpression operand = TryToEvaluate(node.Operand);

                        var convertable = operand.Value as IConvertible;
                        if (convertable == null)
                        {
                            var invalidCast = string.Format("Resources.ExpressionEvaluatorInvalidCast {0} {1}", operand.Value.GetType(), node.Type);
                            throw new InvalidCastException(invalidCast);
                        }

                        object value = convertable.ToType(node.Type, CultureInfo.CurrentCulture);
                        return Expression.Constant(value, value.GetType());
                    }
            }

            string message = string.Format("Resources.ExpressionEvaluatorUnableToEvaluate {0}", node);
            throw new NotSupportedException(message);
        }

        private ConstantExpression TryToEvaluate(Expression expression)
        {
            Expression result = Evaluate(expression);
            if (result.NodeType == ExpressionType.Constant)
            {
                return (ConstantExpression)result;
            }

            string message = string.Format("Resources.ExpressionEvaluatorUnableToEvaluate {0}", expression);
            throw new NotSupportedException(message);
        }

        protected override Expression VisitNew(NewExpression node)
        {
            var arguments = new object[node.Arguments.Count];

            for (int i = 0; i < node.Arguments.Count; i++)
            {
                ConstantExpression result = TryToEvaluate(node.Arguments[i]);
                arguments[i] = result.Value;
            }

            return Expression.Constant(node.Constructor.Invoke(arguments), node.Type);
        }

        protected override Expression VisitNewArray(NewArrayExpression node)
        {
            if (node.Type != typeof(Byte[]))
            {
                String message = String.Format("Resources.ExpressionEvaluatorTypeNotSupported {0}", node.Type);
                throw new NotSupportedException(message);
            }

            var array = new Byte[node.Expressions.Count];

            for (int i = 0; i < node.Expressions.Count; i++)
            {
                ConstantExpression result = TryToEvaluate(node.Expressions[i]);
                array[i] = (Byte)result.Value;
            }

            return Expression.Constant(array, node.Type);
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            var arguments = new object[node.Arguments.Count];

            for (int i = 0; i < node.Arguments.Count; i++)
            {
                ConstantExpression result = TryToEvaluate(node.Arguments[i]);
                arguments[i] = result.Value;
            }

            object instance = null;

            if (node.Object != null)
            {
                ConstantExpression constantObject = TryToEvaluate(node.Object);
                instance = constantObject.Value;
            }

            object value = node.Method.Invoke(instance, arguments);

            return Expression.Constant(value, node.Type);
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            if (node.Expression == null)
            {
                return GetMemberConstant(node);
            }

            switch (node.Expression.NodeType)
            {
                case ExpressionType.Constant:
                case ExpressionType.MemberAccess:
                    return GetMemberConstant(node);

                default:
                    return base.VisitMember(node);
            }
        }

        private ConstantExpression GetMemberConstant(MemberExpression node)
        {
            object value=null;

            var member = node.Member;


            FieldInfo field = member as FieldInfo;
            if (field != null)
                value = GetFieldValue(node);

            PropertyInfo property = member as PropertyInfo;
            if (property != null)
                value = GetPropertyValue(node);

            if(property != null && field != null)
                throw new NotSupportedException(string.Format("Invalid member type: {0}", node.Member.DeclaringType));
             

            return Expression.Constant(value, node.Type);
        }

        private object GetFieldValue(MemberExpression node)
        {
            var fieldInfo = (FieldInfo)node.Member;
            object instance = null;

            if (node.Expression != null)
            {
                ConstantExpression ce = TryToEvaluate(node.Expression);
                instance = ce.Value;
            }

            return fieldInfo.GetValue(instance);
        }

        private object GetPropertyValue(MemberExpression node)
        {
            var propertyInfo = (PropertyInfo)node.Member;
            object instance = null;

            if (node.Expression != null)
            {
                ConstantExpression ce = TryToEvaluate(node.Expression);
                instance = ce.Value;
            }

            return propertyInfo.GetValue(instance, null);
        }
    }
}
