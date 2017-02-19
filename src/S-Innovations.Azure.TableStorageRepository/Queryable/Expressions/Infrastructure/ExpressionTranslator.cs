using Microsoft.WindowsAzure.Storage.Table;
using Microsoft.Extensions.Logging;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using System.Threading.Tasks;

namespace SInnovations.Azure.TableStorageRepository.Queryable.Expressions.Infrastructure
{
    /// <summary>
    ///     Expression translator.
    ///     http://msdn.microsoft.com/en-us/library/windowsazure/dd894031.aspx
    /// </summary>
    internal sealed class ExpressionTranslator : ExpressionVisitor
    {
        private struct KeyFilter
        {
            public String Type { get; set; }
            public int Position { get; set; }
            public String Value { get; set; }

            public string PropertyName { get; set; }
            public bool StartsWith { get;  set; }
        }
        private readonly ILogger Logger;

        private readonly ExpressionEvaluator _constantEvaluator;
        private readonly IDictionary<string, string> _nameChanges;
        EntityTypeConfiguration _configuration;
        private StringBuilder _filter;
        private List<KeyFilter> _keyFilters;

        private ITranslationResult _result;

        /// <summary>
        ///     Constructor.
        /// </summary>
        /// <param name="nameChanges"></param>
        internal ExpressionTranslator(ILoggerFactory logFactory,EntityTypeConfiguration configuration)
        {
            Logger = logFactory.CreateLogger<ExpressionTranslator>();
            //_nameChanges = nameChanges;
            _configuration = configuration;
            _nameChanges = configuration.KeyMappings;

            _constantEvaluator = new ExpressionEvaluator();
        }

        public void Translate(ITranslationResult result, MethodCallExpression method)
        {
            _result = result;

            if (method.Arguments.Count != 2)
            {
                return;
            }

            var lambda = (LambdaExpression)StripQuotes(method.Arguments[1]);

            if (lambda.Body.NodeType == ExpressionType.Constant)
            {
                return;
            }

            _filter = new StringBuilder();
            _keyFilters = new List<KeyFilter>();


            Visit(lambda.Body);

            var odataFilter = TrimString(_filter);

            if (_keyFilters.Any())
            {


                var filter = new StringBuilder();

                var keys = _keyFilters.GroupBy(k => k.Type)
                    .Select(k => new
                    {
                        k.Key,
                        PropertyName = string.Join(TableStorageContext.KeySeparator, k.OrderBy(v => v.Position).Select(v => v.PropertyName)),
                        StartsWithPattern = string.Join(TableStorageContext.KeySeparator, k.OrderBy(v => v.Position).Select(v => v.Value)),
                        HasStartsWith = k.Any(v=>v.StartsWith),// string.Join(TableStorageContext.KeySeparator, k.OrderBy(v => v.Position).Select(v => v.Value)),
                    }).ToArray();

                for (int i = 0; i < keys.Length; ++i)
                {

                    var key = keys[i];
                    if (!_nameChanges.Keys.Any(k => k.StartsWith(key.PropertyName)))
                        throw new Exception("Make sure that all the filters are used when having hasKeys(p=new { p.p1, p.p2}) ");

                    if (i > 0)
                        filter.Append(" and ");


                    if (!key.HasStartsWith)
                    {
                        filter.Append(key.Key);
                        filter.Append(" eq '");
                        filter.Append(key.StartsWithPattern);
                        filter.Append("'");


                    }else { 

                        var length = key.StartsWithPattern.Length - 1;
                        var lastChar = key.StartsWithPattern[length];
                        var nextLastChar = (char)(lastChar + 1);
                        var startsWithEndPattern = key.StartsWithPattern.Substring(0, length) + nextLastChar;


                        filter.Append(key.Key);
                        filter.Append(" ge '");
                        filter.Append(key.StartsWithPattern);
                        filter.Append("' and ");
                        filter.Append(key.Key);
                        filter.Append(" lt '");
                        filter.Append(startsWithEndPattern);
                        filter.Append("'");

                    }

                   

                }

                //var i = 0;
                //if (odataFilter.StartsWith(" and "))
                //    i = " and ".Length;
                //if (odataFilter.StartsWith("( and )"))
                //    i = "( and )".Length;

                //   "(( and ) and ) and"
                int idx = 0;
                do
                {
                    idx = odataFilter.IndexOf("( and )");
                    if (idx > -1)
                    {
                        odataFilter = odataFilter.Remove(idx, "( and )".Length);
                    }

                } while (idx > -1);
                if (odataFilter.Equals(" and "))
                    odataFilter = "";

                odataFilter = filter.ToString() + odataFilter;
            }

            _result.AddFilter(odataFilter);
        }

        public void AddPostProcessing(MethodCallExpression method)
        {
            Type type = method.Arguments[0].Type.GenericTypeArguments[0];
            Type genericType = typeof(IQueryable<>).MakeGenericType(type);

            ParameterExpression parameter = Expression.Parameter(genericType, null);
            MethodInfo methodInfo = typeof(System.Linq.Queryable)
                .GetRuntimeMethods()
                .Single(p => p.Name == method.Method.Name && p.GetParameters().Length == 1)
                .MakeGenericMethod(type);

            MethodCallExpression call = Expression.Call(methodInfo, parameter);

            _result.AddPostProcesing(Expression.Lambda(call, parameter));
        }

        private static String TrimString(StringBuilder builder)
        {
            int i = 0;
            int j = builder.Length - 1;

            // Trim spaces
            while (i < j && builder[i] == ' ')
            {
                i++;
            }



            while (j > i && builder[j] == ' ')
            {
                j--;
            }

            // Remove Parentheses
            while (j - i > 2 && builder[i] == '(' && builder[j] == ')')
            {
                i++;
                j--;
            }


            return builder.ToString(i, j - i + 1);
        }

        public static Expression StripQuotes(Expression expression)
        {
            while (expression.NodeType == ExpressionType.Quote)
            {
                expression = ((UnaryExpression)expression).Operand;
            }

            return expression;
        }

        protected override Expression VisitUnary(UnaryExpression unary)
        {
            _filter.AppendFormat(" {0} ", unary.NodeType.Serialize());

            Visit(unary.Operand);

            return unary;
        }

        protected override Expression VisitBinary(BinaryExpression binary)
        {
            ExpressionType nodeType = binary.NodeType;
            ExpressionType leftType = binary.Left.NodeType;
            ExpressionType rightType = binary.Right.NodeType;

            bool paranthesesRequired = nodeType.IsSupported() && (leftType.IsSupported() || rightType.IsSupported());

            if (IsPartionOrRowKeyNameChange(binary))
            {
                return binary;
            }

            if (paranthesesRequired)
            {
                _filter.Append("(");
            }

            //If its part of partition or rowkey


            // Left part
            if (leftType == ExpressionType.Call)
            {
                if (AppendBinaryCall((MethodCallExpression)binary.Left, nodeType, null, false))
                {
                    return binary;
                }
                Logger.LogWarning("Please report this if you see it in your logs, ID:F68BA48F-DA61-4DEB-A078-5D10F722E324");
            }
            else
            {
               
                AppendBinaryPart(binary.Left, leftType, null, false);
            }

            // Comparison
            _filter.AppendFormat(" {0} ", nodeType.Serialize());

            // Right part
            AppendRightPart(binary, nodeType, rightType, false);

            if (paranthesesRequired)
            {
                _filter.Append(")");
            }

            return binary;
        }

        private void AppendRightPart(BinaryExpression binary, ExpressionType nodeType, ExpressionType rightType, bool isKey)
        {
            Func<string, string> encoder = null;
            Delegate valueConverter = null;       
            if (binary.Left.NodeType == ExpressionType.MemberAccess)
            {
                var name = (binary.Left as MemberExpression).Member.Name;
                if (_configuration.PropertiesToEncode.ContainsKey(name))
                {
                    encoder = _configuration.PropertiesToEncode[name].Encoder;

                }
                if (isKey && _configuration.IgnoreKeyPropertyRemovables.ContainsKey(name))
                {
                    valueConverter=((Delegate)_configuration.IgnoreKeyPropertyRemovables[name]);
                }

            }

            if (rightType == ExpressionType.Call)
            {
                var methodCall = (MethodCallExpression)binary.Right;

                if (AppendBinaryCall(methodCall, nodeType, encoder, isKey))
                {
                    string message = String.Format("Resources.TranslatorMethodNotSupported {0}", methodCall.Method.Name);
                    throw new ArgumentException(message);
                }
            }
            else
            {




                AppendBinaryPart(binary.Right, rightType, encoder, isKey, valueConverter);


            }
        }
     

        private bool Is(MemberExpression member, out string key, out string[] keys)
        {
            key = null; keys = null;
            foreach (var properties in _nameChanges.Keys)
            {
                key = properties;

                keys = key.Split(new[] { TableStorageContext.KeySeparator }, StringSplitOptions.RemoveEmptyEntries);
                if(keys.Any())// (keys.Skip(1).Any())
                {

                    if (keys.Any(k => k == member.Member.Name))
                    {
                        return true;
                    }

                }

            }
            return false;

        }
        private bool IsPartionOrRowKeyNameChange(BinaryExpression binary)
        {
           
            var left = binary.Left;

            if (left.NodeType == ExpressionType.MemberAccess)
            {
                var member = (MemberExpression)binary.Left;
                string key;
                string[] keys;
                if (Is(member, out key, out keys))
                {
                    var old = _filter;
                    var startsWithPattern = "";
                    _filter = new StringBuilder();

                   

                    AppendRightPart(binary, binary.NodeType, binary.Right.NodeType, isKey(_nameChanges[key]));
                    // AppendBinaryPart(binary.Right, binary.Right.NodeType);
                    startsWithPattern = _filter.ToString().Trim('\'');
                    _filter = old;




                    _keyFilters.Add(new KeyFilter
                    {
                        PropertyName = member.Member.Name,
                        Type = _nameChanges[key],
                        Value = _configuration.PropertiesToEncode.ContainsKey(member.Member.Name) ?
                            _configuration.PropertiesToEncode[member.Member.Name].Encoder(startsWithPattern) :
                            startsWithPattern,
                        Position = Array.IndexOf(keys, member.Member.Name),
                    });

                    var overrideIsKey = _configuration.IgnoreKeyPropertyRemovables.ContainsKey(member.Member.Name);
                    if (overrideIsKey)
                        return false;
                    return true;
                }


            }
            return false;
        }

        private bool isKey(string p)
        {
            return p == "PartitionKey" || p == "RowKey";
        }

        private void AppendBinaryPart(Expression node, ExpressionType type, Func<string, string> encoder = null, bool isKey = false, Delegate valueConverter=null)
        {
            switch (type)
            {
                case ExpressionType.Invoke:
                    var invocation = (InvocationExpression)node;
                    Visit(invocation.Expression);
                    break;

                case ExpressionType.New:
                case ExpressionType.NewArrayInit:
                case ExpressionType.Constant:
                    AppendConstant(_constantEvaluator.Evaluate(node), encoder, isKey);
                    break;

                case ExpressionType.Convert:
                case ExpressionType.ConvertChecked:
                    var unary = (UnaryExpression)node;
                    AppendConstant(_constantEvaluator.Evaluate(unary.Operand), encoder, false);
                    break;

                case ExpressionType.MemberAccess:
                    var member = (MemberExpression)node;
                    Expression expression = member.Expression;
                    if (expression != null && expression.NodeType == ExpressionType.Parameter)
                    {
                        AppendParameter(node);
                    }
                    else
                    {


                        AppendConstant(node, encoder, isKey, valueConverter);
                    }
                    break;

                default:
                    // Check whether expression is binary
                    if (type.IsSupported())
                    {
                        Visit(node);
                    }
                    else
                    {
                        string message = String.Format("Resources.TranslatorMemberNotSupported {0}", type);
                        throw new ArgumentException(message);
                    }
                    break;
            }
        }

        protected override Expression VisitMethodCall(MethodCallExpression node)
        {
            switch (node.Method.Name)
            {
                case "StartsWith":

                    var member = (MemberExpression)node.Object;
                    var propName = member.Member.Name;

                    var instance = _constantEvaluator.Evaluate(node.Arguments[0]) as ConstantExpression;
                    var startsWithPattern = instance.Value as string;
                    if (_configuration.PropertiesToEncode.ContainsKey(propName))
                        startsWithPattern = _configuration.PropertiesToEncode[propName].Encoder(startsWithPattern);

                    var length = startsWithPattern.Length - 1;
                    var lastChar = startsWithPattern[length];
                    var nextLastChar = (char)(lastChar + 1);
                    var startsWithEndPattern = startsWithPattern.Substring(0, length) + nextLastChar;


                    string key;
                    string[] keys;
                    if (Is(member, out key, out keys))
                    {
                        _keyFilters.Add(new KeyFilter
                        {
                            PropertyName = propName,
                            Type = _nameChanges[key],
                            Value = startsWithPattern,
                            Position = Array.IndexOf(keys, propName),
                            StartsWith = true
                        });
                    }
                    else
                    {
                        _filter.Append(member.Member.Name);
                        _filter.Append(" ge '");
                        _filter.Append(startsWithPattern);
                        _filter.Append("' and ");
                        _filter.Append(member.Member.Name);
                        _filter.Append(" lt '");
                        _filter.Append(startsWithEndPattern);
                        _filter.Append("'");
                    }


                    break;
                case "Contains":
                    if (node.Arguments.Count == 1)
                    {
                        var result = (ConstantExpression)_constantEvaluator.Evaluate(node.Object);

                        var enumerable = result.Value as IEnumerable;
                        if (enumerable == null)
                        {
                            string message = string.Format("Resources.TranslatorMethodInvalidArgument {0}", node.Method.Name);
                            throw new ArgumentException(message);
                        }

                        Expression parameter = node.Arguments[0];

                        // determine equality value
                        string equality;

                        if (_filter.Length >= 5 &&
                            _filter.ToString(_filter.Length - 5, 5) == " not ")
                        {
                            _filter.Remove(_filter.Length - 5, 5);
                            equality = " ne ";
                        }
                        else
                        {
                            equality = " eq ";
                        }

                        _filter.Append("(");
                        int count = 0;

                        foreach (object value in enumerable)
                        {
                            AppendParameter(parameter);
                            _filter.Append(equality);
                            AppendConstant(Expression.Constant(value), null, false); //Investigate if null is okay
                            _filter.Append(" or ");
                            count++;
                        }

                        if (count > 0)
                        {
                            _filter.Remove(_filter.Length - 4, 4);
                            _filter.Append(")");
                        }
                        else
                        {
                            _filter.Remove(_filter.Length - 1, 1);
                        }
                    }
                    else
                    {
                        string message = string.Format("Resources.TranslatorMethodInvalidArgument {0}", node.Method.Name);
                        throw new ArgumentException(message);
                    }
                    break;

                //case "ToString":
                //    ConstantExpression constant;

                //    if (node.Object != null)
                //    {
                //        var instance = _constantEvaluator.Evaluate(node.Object);
                //        //node.Method.Invoke(instance,node.Arguments.Select(_constantEvaluator)
                //        constant = Expression.Constant(instance.ToString());
                //    }
                //    else
                //    {
                //        constant = Expression.Constant(string.Empty);
                //    }
                //    var test = _constantEvaluator.Visit(node);
                //    AppendConstant(test);
                //    //AppendConstant(constant);
                //    break;

                default:
                    AppendConstant(_constantEvaluator.Evaluate(node), null, false);
                    break;
            }

            return node;
        }

        /// <summary>
        ///     Translates method call expression.
        /// </summary>
        /// <param name="node">Expression.</param>
        /// <param name="type">Expression type.</param>
        /// <returns>Whether expression has been completely translated.</returns>
        private bool AppendBinaryCall(MethodCallExpression node, ExpressionType type, Func<string, string> encoder, bool isKey)
        {
            switch (node.Method.Name)
            {
                case "CompareTo":
                    AppendParameter(_constantEvaluator.Evaluate(node.Object));
                    _filter.AppendFormat(" {0} ", type.Serialize());
                    AppendConstant(_constantEvaluator.Evaluate(node.Arguments[0]), encoder, isKey);
                    return true;

                case "Compare":
                case "CompareOrdinal":
                    if (node.Arguments.Count >= 2)
                    {
                        AppendParameter(_constantEvaluator.Evaluate(node.Arguments[0]));
                        _filter.AppendFormat(" {0} ", type.Serialize());
                        AppendConstant(_constantEvaluator.Evaluate(node.Arguments[1]), encoder, isKey);
                    }
                    else
                    {
                        string message = string.Format("Resources.TranslatorMethodInvalidArgument {0}", node.Method.Name);
                        throw new ArgumentException(message);
                    }
                    return true;

                default:
                    VisitMethodCall(node);
                    break;
            }

            return false;
        }

        protected override Expression VisitMember(MemberExpression node)
        {
            AppendParameter(node);
            return node;
        }

        private void AppendParameter(Expression node)
        {
            if (node.NodeType != ExpressionType.MemberAccess)
            {
                string message = String.Format("Resources.TranslatorMemberNotSupported {0}", node.NodeType);
                throw new NotSupportedException(message);
            }

            var member = (MemberExpression)node;

            // Append member
            string name;
            string memberName = member.Member.Name;
            if (!_nameChanges.TryGetValue(memberName, out name))
            {
                name = memberName;
            }

            _filter.Append(name);
        }

        private void AppendConstant(Expression node, Func<string, string> encoder, bool isKey,Delegate valueconverter = null)
        {
            // Evaluate if required
            if (node.NodeType != ExpressionType.Constant)
            {
                Expression result = _constantEvaluator.Evaluate(node);
                if (result.NodeType != ExpressionType.Constant)
                {
                    string message = String.Format("Resources.TranslatorUnableToEvaluateExpression {0}", node);
                    throw new InvalidOperationException(message);
                }

                node = result;
            }

            var constant = (ConstantExpression)node;

            if (valueconverter != null)
            {
                constant = ConstantExpression.Constant(valueconverter.DynamicInvoke(constant.Value));
            }
            if (encoder != null && constant.Value.GetType() == typeof(string))
            {
                constant = ConstantExpression.Constant(encoder((string)constant.Value));
                // constant.Value = (object)encoder((string)constant.Value);
            }
            _filter.Append(constant.Serialize(isKey));
        }
    }
}
