using Nest;
using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;

namespace Birko.Data.ElasticSearch
{
    public static class ElasticSearch
    {
        /// <summary>
        /// The maximum result window for queries.
        /// </summary>
        public static int MaxResultWindow { get; set; } = 10000;

        /// <summary>
        /// Maximum size for bulk operations.
        /// </summary>
        public static readonly int MaxBulkSize = 10000;

        /// <summary>
        /// Default scroll time for large result sets.
        /// </summary>
        public static TimeSpan DefaultScrollTime { get; set; } = TimeSpan.FromMinutes(1);

        private static readonly ConcurrentDictionary<string, ElasticClient> _clients = new();
        private static readonly ConcurrentDictionary<string, Func<object>> _expressionCache = new();

        /// <summary>
        /// Maximum number of retries for transient failures. Default is 3.
        /// </summary>
        public static int MaxRetries { get; set; } = 3;

        /// <summary>
        /// Request timeout for Elasticsearch operations. Default is 60 seconds.
        /// </summary>
        public static TimeSpan RequestTimeout { get; set; } = TimeSpan.FromSeconds(60);

        public static ElasticClient GetClient(Stores.Settings settings)
        {
            var settingsId = settings.GetId();
            return _clients.GetOrAdd(settingsId, id =>
            {
                var local = new Uri(settings.Location);
                var clientSettings = new ConnectionSettings(local)
                    .DisableDirectStreaming()
                    .MaximumRetries(MaxRetries)
                    .RequestTimeout(RequestTimeout);
                return new ElasticClient(clientSettings);
            });
        }

        public static QueryBase? ParseExpression(Expression? expr = null, Type? exprType = null, string? fieldPrefix = null)
        {
            return expr switch
            {
                null => null,
                LambdaExpression lambda => ParseLambda(lambda, fieldPrefix),
                BinaryExpression binary => ParseBinary(binary, exprType, fieldPrefix),
                MethodCallExpression method => ParseMethodCall(method, exprType, fieldPrefix),
                MemberExpression member => ParseMember(member, exprType, fieldPrefix),
                UnaryExpression unary => ParseUnary(unary, exprType, fieldPrefix),
                ConstantExpression constant => ParseConstant(constant),
                _ => null
            };
        }

        private static QueryBase? ParseLambda(LambdaExpression lambda, string? fieldPrefix)
        {
            var type = lambda.Parameters.FirstOrDefault()?.Type;
            return ParsePredicate(lambda.Body, type, fieldPrefix);
        }

        /// <summary>
        /// Parses an expression that appears in BOOLEAN (predicate) position — a lambda body, an operand of
        /// &amp;&amp; / || / &amp; / |, or the operand of !. Unlike <see cref="ParseExpression"/>, a bare
        /// boolean member (<c>x.IsActive</c>) becomes <c>IsActive == true</c> and a constant boolean
        /// (<c>x =&gt; true</c>) becomes a match-all / match-none query, rather than a value-carrying term.
        /// </summary>
        private static QueryBase? ParsePredicate(Expression expr, Type? exprType, string? fieldPrefix)
        {
            // Unwrap Convert(..., object) inserted by Func<T, object> filters.
            if (expr is UnaryExpression convert && convert.NodeType == ExpressionType.Convert)
                return ParsePredicate(convert.Operand, exprType, fieldPrefix);

            // Constant-foldable boolean predicate: x => true, x => false, closure bool, 1 < 2, etc.
            if (expr.Type == typeof(bool) && !ContainsParameter(expr))
            {
                if (EvaluateExpression(expr) is bool b)
                    return b ? new MatchAllQuery() : (QueryBase)new MatchNoneQuery();
            }

            // Bare boolean member of the parameter: x => x.IsActive → IsActive == true.
            if (expr is MemberExpression member && member.Type == typeof(bool)
                && exprType != null && IsDirectMemberOfParameter(member, exprType))
            {
                var name = FormatFieldName(member.Member.Name, fieldPrefix);
                return name != null ? new TermQuery { Field = name, Value = true } : null;
            }

            return ParseExpression(expr, exprType, fieldPrefix);
        }

        private static QueryBase? ParseBinary(BinaryExpression binary, Type? exprType, string? fieldPrefix)
        {
            switch (binary.NodeType)
            {
                // Short-circuit && and bitwise & on booleans both mean logical AND in a predicate.
                case ExpressionType.AndAlso:
                    return CombineBool(binary, isOr: false, exprType, fieldPrefix);
                case ExpressionType.And when binary.Type == typeof(bool):
                    return CombineBool(binary, isOr: false, exprType, fieldPrefix);
                // Short-circuit || and bitwise | on booleans both mean logical OR in a predicate.
                case ExpressionType.OrElse:
                    return CombineBool(binary, isOr: true, exprType, fieldPrefix);
                case ExpressionType.Or when binary.Type == typeof(bool):
                    return CombineBool(binary, isOr: true, exprType, fieldPrefix);
                default:
                    return ParseComparison(binary, exprType, fieldPrefix);
            }
        }

        private static QueryBase? CombineBool(BinaryExpression binary, bool isOr, Type? exprType, string? fieldPrefix)
        {
            var leftQuery = ParsePredicate(binary.Left, exprType, fieldPrefix);
            var rightQuery = ParsePredicate(binary.Right, exprType, fieldPrefix);
            var queries = new List<QueryContainer>(2);
            if (leftQuery != null) queries.Add(new(leftQuery));
            if (rightQuery != null) queries.Add(new(rightQuery));
            if (queries.Count == 0)
                return null;
            return isOr ? new BoolQuery { Should = queries } : new BoolQuery { Must = queries };
        }

        private static QueryBase? ParseComparison(BinaryExpression binary, Type? exprType, string? fieldPrefix)
        {
            var left = ParseExpression(binary.Left, exprType, fieldPrefix) as ITermQuery;
            var right = ParseExpression(binary.Right, exprType, fieldPrefix) as ITermQuery;

            var field = left?.Field ?? right?.Field;
            var value = right?.Value ?? left?.Value;

            // Handle null comparisons: Equal(member, null) → must_not exists, NotEqual(member, null) → exists
            if (field != null && value == null)
            {
                return binary.NodeType switch
                {
                    ExpressionType.Equal => new BoolQuery
                    {
                        MustNot = new QueryContainer[] { new ExistsQuery { Field = field } }
                    },
                    ExpressionType.NotEqual => new ExistsQuery { Field = field },
                    _ => null
                };
            }

            if (field == null || value == null)
                return null;

            double? doubleValue = TryConvertToDouble(value);

            return binary.NodeType switch
            {
                ExpressionType.GreaterThan => new NumericRangeQuery { Field = field, GreaterThan = doubleValue },
                ExpressionType.GreaterThanOrEqual => new NumericRangeQuery { Field = field, GreaterThanOrEqualTo = doubleValue },
                ExpressionType.LessThan => new NumericRangeQuery { Field = field, LessThan = doubleValue },
                ExpressionType.LessThanOrEqual => new NumericRangeQuery { Field = field, LessThanOrEqualTo = doubleValue },
                ExpressionType.Equal => new TermQuery { Field = field, Value = value },
                ExpressionType.NotEqual => new BoolQuery
                {
                    MustNot = new QueryContainer[] { new TermQuery { Field = field, Value = value } }
                },
                _ => null
            };
        }

        private static double? TryConvertToDouble(object value)
        {
            if (value == null)
                return null;

            try
            {
                return Convert.ToDouble(value);
            }
            catch (InvalidCastException)
            {
                return null;
            }
            catch (FormatException)
            {
                return null;
            }
            catch (OverflowException)
            {
                return null;
            }
        }

        private static QueryBase? ParseMethodCall(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            return call.Method.Name switch
            {
                "Property" when call.Arguments.LastOrDefault() is ConstantExpression propName =>
                    new TermQuery { Field = FormatFieldName(propName.Value as string, fieldPrefix) },

                "IsNullOrEmpty" =>
                    ParseIsNullOrEmpty(call, exprType, fieldPrefix),

                "StartsWith" =>
                    ParseStartsWith(call, exprType, fieldPrefix),

                "EndsWith" =>
                    ParseEndsWith(call, exprType, fieldPrefix),

                "Contains" =>
                    ParseContains(call, exprType, fieldPrefix),

                "MultiMatch" =>
                    ParseMultiMatch(call, exprType, fieldPrefix),

                "Any" =>
                    ParseAny(call, exprType, fieldPrefix),

                // Case-normalising calls have no term/keyword ES equivalent; treat them as transparent so the
                // wrapped column still resolves (case-sensitivity is delegated to the field's analyzer).
                "ToLower" or "ToLowerInvariant" or "ToUpper" or "ToUpperInvariant" =>
                    ParseExpression(call.Object, exprType, fieldPrefix),

                _ => ParseConstantCall(call)
            };
        }

        private static QueryBase? ParseConstantCall(MethodCallExpression call)
        {
            var value = EvaluateExpression(call);
            return value != null ? new TermQuery { Value = value } : null;
        }

        private static QueryBase? ParseIsNullOrEmpty(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            var field = ParseExpression(call.Arguments.First(), exprType, fieldPrefix) as ITermQuery;
            if (field?.Field == null)
                return null;

            return new BoolQuery
            {
                MustNot = new[] { new QueryContainer(new WildcardQuery { Field = field.Field, Value = "*" }) }
            };
        }

        private static QueryBase? ParseStartsWith(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            var swField = ParseExpression(call.Object, exprType, fieldPrefix) as ITermQuery;
            var swVal = ParseExpression(call.Arguments.First(), exprType, fieldPrefix) as ITermQuery;

            if (swField?.Field == null || swVal?.Value == null)
                return null;

            return new PrefixQuery { Field = swField.Field, Value = swVal.Value };
        }

        private static QueryBase? ParseEndsWith(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            var ewField = ParseExpression(call.Object, exprType, fieldPrefix) as ITermQuery;
            var ewVal = ParseExpression(call.Arguments.First(), exprType, fieldPrefix) as ITermQuery;

            if (ewField?.Field == null || ewVal?.Value == null)
                return null;

            // "ends with x" has no dedicated ES query; a leading-wildcard match is the equivalent of SQL LIKE '%x'.
            return new WildcardQuery { Field = ewField.Field, Value = "*" + ewVal.Value };
        }

        private static QueryBase? ParseContains(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            // String.Contains(substring) → substring match (mirrors SQL LIKE '%x%').
            if (call.Method.DeclaringType == typeof(string))
            {
                var cField = ParseExpression(call.Object, exprType, fieldPrefix) as ITermQuery;
                var cVal = ParseExpression(call.Arguments.First(), exprType, fieldPrefix) as ITermQuery;

                if (cField?.Field == null || cVal?.Value == null)
                    return null;

                return new QueryStringQuery { DefaultField = cField.Field, Query = (string)cVal.Value };
            }

            // Collection.Contains(...) — either constCollection.Contains(x.Member) (the IN pattern) or
            // x.ArrayMember.Contains(constValue) (array membership). The field is the parameter-bound operand;
            // the other operand supplies the value(s).
            Expression? a = call.Object;
            Expression? b;
            if (a != null)
            {
                // instance form: a.Contains(b)
                b = call.Arguments.FirstOrDefault();
            }
            else
            {
                // static Enumerable.Contains(source, item)
                a = call.Arguments.ElementAtOrDefault(0);
                b = call.Arguments.ElementAtOrDefault(1);
            }
            if (a == null || b == null)
                return null;

            Expression fieldExpr, valueExpr;
            if (ContainsParameter(a) && !ContainsParameter(b)) { fieldExpr = a; valueExpr = b; }
            else if (ContainsParameter(b) && !ContainsParameter(a)) { fieldExpr = b; valueExpr = a; }
            else return null;

            var fieldQuery = ParseExpression(fieldExpr, exprType, fieldPrefix) as ITermQuery;
            if (fieldQuery?.Field == null)
                return null;

            var value = EvaluateExpression(valueExpr);
            if (value == null)
                return null;

            // A collection value → terms query (IN); a scalar → single term (array membership).
            if (value is System.Collections.IEnumerable enumerable && value is not string)
            {
                var terms = enumerable.Cast<object>().Where(v => v != null).ToArray();
                return terms.Length > 0 ? new TermsQuery { Field = fieldQuery.Field, Terms = terms } : null;
            }

            return new TermQuery { Field = fieldQuery.Field, Value = value };
        }

        private static QueryBase? ParseMultiMatch(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            var mmVal = ParseExpression(call.Arguments.Last(), exprType, fieldPrefix) as ITermQuery;
            if (mmVal?.Value == null)
                return null;

            var fields = (call.Arguments.First() as NewArrayExpression)?.Expressions
                .Select(m => (ParseExpression(m, exprType, fieldPrefix) as ITermQuery)?.Field)
                .Where(f => f != null)
                .ToArray();

            if (fields == null || fields.Length == 0)
                return null;

            return new MultiMatchQuery { Query = (string)mmVal.Value, Fields = fields };
        }

        private static QueryBase? ParseAny(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            var anyField = ParseExpression(call.Arguments.First(), exprType, fieldPrefix) as ITermQuery;
            if (anyField?.Field == null)
                return null;

            var query = ParseExpression(call.Arguments.Last(), exprType, anyField.Field.Name);
            if (query == null)
                return null;

            return new NestedQuery
            {
                Path = anyField.Field,
                Query = query
            };
        }

        private static QueryBase? ParseMember(MemberExpression member, Type? exprType, string? fieldPrefix)
        {
            // Nullable HasValue access: x.NullableProp.HasValue → exists query (IS NOT NULL)
            if (member.Member.Name == "HasValue"
                && member.Expression is MemberExpression hasValueInner
                && member.Member.ReflectedType != null
                && Nullable.GetUnderlyingType(member.Member.ReflectedType) != null
                && exprType != null && IsDirectMemberOfParameter(hasValueInner, exprType))
            {
                var name = FormatFieldName(hasValueInner.Member.Name, fieldPrefix);
                return name != null ? new ExistsQuery { Field = name } : null;
            }

            // Direct member access (e.g., x => x.Name)
            if (exprType != null && IsDirectMemberOfParameter(member, exprType))
            {
                var name = FormatFieldName(member.Member.Name, fieldPrefix);
                if (name == null)
                    return null;

                // Add .keyword suffix for string fields if not already present
                if (member.Type == typeof(string) && !name.EndsWith(".keyword", StringComparison.OrdinalIgnoreCase))
                {
                    name += ".keyword";
                }
                return new TermQuery { Field = name };
            }

            // Constant, closure, or non-parameter member access — evaluate as value
            if (!ContainsParameter(member))
            {
                var value = EvaluateExpression(member);
                return value != null ? new TermQuery { Value = value } : null;
            }

            // Parameter-bound sub-expression — recurse
            if (member.Expression != null)
                return ParseExpression(member.Expression, exprType, fieldPrefix);

            return null;
        }

        private static QueryBase? ParseUnary(UnaryExpression unary, Type? exprType, string? fieldPrefix)
        {
            return unary.NodeType switch
            {
                ExpressionType.Convert => ParseExpression(unary.Operand, exprType, fieldPrefix),
                ExpressionType.Not => ParseNot(unary, exprType, fieldPrefix),
                _ => null
            };
        }

        private static QueryBase? ParseNot(UnaryExpression unary, Type? exprType, string? fieldPrefix)
        {
            var operandQuery = ParsePredicate(unary.Operand, exprType, fieldPrefix);
            if (operandQuery == null)
                return null;

            return new BoolQuery
            {
                MustNot = new QueryContainer[] { new(operandQuery) }
            };
        }

        private static QueryBase? ParseConstant(ConstantExpression constant)
        {
            return constant.Value != null ? new TermQuery { Value = constant.Value } : null;
        }

        private static bool IsDirectMemberOfParameter(MemberExpression member, Type type)
        {
            return type != null &&
                   member.Member.ReflectedType != null &&
                   member.Member.ReflectedType.IsAssignableFrom(type) &&
                   member.Expression != null &&
                   (member.Expression.NodeType == ExpressionType.Parameter ||
                    member.Expression.NodeType == ExpressionType.TypeAs ||
                    (member.Expression is UnaryExpression conv
                        && conv.NodeType == ExpressionType.Convert
                        && conv.Operand.NodeType == ExpressionType.Parameter));
        }

        private static string? FormatFieldName(string? name, string? prefix)
        {
            if (string.IsNullOrEmpty(name))
                return null;

            // Convert to camelCase efficiently using string.Create
            var camel = string.Create(name.Length, name, (chars, source) =>
            {
                chars[0] = char.ToLowerInvariant(source[0]);
                for (int i = 1; i < source.Length; i++)
                {
                    chars[i] = source[i];
                }
            });

            return string.IsNullOrEmpty(prefix) ? camel : $"{prefix}.{camel}";
        }

        private static bool ContainsParameter(Expression expr)
        {
            if (expr is ParameterExpression)
                return true;
            if (expr is LambdaExpression lambda)
                return lambda.Parameters.Count > 0;
            if (expr is MemberExpression me)
                return me.Expression != null && ContainsParameter(me.Expression);
            if (expr is MethodCallExpression mc)
            {
                if (mc.Object != null && ContainsParameter(mc.Object))
                    return true;
                return mc.Arguments.Any(ContainsParameter);
            }
            if (expr is UnaryExpression ue)
                return ContainsParameter(ue.Operand);
            if (expr is BinaryExpression be)
                return ContainsParameter(be.Left) || ContainsParameter(be.Right);
            return false;
        }

        private static object? EvaluateExpression(Expression expr)
        {
            // Parameter-bound sub-expressions have no runtime value. Never attempt to compile/evaluate them —
            // doing so throws "variable 'x' referenced from scope '', but it is not defined". Callers that
            // reach here with a parameter-bound node (e.g. an unrecognised method call) get null instead.
            if (ContainsParameter(expr))
                return null;

            if (expr is ConstantExpression c)
                return c.Value;

            if (expr is MemberExpression m)
            {
                object? container = null;
                if (m.Expression != null)
                    container = EvaluateExpression(m.Expression);

                try
                {
                    if (m.Member is FieldInfo fi)
                        return fi.GetValue(container);
                    if (m.Member is PropertyInfo pi)
                        return pi.GetValue(container);
                }
                catch (TargetException)
                {
                    // Non-static member on null container — fall through to lambda compilation
                }
            }

            if (expr is UnaryExpression ue && ue.NodeType == ExpressionType.Convert)
                return EvaluateExpression(ue.Operand);

            if (expr is MethodCallExpression mce)
            {
                // Unwrap implicit/explicit conversion operators — e.g. the int[] → ReadOnlySpan<int> that
                // .NET binds for MemoryExtensions.Contains. Evaluate the source operand (the array), not the
                // ref-struct conversion, which reflection cannot invoke.
                if (mce.Method.IsSpecialName
                    && (mce.Method.Name == "op_Implicit" || mce.Method.Name == "op_Explicit")
                    && mce.Arguments.Count == 1)
                    return EvaluateExpression(mce.Arguments[0]);

                object? instance = mce.Object != null ? EvaluateExpression(mce.Object) : null;
                var args = new object?[mce.Arguments.Count];
                for (int i = 0; i < mce.Arguments.Count; i++)
                    args[i] = EvaluateExpression(mce.Arguments[i]);
                return mce.Method.Invoke(instance, args);
            }

            // Fallback: compile as parameterless lambda with cache
            var cacheKey = expr.ToString();
            var func = _expressionCache.GetOrAdd(cacheKey, _ =>
            {
                // Box value types so the delegate is always Func<object>
                var body = expr.Type.IsValueType
                    ? Expression.Convert(expr, typeof(object))
                    : expr;
                return Expression.Lambda<Func<object>>(body).Compile();
            });

            return func();
        }
    }
}
