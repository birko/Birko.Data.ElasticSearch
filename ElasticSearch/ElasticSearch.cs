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

        public static ElasticClient GetClient(Stores.Settings settings)
        {
            var settingsId = settings.GetId();
            return _clients.GetOrAdd(settingsId, id =>
            {
                var local = new Uri(settings.Location);
                var clientSettings = new ConnectionSettings(local)
                    .DisableDirectStreaming();
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
            return ParseExpression(lambda.Body, type, fieldPrefix);
        }

        private static QueryBase? ParseBinary(BinaryExpression binary, Type? exprType, string? fieldPrefix)
        {
            switch (binary.NodeType)
            {
                case ExpressionType.AndAlso:
                    {
                        var leftQuery = ParseExpression(binary.Left, exprType, fieldPrefix);
                        var rightQuery = ParseExpression(binary.Right, exprType, fieldPrefix);
                        var queries = new List<QueryContainer>(2);
                        if (leftQuery != null) queries.Add(new(leftQuery));
                        if (rightQuery != null) queries.Add(new(rightQuery));
                        return queries.Count > 0 ? new BoolQuery { Must = queries } : null;
                    }
                case ExpressionType.OrElse:
                    {
                        var leftQuery = ParseExpression(binary.Left, exprType, fieldPrefix);
                        var rightQuery = ParseExpression(binary.Right, exprType, fieldPrefix);
                        var queries = new List<QueryContainer>(2);
                        if (leftQuery != null) queries.Add(new(leftQuery));
                        if (rightQuery != null) queries.Add(new(rightQuery));
                        return queries.Count > 0 ? new BoolQuery { Should = queries } : null;
                    }
                default:
                    return ParseComparison(binary, exprType, fieldPrefix);
            }
        }

        private static QueryBase? ParseComparison(BinaryExpression binary, Type? exprType, string? fieldPrefix)
        {
            var left = ParseExpression(binary.Left, exprType, fieldPrefix) as ITermQuery;
            var right = ParseExpression(binary.Right, exprType, fieldPrefix) as ITermQuery;

            var field = left?.Field ?? right?.Field;
            var value = right?.Value ?? left?.Value;

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

                "Contains" =>
                    ParseContains(call, exprType, fieldPrefix),

                "MultiMatch" =>
                    ParseMultiMatch(call, exprType, fieldPrefix),

                "Any" =>
                    ParseAny(call, exprType, fieldPrefix),

                _ => new TermQuery { Value = EvaluateExpression(call) }
            };
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

        private static QueryBase? ParseContains(MethodCallExpression call, Type? exprType, string? fieldPrefix)
        {
            var cField = ParseExpression(call.Object, exprType, fieldPrefix) as ITermQuery;
            var cVal = ParseExpression(call.Arguments.First(), exprType, fieldPrefix) as ITermQuery;

            if (cField?.Field == null || cVal?.Value == null)
                return null;

            return new QueryStringQuery { DefaultField = cField.Field, Query = (string)cVal.Value };
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

            // Constant or closure member access (e.g., x => x.Name == localVariable)
            if (member.Expression is ConstantExpression || member.Expression == null)
            {
                var value = EvaluateExpression(member);
                return value != null ? new TermQuery { Value = value } : null;
            }

            // Try to evaluate, otherwise recurse
            try
            {
                var value = EvaluateExpression(member);
                return value != null ? new TermQuery { Value = value } : null;
            }
            catch
            {
                return ParseExpression(member.Expression, null, fieldPrefix);
            }
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
            var operandQuery = ParseExpression(unary.Operand, exprType, fieldPrefix);
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
                    member.Expression.NodeType == ExpressionType.TypeAs);
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

        private static object? EvaluateExpression(Expression expr)
        {
            if (expr is ConstantExpression c)
                return c.Value;

            if (expr is MemberExpression m && m.Expression is ConstantExpression mc)
            {
                if (m.Member is FieldInfo fi)
                    return fi.GetValue(mc.Value);
                if (m.Member is PropertyInfo pi)
                    return pi.GetValue(mc.Value);
            }

            // Use expression string as cache key (Expression doesn't implement GetHashCode/Equals)
            var cacheKey = expr.ToString();
            var func = _expressionCache.GetOrAdd(cacheKey, _ =>
            {
                var lambda = Expression.Lambda(expr);
                return (Func<object>)lambda.Compile();
            });

            return func.DynamicInvoke();
        }
    }
}
