using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Xml.Linq;

namespace Birko.Data.ElasticSearch
{
    public static class ElasticSearch
    {
        private static Dictionary<string, ElasticClient> _clients;

        public static ElasticClient GetClient(Stores.Settings settings)
        {
            if (_clients == null)
            {
                _clients = new Dictionary<string, ElasticClient>();
            }
            if (!_clients.ContainsKey(settings.GetId()))
            {
                var local = new Uri(settings.Location);
                ConnectionSettings clientSettings = new ConnectionSettings(local)
                        .DisableDirectStreaming();
                _clients.Add(settings.GetId(), new ElasticClient(clientSettings));
            }
            return _clients[settings.GetId()];
        }

        public static QueryBase ParseExpression(Expression? expr = null, Type? exprType = null, string fieldPrefix = null)
        {
            if (expr != null)
            {
                if (expr is LambdaExpression lambdaExpression)
                {
                    var type = lambdaExpression.Parameters?.FirstOrDefault()?.Type;
                    return ParseExpression(lambdaExpression.Body, type, fieldPrefix);
                }
                else if (expr is BinaryExpression binaryExpression)
                {
                    switch (binaryExpression.NodeType)
                    {
                        case ExpressionType.Add:
                        case ExpressionType.AddChecked:
                            break;
                        case ExpressionType.Subtract:
                        case ExpressionType.SubtractChecked:
                            break;
                        case ExpressionType.Multiply:
                        case ExpressionType.MultiplyChecked:
                            break;
                        case ExpressionType.Divide:
                            break;
                        case ExpressionType.Modulo:
                            break;
                        case ExpressionType.GreaterThan:
                            {
                                var tq1 = (ITermQuery)ParseExpression(binaryExpression.Left, exprType, fieldPrefix);
                                var tq2 = (ITermQuery)ParseExpression(binaryExpression.Right, exprType, fieldPrefix);
                                var value = tq2?.Value ?? tq1.Value;
                                return new NumericRangeQuery()
                                {
                                    Field = tq1?.Field ?? tq2.Field,
                                    GreaterThan = value != null ? Convert.ToDouble(value) : null
                                };
                            }
                        case ExpressionType.GreaterThanOrEqual:
                            {
                                var tq1 = (ITermQuery)ParseExpression(binaryExpression.Left, exprType, fieldPrefix);
                                var tq2 = (ITermQuery)ParseExpression(binaryExpression.Right, exprType, fieldPrefix);
                                var value = tq2?.Value ?? tq1.Value;
                                return new NumericRangeQuery()
                                {
                                    Field = tq1?.Field ?? tq2.Field,
                                    GreaterThanOrEqualTo = value != null ? Convert.ToDouble(value) : null
                                };
                            }
                        case ExpressionType.LessThan:
                            {
                                var tq1 = (ITermQuery)ParseExpression(binaryExpression.Left, exprType, fieldPrefix);
                                var tq2 = (ITermQuery)ParseExpression(binaryExpression.Right, exprType, fieldPrefix);
                                var value = tq2?.Value ?? tq1.Value;
                                return new NumericRangeQuery()
                                {
                                    Field = tq1?.Field ?? tq2.Field,
                                    LessThan = value != null ? Convert.ToDouble(value) : null
                                };
                            }
                        case ExpressionType.LessThanOrEqual:
                            {
                                var tq1 = (ITermQuery)ParseExpression(binaryExpression.Left, exprType, fieldPrefix);
                                var tq2 = (ITermQuery)ParseExpression(binaryExpression.Right, exprType, fieldPrefix);
                                var value = tq2?.Value ?? tq1.Value;
                                return new NumericRangeQuery()
                                {
                                    Field = tq1?.Field ?? tq2.Field,
                                    LessThanOrEqualTo = value != null ? Convert.ToDouble(value) : null
                                };
                            }
                        case ExpressionType.Equal:
                            {
                                var tq1 = (ITermQuery)ParseExpression(binaryExpression.Left, exprType, fieldPrefix);
                                var tq2 = (ITermQuery)ParseExpression(binaryExpression.Right, exprType, fieldPrefix);
                                return new TermQuery()
                                {
                                    Field = tq1?.Field ?? tq2.Field,
                                    Value = tq2?.Value ?? tq1.Value,
                                };
                            }
                        case ExpressionType.NotEqual:
                            {
                                var tq1 = (ITermQuery)ParseExpression(binaryExpression.Left, exprType, fieldPrefix);
                                var tq2 = (ITermQuery)ParseExpression(binaryExpression.Right, exprType, fieldPrefix);
                                return new BoolQuery()
                                {
                                    MustNot = new QueryContainer[] {
                                        new TermQuery() {
                                            Field = tq1?.Field ?? tq2.Field,
                                            Value = tq2?.Value ?? tq1.Value,
                                        }
                                    }
                                };
                            }
                        case ExpressionType.And:
                            break;
                        case ExpressionType.Or:
                            break;
                        case ExpressionType.AndAlso:
                            return new BoolQuery()
                            {
                                Must = new[] {
                                    ParseExpression(binaryExpression.Left, exprType, fieldPrefix),
                                    ParseExpression(binaryExpression.Right, exprType, fieldPrefix)
                                }.Where(x => x != null).Select(x => new QueryContainer(x))
                            };
                        case ExpressionType.OrElse:
                            return new BoolQuery()
                            {
                                Should = new[] {
                                    ParseExpression(binaryExpression.Left, exprType, fieldPrefix),
                                    ParseExpression(binaryExpression.Right, exprType, fieldPrefix)
                                }.Where(x => x != null).Select(x => new QueryContainer(x))
                            };
                    }
                }
                else if (expr is MethodCallExpression callExpression)
                {
                    if (callExpression.Method.Name == "Property")
                    {
                        if (callExpression.Arguments.LastOrDefault() is ConstantExpression propertyName)
                        {
                            var termQuery = new TermQuery();
                            var name = (string)propertyName.Value;
                            name = name.First().ToString().ToLower() + name[1..];
                            if (!string.IsNullOrEmpty(fieldPrefix))
                            {
                                name = $"{fieldPrefix}.{name}";
                            }
                            termQuery.Field = name;
                            return termQuery;
                        }
                    }
                    if (callExpression.Method.Name == "IsNullOrEmpty")
                    {
                        var field = (ITermQuery)ParseExpression(callExpression.Arguments.First(), exprType, fieldPrefix);
                        return new BoolQuery
                        {
                            MustNot = new[]
                            {
                                new QueryContainer(new WildcardQuery() {
                                    Field = field.Field,
                                    Value = "*"
                                })
                            }
                        };
                    }
                    if (callExpression.Method.Name == "StartsWith")
                    {
                        var q = new PrefixQuery();
                        var field = (ITermQuery)ParseExpression(callExpression.Object, exprType, fieldPrefix);
                        var val = (ITermQuery)ParseExpression(callExpression.Arguments.First(), exprType, fieldPrefix);
                        return new PrefixQuery
                        {
                            Field = field.Field,
                            Value = val.Value,
                        };
                    }
                    if (callExpression.Method.Name == "EndsWith")
                    {
                        //var q = new ();
                        //var field = ParseExpression(callExpression.Object, exprType, fieldPrefix);
                        //var val = ParseExpression(callExpression.Arguments.First(), exprType, fieldPrefix);
                        //return q;
                    }
                    if (callExpression.Method.Name == "Contains")
                    {
                        var field = (ITermQuery)ParseExpression(callExpression.Object, exprType, fieldPrefix);
                        var val = (ITermQuery)ParseExpression(callExpression.Arguments.First(), exprType, fieldPrefix);
                        return new QueryStringQuery()
                        {
                            DefaultField = field.Field,
                            Query = (string)val.Value
                        };
                    }
                    if (callExpression.Method.Name == "MultiMatch")
                    {
                        var termQuery = (ITermQuery)ParseExpression(callExpression.Arguments.Last(), exprType, fieldPrefix);
                        return new Nest.MultiMatchQuery()
                        {
                            Query = (string)termQuery.Value,
                            Fields = (callExpression.Arguments.First() as NewArrayExpression).Expressions
                                .Select(m => ((TermQuery)ParseExpression(m, exprType))?.Field).ToArray()
                        };
                    }
                    if (callExpression.Method.Name == "Any")
                    {
                        var termQuery = (ITermQuery)ParseExpression(callExpression.Arguments.First(), exprType, fieldPrefix);
                        var nestQuery = new NestedQuery
                        {
                            Path = termQuery.Field,
                            Query = ParseExpression(callExpression.Arguments.Last(), exprType, termQuery.Field.Name)
                        };
                        return nestQuery;
                    }

                    var f = Expression.Lambda(callExpression).Compile();
                    var value = f.DynamicInvoke();
                    return new TermQuery()
                    {
                        Value = value
                    };
                }
                else if (expr is MemberExpression memberExpression)
                {
                    if (
                        exprType != null
                        && memberExpression.Member.ReflectedType.IsAssignableFrom(exprType)
                        && (memberExpression.Expression.NodeType == ExpressionType.Parameter || memberExpression.Expression.NodeType == ExpressionType.TypeAs)
                    )
                    {
                        var name = memberExpression.Member.Name;
                        name = name.First().ToString().ToLower() + name[1..];
                        if (memberExpression.Type == typeof(string) && !name.EndsWith(".keyword"))
                        {
                            name += ".keyword";
                        }
                        if (!string.IsNullOrEmpty(fieldPrefix))
                        {
                            name = $"{fieldPrefix}.{name}";
                        }
                        return new TermQuery()
                        {
                            Field = name
                        };
                    }
                    if (memberExpression.Expression is ConstantExpression constantExpression)
                    {
                        Type type = constantExpression.Value.GetType();
                        var value = type.InvokeMember(memberExpression.Member.Name, BindingFlags.GetField | BindingFlags.GetProperty, null, constantExpression.Value, null);
                        return new TermQuery()
                        {
                            Value = value,
                        };
                    }
                    else if (memberExpression is MemberExpression memberExpression1)
                    {
                        var f = Expression.Lambda(memberExpression).Compile();
                        var value = f.DynamicInvoke();
                        return new TermQuery()
                        {
                            Value = value,
                        };
                    }
                    else if (memberExpression.Expression != null)
                    {
                        return ParseExpression(memberExpression.Expression, null, fieldPrefix); // not resending type here
                    }
                    else
                    {
                        var f = Expression.Lambda(memberExpression).Compile();
                        var value = f.DynamicInvoke();
                        return new TermQuery()
                        {
                            Value = value,
                        };
                    }
                }
                else if (expr is UnaryExpression unaryExpression)
                {
                    if (unaryExpression.NodeType == ExpressionType.Convert)
                    {
                        return ParseExpression(unaryExpression.Operand, exprType, fieldPrefix);
                    }
                    if (unaryExpression.NodeType == ExpressionType.Not)
                    {
                        return new BoolQuery()
                        {
                            MustNot = new QueryContainer[] {
                                ParseExpression(unaryExpression.Operand, exprType, fieldPrefix)
                            }
                        };
                    }
                }
                else if (expr is ConstantExpression constantExpression)
                {
                    var value = constantExpression.Value;
                    return new TermQuery()
                    {
                        Value = value,
                    };
                }
            }
            return null;
        }
    }
}
