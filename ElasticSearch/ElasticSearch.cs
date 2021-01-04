using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

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


        public static QueryBase ParseExpression(Expression expr, QueryBase parentQuery = null , Type exprType = null)
        {
            if (expr != null)
            {
                if (expr is LambdaExpression lambdaExpression)
                {
                    var type = lambdaExpression.Parameters?.FirstOrDefault()?.Type;
                    return ParseExpression(lambdaExpression.Body, null,  type);
                }
                else if (expr is BinaryExpression binaryExpression)
                {
                    Nest.QueryBase q = null;
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
                            var termQuery = new TermQuery();
                            ParseExpression(binaryExpression.Left, termQuery, exprType);
                            ParseExpression(binaryExpression.Right, termQuery, exprType);
                            q = new NumericRangeQuery()
                            {
                                Field = termQuery.Field,
                                GreaterThan = (double?)termQuery.Value
                            };
                            break;
                        case ExpressionType.GreaterThanOrEqual:
                            var termQuery2 = new TermQuery();
                            ParseExpression(binaryExpression.Left, termQuery2, exprType);
                            ParseExpression(binaryExpression.Right, termQuery2, exprType);
                            q = new NumericRangeQuery()
                            {
                                Field = termQuery2.Field,
                                GreaterThanOrEqualTo = (double?)termQuery2.Value
                            };
                            break;
                        case ExpressionType.LessThan:
                            var termQuery3 = new TermQuery();
                            ParseExpression(binaryExpression.Left, termQuery3, exprType);
                            ParseExpression(binaryExpression.Right, termQuery3, exprType);
                            q = new NumericRangeQuery()
                            {
                                Field = termQuery3.Field,
                                LessThan = (double?)termQuery3.Value
                            };
                            break;
                        case ExpressionType.LessThanOrEqual:
                            var termQuery4 = new TermQuery();
                            ParseExpression(binaryExpression.Left, termQuery4, exprType);
                            ParseExpression(binaryExpression.Right, termQuery4, exprType);
                            q = new NumericRangeQuery()
                            {
                                Field = termQuery4.Field,
                                LessThanOrEqualTo = (double?)termQuery4.Value
                            };
                            break;
                        case ExpressionType.Equal:
                            q = new TermQuery();
                            ParseExpression(binaryExpression.Left, q, exprType);
                            ParseExpression(binaryExpression.Right, q, exprType);
                            break;
                        case ExpressionType.NotEqual:
                            var termQuery5 = new TermQuery();
                            ParseExpression(binaryExpression.Left, termQuery5, exprType);
                            ParseExpression(binaryExpression.Right, termQuery5, exprType);
                            q = new BoolQuery()
                            {
                                MustNot = new QueryContainer[] {
                                    termQuery5
                                }
                            };                         
                            break;
                        case ExpressionType.And:
                            break;
                        case ExpressionType.Or:
                            break;
                        case ExpressionType.AndAlso:
                            q = new BoolQuery()
                            {
                                Must = new[] {
                                    ParseExpression(binaryExpression.Left, null, exprType),
                                    ParseExpression(binaryExpression.Right, null, exprType)
                                }.Where(x => x != null).Select(x => new QueryContainer(x))
                            };
                            break;
                        case ExpressionType.OrElse:
                            q = new BoolQuery()
                            {
                                Should = new[] {
                                    ParseExpression(binaryExpression.Left, null, exprType),
                                    ParseExpression(binaryExpression.Right, null, exprType)
                                }.Where(x => x != null).Select(x => new QueryContainer(x))
                            };
                            break;
                    }

                    return q;
                }
                else if (expr is MethodCallExpression callExpression)
                {

                    var f = Expression.Lambda(callExpression).Compile();
                    var value = f.DynamicInvoke();
                    if (parentQuery is Nest.TermQuery constTempQuery)
                    {
                        constTempQuery.Value = value;
                    }
                    return  parentQuery;
                }
                else if (expr is UnaryExpression unaryExpression)
                {
                    if (unaryExpression.NodeType == ExpressionType.Convert)
                    {
                        return ParseExpression(unaryExpression.Operand, parentQuery, exprType);
                    }
                }
                else if (expr is MemberExpression memberExpression)
                {
                    string name = string.Empty;
                    if (
                        exprType != null
                        && memberExpression.Member.ReflectedType.IsAssignableFrom(exprType)
                        && (memberExpression.Expression.NodeType == ExpressionType.Parameter || memberExpression.Expression.NodeType == ExpressionType.TypeAs)
                    )
                    {
                        name = memberExpression.Member.Name;
                        name = name.First().ToString().ToLower() + name[1..];
                        if (parentQuery is Nest.IFieldNameQuery constTempQuery)
                        {
                            constTempQuery.Field = new Field(name);
                        }
                    }
                    if (string.IsNullOrEmpty(name))
                    {
                        if (memberExpression.Expression is ConstantExpression constantExpression)
                        {
                            Type type = constantExpression.Value.GetType();
                            var value = type.InvokeMember(memberExpression.Member.Name, BindingFlags.GetField, null, constantExpression.Value, null);
                            if (parentQuery is Nest.TermQuery constTempQuery)
                            {
                                constTempQuery.Value = value;
                            }
                            return null;
                        }
                        else if (memberExpression.Expression != null)
                        {
                            return ParseExpression(memberExpression.Expression, parentQuery); // not resending type here
                        }
                        else
                        {
                            var f = Expression.Lambda(memberExpression).Compile();
                            var value = f.DynamicInvoke();
                            if (parentQuery is Nest.TermQuery constTempQuery)
                            {
                                constTempQuery.Value = value;
                            }
                            return null;
                        }
                    }
                    else
                    {
                        return parentQuery;
                    }
                }
                else if (expr is ConstantExpression constantExpression)
                {
                    var value = constantExpression.Value;
                    if (parentQuery is Nest.TermQuery  constTempQuery)
                    {
                        constTempQuery.Value = value;
                    }
                    return null;
                }
            }
            return null;
        }
    }
}
