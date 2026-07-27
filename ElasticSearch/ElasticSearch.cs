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

        /// <summary>
        /// Translates a STORE FILTER into a query, enforcing <b>CR-H047</b>: a filter that was supplied but
        /// cannot be translated must never silently widen to match-all.
        /// </summary>
        /// <remarks>
        /// <para>
        /// <see cref="ParseExpression"/> returns <c>null</c> for any shape it cannot express — an
        /// unresolvable field, an unparseable operand, an unsupported node type. Assigning that straight to
        /// a request's <c>Query</c> produces a request with NO query, which ElasticSearch reads as
        /// match-all: a filtered / existence / permission check silently becomes a full-result set. On a
        /// <c>_delete_by_query</c> or <c>_update_by_query</c> the same null is far worse than a wrong read.
        /// </para>
        /// <para>
        /// The two meanings of "no rows" are deliberately kept apart, and only one of them is an error:
        /// a predicate that legitimately matches nothing (an empty or null collection in a
        /// <c>Contains</c>) is translated to an explicit <c>MatchNoneQuery</c> by
        /// <c>ParseContains</c> and passes through here untouched. Only "I could not express this" throws.
        /// </para>
        /// <para>
        /// A <c>null</c> <paramref name="filter"/> means no filter was supplied at all, which is a
        /// different thing again: it returns <c>null</c> so the caller can omit the query and read
        /// everything on purpose. Callers whose filter is mandatory — the destructive by-query paths —
        /// must use <see cref="ParseRequiredFilterQuery"/> instead.
        /// </para>
        /// </remarks>
        /// <exception cref="NotSupportedException">The filter was supplied but could not be translated.</exception>
        public static QueryBase? ParseFilterQuery<T>(Expression<Func<T, bool>>? filter)
        {
            if (filter == null)
            {
                return null;
            }

            QueryBase? query;
            try
            {
                query = ParseExpression(filter);
            }
            catch (Exception ex)
            {
                throw new NotSupportedException(UntranslatableFilterMessage(filter), ex);
            }

            if (query == null)
            {
                throw new NotSupportedException(UntranslatableFilterMessage(filter));
            }

            return query;
        }

        /// <summary>
        /// <see cref="ParseFilterQuery{T}"/> for operations where the filter is MANDATORY — the
        /// destructive <c>_delete_by_query</c> / <c>_update_by_query</c> paths. A missing filter there
        /// would mean "every document in the index", which is never an acceptable default for a
        /// destructive operation, so it is rejected rather than translated.
        /// </summary>
        public static QueryBase ParseRequiredFilterQuery<T>(Expression<Func<T, bool>> filter)
        {
            if (filter == null)
            {
                throw new ArgumentNullException(
                    nameof(filter),
                    "A filter is required for a by-query delete/update: a missing filter would target every document in the index.");
            }

            return ParseFilterQuery(filter)!;
        }

        private static string UntranslatableFilterMessage(Expression filter)
            => $"The filter expression could not be translated to an ElasticSearch query: {filter}. "
             + "Use a simpler filter (binary comparisons, && / ||, and the supported string/collection methods).";

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
            // Canonicalise once at the lambda boundary via the shared normalizer (Birko.Data.Core):
            // funcletize parameter-free subtrees and desugar boolean ternary (c ? t : f) and boolean
            // null-coalescing (a ?? b) into AND/OR/NOT, which the query builder below already handles.
            var body = Birko.Data.Expressions.ExpressionNormalizer.Normalize(lambda.Body) ?? lambda.Body;
            return ParsePredicate(body, type, fieldPrefix);
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
            // Value-expression operand — column arithmetic (x.A + x.B > 5), value null-coalescing
            // ((x.Score ?? 0) > 5) or a value-position ternary ((x.Vip ? x.A : x.B) > 5, i.e. CASE compared
            // to something). These cannot be expressed as a term/range query, so emit a Painless script
            // query instead. (Boolean-typed ?:/?? were already desugared to AND/OR by the normalizer.)
            if (IsScriptValueOperand(UnwrapConvertEs(binary.Left)) || IsScriptValueOperand(UnwrapConvertEs(binary.Right)))
                return BuildScriptComparison(binary, exprType, fieldPrefix);

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

        // ---- Painless script queries for value-expression operands (arithmetic / value-?? / value-CASE) ----

        private static Expression UnwrapConvertEs(Expression expr)
            => expr is UnaryExpression u && u.NodeType is ExpressionType.Convert or ExpressionType.ConvertChecked
                ? UnwrapConvertEs(u.Operand)
                : expr;

        private static bool IsArithmeticEs(ExpressionType type)
            => type is ExpressionType.Add or ExpressionType.AddChecked
                or ExpressionType.Subtract or ExpressionType.SubtractChecked
                or ExpressionType.Multiply or ExpressionType.MultiplyChecked
                or ExpressionType.Divide or ExpressionType.Modulo;

        /// <summary>True for an operand ES must render as a Painless value: arithmetic, coalescing, or a ternary.</summary>
        private static bool IsScriptValueOperand(Expression expr)
            => (expr is BinaryExpression b && (IsArithmeticEs(b.NodeType) || b.NodeType == ExpressionType.Coalesce))
                || expr is ConditionalExpression;

        private static bool IsNumericTypeEs(Type type)
        {
            type = Nullable.GetUnderlyingType(type) ?? type;
            return Type.GetTypeCode(type) is TypeCode.Byte or TypeCode.SByte
                or TypeCode.Int16 or TypeCode.UInt16 or TypeCode.Int32 or TypeCode.UInt32
                or TypeCode.Int64 or TypeCode.UInt64 or TypeCode.Single or TypeCode.Double or TypeCode.Decimal;
        }

        private static string? ScriptComparisonOperator(ExpressionType type) => type switch
        {
            ExpressionType.Equal => "==",
            ExpressionType.NotEqual => "!=",
            ExpressionType.LessThan => "<",
            ExpressionType.LessThanOrEqual => "<=",
            ExpressionType.GreaterThan => ">",
            ExpressionType.GreaterThanOrEqual => ">=",
            _ => null,
        };

        /// <summary>
        /// Builds a Painless <see cref="ScriptQuery"/> for a comparison whose operand(s) are value-expressions.
        /// Fields accessed outside a coalesce are collected and guarded with an existence precondition so a
        /// missing/null field yields <c>false</c> (matching C# null-propagation → the doc is excluded) instead
        /// of a Painless runtime error. Throws <see cref="NotSupportedException"/> for anything it cannot
        /// faithfully script, rather than silently dropping the filter.
        /// </summary>
        private static QueryBase? BuildScriptComparison(BinaryExpression binary, Type? exprType, string? fieldPrefix)
        {
            var op = ScriptComparisonOperator(binary.NodeType);
            if (op == null)
                throw new NotSupportedException($"Cannot script comparison operator {binary.NodeType} for ElasticSearch.");

            var required = new HashSet<string>();
            var leftScript = ScriptValue(binary.Left, exprType, fieldPrefix, required);
            var rightScript = ScriptValue(binary.Right, exprType, fieldPrefix, required);

            var body = $"({leftScript} {op} {rightScript})";
            var script = required.Count == 0
                ? body
                : $"({string.Join(" && ", required.Select(f => $"doc['{f}'].size() > 0"))}) ? {body} : false";

            return new ScriptQuery { Script = new InlineScript(script) };
        }

        private static string ScriptValue(Expression expr, Type? exprType, string? fieldPrefix, HashSet<string> required)
        {
            expr = UnwrapConvertEs(expr);
            switch (expr)
            {
                case ConditionalExpression cond:
                    return $"({ScriptBool(cond.Test, exprType, fieldPrefix, required)} ? "
                        + $"{ScriptValue(cond.IfTrue, exprType, fieldPrefix, required)} : "
                        + $"{ScriptValue(cond.IfFalse, exprType, fieldPrefix, required)})";
                case BinaryExpression b when IsArithmeticEs(b.NodeType):
                {
                    var op = b.NodeType switch
                    {
                        ExpressionType.Add or ExpressionType.AddChecked => "+",
                        ExpressionType.Subtract or ExpressionType.SubtractChecked => "-",
                        ExpressionType.Multiply or ExpressionType.MultiplyChecked => "*",
                        ExpressionType.Divide => "/",
                        ExpressionType.Modulo => "%",
                        _ => throw new NotSupportedException($"Unsupported arithmetic operator {b.NodeType}"),
                    };
                    return $"({ScriptValue(b.Left, exprType, fieldPrefix, required)} {op} {ScriptValue(b.Right, exprType, fieldPrefix, required)})";
                }
                case BinaryExpression b when b.NodeType == ExpressionType.Coalesce:
                {
                    // a ?? c — a's absence is handled here, so it is NOT added to the required-existence guard.
                    var inner = UnwrapConvertEs(b.Left);
                    if (inner is MemberExpression cm && TryScriptFieldName(cm, exprType, fieldPrefix, out var cfield))
                    {
                        var fallback = ScriptValue(b.Right, exprType, fieldPrefix, required);
                        return $"(doc['{cfield}'].size() == 0 ? {fallback} : doc['{cfield}'].value)";
                    }
                    if (!ContainsParameter(inner))
                    {
                        var lv = EvaluateExpression(inner);
                        return lv != null ? ScriptConstant(lv) : ScriptValue(b.Right, exprType, fieldPrefix, required);
                    }
                    throw new NotSupportedException("ElasticSearch script coalescing supports only `field ?? value`.");
                }
                case MemberExpression valueMember when valueMember.Member.Name == "Value"
                    && valueMember.Member.ReflectedType != null
                    && Nullable.GetUnderlyingType(valueMember.Member.ReflectedType) != null
                    && valueMember.Expression is MemberExpression innerNullable:
                    return ScriptValue(innerNullable, exprType, fieldPrefix, required);
                case MemberExpression m when TryScriptFieldName(m, exprType, fieldPrefix, out var field):
                    required.Add(field);
                    return $"doc['{field}'].value";
                default:
                    if (!ContainsParameter(expr))
                        return ScriptConstant(EvaluateExpression(expr));
                    throw new NotSupportedException($"Cannot translate operand '{expr}' into an ElasticSearch script value.");
            }
        }

        private static string ScriptBool(Expression expr, Type? exprType, string? fieldPrefix, HashSet<string> required)
        {
            expr = UnwrapConvertEs(expr);
            switch (expr)
            {
                case UnaryExpression u when u.NodeType == ExpressionType.Not:
                    return $"(!{ScriptBool(u.Operand, exprType, fieldPrefix, required)})";
                case BinaryExpression b when b.NodeType is ExpressionType.AndAlso or ExpressionType.And:
                    return $"({ScriptBool(b.Left, exprType, fieldPrefix, required)} && {ScriptBool(b.Right, exprType, fieldPrefix, required)})";
                case BinaryExpression b when b.NodeType is ExpressionType.OrElse or ExpressionType.Or:
                    return $"({ScriptBool(b.Left, exprType, fieldPrefix, required)} || {ScriptBool(b.Right, exprType, fieldPrefix, required)})";
                case BinaryExpression b when b.NodeType is ExpressionType.Equal or ExpressionType.NotEqual
                        && (IsNullConstantEs(b.Left) || IsNullConstantEs(b.Right)):
                {
                    var operand = UnwrapConvertEs(IsNullConstantEs(b.Right) ? b.Left : b.Right);
                    if (operand is MemberExpression nm && TryScriptFieldName(nm, exprType, fieldPrefix, out var nfield))
                        return b.NodeType == ExpressionType.Equal ? $"(doc['{nfield}'].size() == 0)" : $"(doc['{nfield}'].size() > 0)";
                    throw new NotSupportedException("ElasticSearch script null-check supports only a direct field.");
                }
                case BinaryExpression b when ScriptComparisonOperator(b.NodeType) is string cop:
                    return $"({ScriptValue(b.Left, exprType, fieldPrefix, required)} {cop} {ScriptValue(b.Right, exprType, fieldPrefix, required)})";
                case MemberExpression m when TryScriptFieldName(m, exprType, fieldPrefix, out var field)
                        && (Nullable.GetUnderlyingType(m.Type) ?? m.Type) == typeof(bool):
                    required.Add(field);
                    return $"doc['{field}'].value";
                default:
                    if (!ContainsParameter(expr) && EvaluateExpression(expr) is bool cb)
                        return cb ? "true" : "false";
                    throw new NotSupportedException($"Cannot translate boolean sub-expression '{expr}' into an ElasticSearch script.");
            }
        }

        private static bool IsNullConstantEs(Expression expr)
            => UnwrapConvertEs(expr) is ConstantExpression c && c.Value == null;

        private static bool TryScriptFieldName(MemberExpression member, Type? exprType, string? fieldPrefix, out string field)
        {
            field = string.Empty;
            if (exprType == null || !IsDirectMemberOfParameter(member, exprType))
                return false;
            var name = FormatFieldName(member.Member.Name, fieldPrefix);
            if (name == null)
                return false;
            if (member.Type == typeof(string) && !name.EndsWith(".keyword", StringComparison.OrdinalIgnoreCase))
                name += ".keyword";
            field = name;
            return true;
        }

        private static string ScriptConstant(object? value)
        {
            switch (value)
            {
                case null:
                    return "null";
                case bool b:
                    return b ? "true" : "false";
                case string s:
                    return "'" + s.Replace("\\", "\\\\").Replace("'", "\\'") + "'";
                case Enum e:
                    return Convert.ToInt64(e, System.Globalization.CultureInfo.InvariantCulture)
                        .ToString(System.Globalization.CultureInfo.InvariantCulture);
            }
            if (IsNumericTypeEs(value.GetType()))
                return Convert.ToString(value, System.Globalization.CultureInfo.InvariantCulture)!;
            throw new NotSupportedException($"Cannot inline a constant of type {value.GetType()} into an ElasticSearch script.");
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
            {
                // A NULL collection is the same case as an empty one: `null.Contains(x)` matches nothing,
                // as does `x.Tags.Contains(null)`. Returning null dropped the clause and widened the query
                // (see the MatchNone note below) — the identical defect, one branch earlier, reachable
                // whenever the collection variable is null rather than empty.
                return new MatchNoneQuery();
            }

            // A collection value → terms query (IN); a scalar → single term (array membership).
            if (value is System.Collections.IEnumerable enumerable && value is not string)
            {
                var terms = enumerable.Cast<object>().Where(v => v != null).ToArray();
                if (terms.Length > 0)
                    return new TermsQuery { Field = fieldQuery.Field, Terms = terms };

                // EMPTY collection → matches NOTHING. Returning null here was a silent-wrong-rows bug:
                // null means "no query produced", and CombineBool DROPS null sub-queries, so
                // `ids.Contains(x.F) && x.Status == active` with an empty `ids` collapsed to
                // `x.Status == active` — the membership filter vanished and the query returned everything
                // matching the remaining clauses. An empty `ids` is a normal outcome of the canonical
                // batch pattern (fetch parents, then filter children by their ids), so this was reachable
                // from ordinary code. `MatchNoneQuery` states "matches nothing" explicitly and survives
                // clause combination. Negation needs no special case: ParseNot wraps this in MustNot, and
                // "must not match nothing" is every document — the correct reading of an empty NOT IN.
                // (SQL had the same defect with a milder outcome — see Birko.Data.SQL 0801738, which
                // renders `1 = 0` / `1 = 1` for the same reason.)
                return new MatchNoneQuery();
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
            if (expr is ConditionalExpression ce)
                return ContainsParameter(ce.Test) || ContainsParameter(ce.IfTrue) || ContainsParameter(ce.IfFalse);
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
