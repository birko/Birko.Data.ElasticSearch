using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Birko.Data.ElasticSearch.Extensions
{
    /// <summary>
    /// Query-DSL <b>marker</b> methods for use only inside an ElasticSearch store filter expression.
    /// They are recognized by name in <c>ElasticSearch.ParseExpression</c> and translated into the
    /// corresponding Nest query — the bodies are never executed. (CR-M088: the previous bodies tried
    /// to <c>Compile()</c> a <see cref="MemberExpression"/> that references a lambda parameter, which
    /// throws at runtime, so calling these outside an analyzed expression tree was always broken.)
    /// </summary>
    public static class EnumerableExtensions
    {
        private const string MarkerMessage =
            "This is a query DSL marker method; use it only inside an ElasticSearch store filter " +
            "expression, where it is translated to a Nest query and never executed directly.";

        public static bool MoreLikeThis(this IEnumerable<MemberExpression> fields, IEnumerable<string> likes, int minFrequency = 1, int maxFrequency= 12, string analyzer = "autocomplete")
            => throw new NotSupportedException(MarkerMessage);

        public static bool MultiMatch(this IEnumerable<MemberExpression> fields, string value)
            => throw new NotSupportedException(MarkerMessage);
    }
}
