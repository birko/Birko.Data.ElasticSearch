using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Birko.Data.ElasticSearch.Extensions
{
    public static class EnumerableExtensions
    {
        public static bool MoreLikeThis(this IEnumerable<MemberExpression> fields, IEnumerable<string> likes, int minFrequency = 1, int maxFrequency= 12, string analyzer = "autocomplete")
        {
            return likes.All(x => fields.MultiMatch(x));
        }

        public static bool MultiMatch(this IEnumerable<MemberExpression> fields, string value)
        {
            return fields.Any(f => Expression.Lambda<Func<string>>(f).Compile().Invoke().Contains(value));
        }
    }
}
