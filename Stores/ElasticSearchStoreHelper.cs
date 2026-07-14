using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Birko.Data.ElasticSearch.Stores
{
    /// <summary>
    /// Shared, store-agnostic helpers used by both the sync <see cref="ElasticSearchStore{T}"/> and the
    /// async <see cref="AsyncElasticSearchStore{T}"/>. Extracted to remove the copy-paste drift risk called
    /// out by CR-L113 — the index-name resolution/sanitization and the <c>PropertyUpdate</c>-to-Painless
    /// script builder were near-identical copies across the two stores.
    /// </summary>
    internal static class ElasticSearchStoreHelper
    {
        /// <summary>
        /// Resolves the ElasticSearch index name for <paramref name="type"/> from the store settings,
        /// applying the ElasticSearch naming rules (see <see cref="SanitizeIndexName"/>).
        /// </summary>
        public static string ResolveIndexName(Settings? settings, Type type)
        {
            if (settings == null)
            {
                throw new InvalidOperationException("Settings not initialized. Call SetSettings() first.");
            }

            if (string.IsNullOrWhiteSpace(settings.Name))
            {
                throw new InvalidOperationException("Settings.Name cannot be empty");
            }

            string indexName = settings.IndexSettings?.FirstOrDefault(x => x.TypeName == type.FullName)?.Name ?? type.Name;
            return SanitizeIndexName($"{settings.Name}_{indexName}");
        }

        /// <summary>
        /// Sanitizes a raw index name according to ElasticSearch rules:
        /// lowercase; cannot start with <c>_</c>, <c>-</c>, or <c>.</c>; cannot contain spaces or any of
        /// <c># \ / * ? " &lt; &gt; | ` , +</c>. Throws when the result is empty or exceeds 255 characters.
        /// </summary>
        public static string SanitizeIndexName(string rawName)
        {
            var sanitizedIndexName = rawName
                .ToLower()
                .Trim()
                .Replace(" ", "_")
                .Replace("#", "_")
                .Replace("\\", "_")
                .Replace("/", "_")
                .Replace("*", "_")
                .Replace("?", "_")
                .Replace("\"", "_")
                .Replace("<", "_")
                .Replace(">", "_")
                .Replace("|", "_")
                .Replace(",", "_")
                .Replace("+", "_")
                .Replace("`", "_");

            // Remove invalid starting characters
            while (sanitizedIndexName.StartsWith("_") ||
                   sanitizedIndexName.StartsWith("-") ||
                   sanitizedIndexName.StartsWith("."))
            {
                sanitizedIndexName = sanitizedIndexName.Substring(1);
            }

            if (string.IsNullOrWhiteSpace(sanitizedIndexName) || sanitizedIndexName.Length > 255)
            {
                throw new InvalidOperationException(
                    $"Invalid index name generated: '{sanitizedIndexName}'. " +
                    $"Index names must be 1-255 characters and cannot contain special characters.");
            }

            return sanitizedIndexName;
        }

        /// <summary>
        /// Builds the inline Painless script (and its parameters) that applies a
        /// <see cref="Data.Stores.PropertyUpdate{T}"/> as a native UpdateByQuery script.
        /// </summary>
        public static (string Script, Dictionary<string, object> Parameters) BuildUpdateScript<T>(
            Data.Stores.PropertyUpdate<T> updates)
            where T : Data.Models.AbstractModel
        {
            var scriptParts = new List<string>();
            var scriptParams = new Dictionary<string, object>();

            foreach (var (property, value) in updates.Assignments)
            {
                var memberExpr = property.Body is UnaryExpression unary
                    ? (MemberExpression)unary.Operand
                    : (MemberExpression)property.Body;

                var fieldName = char.ToLowerInvariant(memberExpr.Member.Name[0]) + memberExpr.Member.Name.Substring(1);
                var paramName = "p_" + memberExpr.Member.Name;
                scriptParts.Add($"ctx._source.{fieldName} = params.{paramName}");
                scriptParams[paramName] = value ?? string.Empty;
            }

            return (string.Join("; ", scriptParts), scriptParams);
        }
    }
}
