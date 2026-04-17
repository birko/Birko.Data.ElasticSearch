using Birko.Data.Stores;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;

namespace Birko.Data.ElasticSearch.Aggregation
{
    /// <summary>
    /// Shared static helper for building and parsing ElasticSearch aggregations.
    /// Used by both <see cref="Stores.ElasticSearchStore{T}"/> and <see cref="Stores.AsyncElasticSearchStore{T}"/>
    /// to avoid duplicating aggregation logic.
    /// </summary>
    public static class StoreAggregationHelper
    {
        /// <summary>
        /// Builds a single metric aggregation container for the given function and field.
        /// Shared by store-level and view-level aggregation.
        /// </summary>
        public static AggregationContainer BuildSingleMetricAggregation(
            AggregateFunction function, string name, string? fieldName)
        {
            return function switch
            {
                AggregateFunction.Sum => new AggregationContainer { Sum = new SumAggregation(name, fieldName) },
                AggregateFunction.Avg => new AggregationContainer { Average = new AverageAggregation(name, fieldName) },
                AggregateFunction.Min => new AggregationContainer { Min = new MinAggregation(name, fieldName) },
                AggregateFunction.Max => new AggregationContainer { Max = new MaxAggregation(name, fieldName) },
                AggregateFunction.Count => new AggregationContainer { ValueCount = new ValueCountAggregation(name, fieldName ?? "_id") },
                _ => throw new NotSupportedException($"Aggregate function {function} is not supported")
            };
        }

        /// <summary>
        /// Builds metric aggregation containers (Sum, Avg, Min, Max, Count) from aggregate field definitions.
        /// </summary>
        /// <param name="aggregates">The aggregate field definitions.</param>
        /// <returns>An aggregation dictionary containing the metric aggregations.</returns>
        public static AggregationDictionary BuildMetricAggregations(IReadOnlyList<AggregateField> aggregates)
        {
            var dict = new AggregationDictionary();
            foreach (var agg in aggregates)
            {
                var alias = agg.ResolvedAlias;
                dict.Add(alias, BuildSingleMetricAggregation(agg.Function, alias, agg.SourcePropertyName));
            }
            return dict;
        }

        /// <summary>
        /// Builds a Terms or Composite aggregation container for GROUP BY operations.
        /// Uses Terms aggregation for single-field grouping, Composite for multi-field grouping.
        /// Shared by store-level and view-level aggregation.
        /// </summary>
        /// <param name="groupByFields">The field names to group by.</param>
        /// <param name="metricAggregations">The metric aggregations to nest inside the group-by aggregation.</param>
        /// <param name="size">Maximum number of buckets to return. Default is 10000.</param>
        /// <returns>An aggregation container for grouping.</returns>
        public static AggregationContainer BuildGroupByAggregation(
            IReadOnlyList<string> groupByFields,
            AggregationDictionary metricAggregations,
            int size = 10000)
        {
            if (groupByFields.Count == 1)
            {
                return new AggregationContainer
                {
                    Terms = new TermsAggregation("group_by")
                    {
                        Field = groupByFields[0],
                        Size = size,
                        Aggregations = metricAggregations
                    }
                };
            }

            var compositeSourceList = new List<ICompositeAggregationSource>();
            foreach (var field in groupByFields)
            {
                compositeSourceList.Add(new TermsCompositeAggregationSource(field) { Field = field });
            }
            return new AggregationContainer
            {
                Composite = new CompositeAggregation("group_by")
                {
                    Size = size,
                    Sources = compositeSourceList,
                    Aggregations = metricAggregations
                }
            };
        }

        /// <summary>
        /// Convenience overload that extracts group-by fields from an <see cref="AggregateQuery{T}"/>.
        /// </summary>
        public static AggregationContainer BuildGroupByAggregation<T>(
            AggregateQuery<T> query,
            AggregationDictionary metricAggregations)
            where T : Models.AbstractModel
        {
            return BuildGroupByAggregation(query.GroupByFields, metricAggregations);
        }

        /// <summary>
        /// Extracts metric values from a bucket into a dictionary.
        /// Shared by store-level and view-level result parsing.
        /// </summary>
        /// <param name="bucket">The bucket to extract metrics from.</param>
        /// <param name="aggregateNames">Map of aggregate name → function type.</param>
        /// <returns>Dictionary of alias → value.</returns>
        public static Dictionary<string, double?> ExtractMetricValues(
            BucketBase bucket,
            IEnumerable<(string Name, AggregateFunction Function)> aggregateNames)
        {
            var values = new Dictionary<string, double?>();
            foreach (var (name, function) in aggregateNames)
            {
                values[name] = function switch
                {
                    AggregateFunction.Sum => bucket.Sum(name)?.Value,
                    AggregateFunction.Avg => bucket.Average(name)?.Value,
                    AggregateFunction.Min => bucket.Min(name)?.Value,
                    AggregateFunction.Max => bucket.Max(name)?.Value,
                    AggregateFunction.Count => bucket.ValueCount(name)?.Value,
                    _ => null
                };
            }
            return values;
        }

        /// <summary>
        /// Extracts metric values from a response-level aggregate dictionary.
        /// Used for ungrouped (global) aggregation responses.
        /// </summary>
        public static Dictionary<string, double?> ExtractMetricValues(
            AggregateDictionary aggregations,
            IEnumerable<(string Name, AggregateFunction Function)> aggregateNames)
        {
            var values = new Dictionary<string, double?>();
            foreach (var (name, function) in aggregateNames)
            {
                values[name] = function switch
                {
                    AggregateFunction.Sum => aggregations.Sum(name)?.Value,
                    AggregateFunction.Avg => aggregations.Average(name)?.Value,
                    AggregateFunction.Min => aggregations.Min(name)?.Value,
                    AggregateFunction.Max => aggregations.Max(name)?.Value,
                    AggregateFunction.Count => aggregations.ValueCount(name)?.Value,
                    _ => null
                };
            }
            return values;
        }

        /// <summary>
        /// Parses the ElasticSearch search response containing aggregation results into a list of <see cref="AggregateResult"/>.
        /// Handles time-bucketed, grouped, and global aggregation responses.
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="response">The search response from ElasticSearch.</param>
        /// <param name="query">The original aggregate query.</param>
        /// <param name="hasGroupBy">Whether the query includes GROUP BY fields.</param>
        /// <param name="hasTimeBucket">Whether the query includes a time bucket interval.</param>
        /// <returns>A list of aggregate results.</returns>
        public static List<AggregateResult> ParseAggregateResponse<T>(
            ISearchResponse<T> response,
            AggregateQuery<T> query,
            bool hasGroupBy,
            bool hasTimeBucket)
            where T : Models.AbstractModel
        {
            var results = new List<AggregateResult>();

            if (hasTimeBucket)
            {
                var dateHistAgg = response.Aggregations.DateHistogram("time_bucket");
                if (dateHistAgg == null) return results;

                foreach (var timeBucket in dateHistAgg.Buckets)
                {
                    if (hasGroupBy)
                    {
                        var nestedResults = ParseGroupedBuckets(timeBucket, query);
                        foreach (var r in nestedResults)
                        {
                            var mutableValues = new Dictionary<string, object?>(r.Values)
                            {
                                ["bucket_time"] = timeBucket.Date
                            };
                            results.Add(new AggregateResult(mutableValues));
                        }
                    }
                    else
                    {
                        var row = new Dictionary<string, object?> { ["bucket_time"] = timeBucket.Date };
                        ExtractMetricsFromBucket(timeBucket, query.Aggregates, row);
                        results.Add(new AggregateResult(row));
                    }
                }
            }
            else if (hasGroupBy)
            {
                var termsAgg = response.Aggregations.Terms("group_by");
                if (termsAgg != null)
                {
                    foreach (var bucket in termsAgg.Buckets)
                    {
                        var row = new Dictionary<string, object?>();
                        if (query.GroupByFields.Count == 1)
                            row[query.GroupByFields[0]] = bucket.Key;
                        ExtractMetricsFromBucket(bucket, query.Aggregates, row);
                        results.Add(new AggregateResult(row));
                    }
                }
                else
                {
                    var compositeAgg = response.Aggregations.Composite("group_by");
                    if (compositeAgg != null)
                    {
                        foreach (var bucket in compositeAgg.Buckets)
                        {
                            var row = new Dictionary<string, object?>();
                            foreach (var field in query.GroupByFields)
                            {
                                if (bucket.Key.TryGetValue(field, out string keyValue))
                                    row[field] = keyValue;
                            }
                            ExtractMetricsFromBucket(bucket, query.Aggregates, row);
                            results.Add(new AggregateResult(row));
                        }
                    }
                }
            }
            else
            {
                var row = new Dictionary<string, object?>();
                foreach (var agg in query.Aggregates)
                {
                    var alias = agg.ResolvedAlias;
                    row[alias] = agg.Function switch
                    {
                        AggregateFunction.Sum => response.Aggregations.Sum(alias)?.Value,
                        AggregateFunction.Avg => response.Aggregations.Average(alias)?.Value,
                        AggregateFunction.Min => response.Aggregations.Min(alias)?.Value,
                        AggregateFunction.Max => response.Aggregations.Max(alias)?.Value,
                        AggregateFunction.Count => response.Aggregations.ValueCount(alias)?.Value,
                        _ => null
                    };
                }
                results.Add(new AggregateResult(row));
            }

            return results;
        }

        /// <summary>
        /// Parses grouped buckets (Terms or Composite) nested inside a parent bucket (e.g., date histogram bucket).
        /// </summary>
        /// <typeparam name="T">The entity type.</typeparam>
        /// <param name="parentBucket">The parent bucket containing nested group-by aggregations.</param>
        /// <param name="query">The original aggregate query.</param>
        /// <returns>A list of aggregate results from the grouped buckets.</returns>
        public static List<AggregateResult> ParseGroupedBuckets<T>(
            BucketBase parentBucket,
            AggregateQuery<T> query)
            where T : Models.AbstractModel
        {
            var results = new List<AggregateResult>();

            var termsAgg = parentBucket.Terms("group_by");
            if (termsAgg != null)
            {
                foreach (var bucket in termsAgg.Buckets)
                {
                    var row = new Dictionary<string, object?>();
                    if (query.GroupByFields.Count == 1)
                        row[query.GroupByFields[0]] = bucket.Key;
                    ExtractMetricsFromBucket(bucket, query.Aggregates, row);
                    results.Add(new AggregateResult(row));
                }
                return results;
            }

            var compositeAgg = parentBucket.Composite("group_by");
            if (compositeAgg != null)
            {
                foreach (var bucket in compositeAgg.Buckets)
                {
                    var row = new Dictionary<string, object?>();
                    foreach (var field in query.GroupByFields)
                    {
                        if (bucket.Key.TryGetValue(field, out string keyValue))
                            row[field] = keyValue;
                    }
                    ExtractMetricsFromBucket(bucket, query.Aggregates, row);
                    results.Add(new AggregateResult(row));
                }
            }

            return results;
        }

        /// <summary>
        /// Extracts metric values (Sum, Avg, Min, Max, Count) from a bucket into a row dictionary.
        /// </summary>
        /// <param name="bucket">The bucket to extract metrics from.</param>
        /// <param name="aggregates">The aggregate field definitions.</param>
        /// <param name="row">The dictionary to populate with metric values.</param>
        public static void ExtractMetricsFromBucket(
            BucketBase bucket,
            IReadOnlyList<AggregateField> aggregates,
            Dictionary<string, object?> row)
        {
            var values = ExtractMetricValues(bucket, aggregates.Select(a => (a.ResolvedAlias, a.Function)));
            foreach (var kvp in values)
            {
                row[kvp.Key] = kvp.Value;
            }
        }

        /// <summary>
        /// Parses a human-readable time interval string into a NEST <see cref="Time"/> object.
        /// Supports TimeSpan format (e.g., "01:00:00") and "value unit" format (e.g., "5 minutes", "1 h").
        /// Defaults to "1h" if the interval cannot be parsed.
        /// </summary>
        /// <param name="interval">The interval string to parse.</param>
        /// <returns>A NEST Time object representing the interval.</returns>
        public static Nest.Time ParseToTime(string interval)
        {
            var ts = Birko.Data.Stores.TimeIntervalParser.Parse(interval);
            if (ts > TimeSpan.Zero)
            {
                return new Nest.Time(ts);
            }

            return new Nest.Time("1h");
        }
    }
}
