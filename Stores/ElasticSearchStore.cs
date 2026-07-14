using Birko.Data.ElasticSearch.Aggregation;
using Birko.Data.ElasticSearch.Highlighting;
using Birko.Data.Stores;
using Birko.Configuration;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;

namespace Birko.Data.ElasticSearch.Stores
{
    /// <summary>
    /// ElasticSearch data store for CRUD operations.
    /// Use <see cref="ElasticSearchBulkStore{T}"/> for bulk operations support.
    /// This store provides basic CRUD operations.
    /// </summary>
    /// <typeparam name="T">The type of entity, must inherit from <see cref="Models.AbstractModel"/>.</typeparam>
    public class ElasticSearchStore<T>
        : AbstractBulkStore<T>
        , ISettingsStore<Settings>
        , IAggregatableStore<T>
         where T : Models.AbstractModel
    {
        /// <summary>
        /// Gets the ElasticClient instance.
        /// </summary>
        public ElasticClient? Connector { get; private set; }

        /// <summary>
        /// The settings for this store.
        /// </summary>
        protected Settings? _settings = null;


        #region Constructors and Initialization

        /// <summary>
        /// Initializes a new instance of the ElasticSearchStore class.
        /// </summary>
        public ElasticSearchStore() : base()
        {
        }

        /// <summary>
        /// Sets the connection settings for this store.
        /// </summary>
        /// <param name="settings">The settings to apply.</param>
        public virtual void SetSettings(ISettings settings)
        {
            if (settings is Settings sets)
            {
                _settings = sets;
                Connector = Data.ElasticSearch.ElasticSearch.GetClient(_settings);
            }
        }

        /// <summary>
        /// Sets the connection settings for this store.
        /// </summary>
        /// <param name="settings">The ElasticSearch settings to apply.</param>
        public virtual void SetSettings(Settings settings)
        {
            SetSettings((ISettings)settings);
        }

        /// <summary>
        /// Initializes the index with a custom descriptor.
        /// </summary>
        /// <param name="indexDescriptor">The index descriptor.</param>
        public void Init(Func<CreateIndexDescriptor, ICreateIndexRequest> indexDescriptor)
        {
            var indexName = GetIndexName();
            if (!Connector!.Indices.Exists(indexName).Exists)
            {
                var response = Connector!.Indices.Create(indexName, indexDescriptor);

                if (!response.IsValid || response.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"Failed to create index {indexName}. DebugInfo: {response.DebugInformation}",
                        response.OriginalException);
                }
            }
        }

        /// <inheritdoc />
        protected override void InitCore()
        {
            Init(cid =>
                cid.Map<T>(m => m.AutoMap())
            );
        }

        #endregion

        #region Core CRUD Operations - Single Item

        /// <inheritdoc />
        protected override Guid CreateCore(T data, StoreDataDelegate<T>? storeDelegate = null)
        {
            if (data == null) return Guid.Empty;

            data.Guid ??= Guid.NewGuid();
            storeDelegate?.Invoke(data);

            try
            {
                var indexName = GetIndexName();
                var response = Connector!.Create(data, i => i.Id(data.Guid).Index(indexName));

                if (!response.IsValid || response.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"ElasticSearch create failed. Index: {indexName}, Guid: {data.Guid}. " +
                        $"DebugInfo: {response.DebugInformation}",
                        response.OriginalException);
                }
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to create document in ElasticSearch", ex);
            }

            return data.Guid.Value;
        }

        /// <inheritdoc />
        protected override T? ReadCore(Expression<Func<T, bool>>? filter = null)
        {
            try
            {
                var indexName = GetIndexName();
                var query = new SearchRequest(indexName)
                {
                    Size = 1,
                    From = 0,
                    Query = Data.ElasticSearch.ElasticSearch.ParseExpression(filter)
                };
                var searchResponse = Connector!.Search<T>(query);

                if (!searchResponse.IsValid || searchResponse.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"ElasticSearch query failed. Index: {indexName}. DebugInfo: {searchResponse.DebugInformation}",
                        searchResponse.OriginalException);
                }

                return (searchResponse.Total > 0) ? searchResponse.Documents.FirstOrDefault() : null;
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to read from ElasticSearch", ex);
            }
        }

        /// <inheritdoc />
        protected override void UpdateCore(T data, StoreDataDelegate<T>? storeDelegate = null)
        {
            if (data == null || data.Guid == null || data.Guid == Guid.Empty) return;

            storeDelegate?.Invoke(data);

            try
            {
                var indexName = GetIndexName();
                var response = Connector!.Update<T, T>(data.Guid, (i) => i.Index(indexName).Doc(data));

                if (!response.IsValid || response.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"ElasticSearch update failed. Index: {indexName}, Guid: {data.Guid}. " +
                        $"DebugInfo: {response.DebugInformation}",
                        response.OriginalException);
                }
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to update document in ElasticSearch", ex);
            }
        }

        /// <inheritdoc />
        protected override void DeleteCore(T data)
        {
            if (data == null || data.Guid == null || data.Guid == Guid.Empty) return;

            try
            {
                var indexName = GetIndexName();
                var response = Connector!.Delete<T>(data.Guid, (i) => i.Index(indexName));

                if (!response.IsValid || response.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"ElasticSearch delete failed. Index: {indexName}, Guid: {data.Guid}. " +
                        $"DebugInfo: {response.DebugInformation}",
                        response.OriginalException);
                }
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to delete document from ElasticSearch", ex);
            }
        }

        #endregion

        #region Core CRUD Operations - Bulk

        protected override void CreateCore(IEnumerable<T> data, StoreDataDelegate<T>? storeDelegate = null)
        {
            if (data == null || Connector == null) return;

            var itemsToCreate = data.Where(x => x != null).Select(x =>
            {
                if (x.Guid == null || x.Guid == Guid.Empty)
                {
                    x.Guid = Guid.NewGuid();
                }
                storeDelegate?.Invoke(x);
                return x;
            });

            Bulk(itemsToCreate, null, null);
        }

        protected override IEnumerable<T> ReadCore(Expression<Func<T, bool>>? filter = null, OrderBy<T>? orderBy = null, int? limit = null, int? offset = null)
        {
            foreach (var item in ReadStream(filter, orderBy, limit, offset))
            {
                yield return item;
            }
        }

        protected override void UpdateCore(IEnumerable<T> data, StoreDataDelegate<T>? storeDelegate = null)
        {
            if (data == null || Connector == null) return;

            var itemsToUpdate = data.Where(x => x != null && x.Guid != null && x.Guid != Guid.Empty).Select(x =>
            {
                storeDelegate?.Invoke(x);
                return x;
            });

            Bulk(null, itemsToUpdate, null);
        }

        protected override void DeleteCore(IEnumerable<T> data)
        {
            if (data == null || Connector == null) return;

            var itemsToDelete = data.Where(x => x != null && x.Guid != null && x.Guid != Guid.Empty);
            Bulk(null, null, itemsToDelete);
        }

        /// <inheritdoc />
        public override void Delete(Expression<Func<T, bool>> filter)
        {
            // CR-L111: lazy-init the index, like every base CRUD method.
            EnsureInitialized();
            if (Connector == null) return;

            var indexName = GetIndexName();
            Connector.DeleteByQuery(new Nest.DeleteByQueryRequest(indexName)
            {
                Query = Data.ElasticSearch.ElasticSearch.ParseExpression(filter)
            });
        }

        /// <inheritdoc />
        public override void Update(Expression<Func<T, bool>> filter, Data.Stores.PropertyUpdate<T> updates)
        {
            // CR-L111: lazy-init the index, like every base CRUD method.
            EnsureInitialized();
            if (Connector == null || updates.Assignments.Count == 0) return;

            var indexName = GetIndexName();
            // CR-L113: the PropertyUpdate -> Painless script builder is shared with the async store.
            var (script, scriptParams) = ElasticSearchStoreHelper.BuildUpdateScript(updates);

            Connector.UpdateByQuery(new Nest.UpdateByQueryRequest(indexName)
            {
                Query = Data.ElasticSearch.ElasticSearch.ParseExpression(filter),
                Script = new Nest.InlineScript(script)
                {
                    Params = scriptParams
                }
            });
        }

        /// <summary>
        /// Performs async bulk operations on ElasticSearch.
        /// </summary>
        /// <param name="create">Items to create.</param>
        /// <param name="update">Items to update.</param>
        /// <param name="delete">Items to delete.</param>
        /// <param name="ct">Cancellation token.</param>
        protected void Bulk(
            IEnumerable<T>? create = null,
            IEnumerable<T>? update = null,
            IEnumerable<T>? delete = null)
        {
            var createList = create?.ToList() ?? Enumerable.Empty<T>();
            var updateList = update?.ToList() ?? Enumerable.Empty<T>();
            var deleteList = delete?.ToList() ?? Enumerable.Empty<T>();

            if (!createList.Any() && !updateList.Any() && !deleteList.Any())
            {
                return;
            }

            // Check bulk size limit
            var totalCount = createList.Count() + updateList.Count() + deleteList.Count();
            if (totalCount > Data.ElasticSearch.ElasticSearch.MaxBulkSize)
            {
                throw new ArgumentException(
                    $"Bulk operation size ({totalCount}) exceeds maximum allowed size ({Data.ElasticSearch.ElasticSearch.MaxBulkSize}). " +
                    $"Please split into smaller batches.");
            }

            var indexName = GetIndexName();
            var bulkResponse = Connector!.Bulk(b =>
            {
                if (createList.Any())
                {
                    b = b.CreateMany<T>(createList, (i, o) => i.Id(o.Guid).Index(indexName));
                }
                if (updateList.Any())
                {
                    b = b.UpdateMany<T>(updateList, (i, o) => i.Id(o.Guid).Index(indexName).Doc(o));
                }
                if (deleteList.Any())
                {
                    b = b.DeleteMany<T>(deleteList, (i, o) => i.Id(o.Guid).Index(indexName));
                }
                return b;
            });

            if (bulkResponse == null)
            {
                throw new InvalidOperationException("Bulk operation returned null response");
            }

            if (!bulkResponse.IsValid)
            {
                var errorItems = bulkResponse.ItemsWithErrors.Take(10);
                var errors = string.Join("\n", errorItems.Select(item =>
                    $"Index: {item.Index}, Id: {item.Id}, Error: {item.Error?.Reason ?? item.Error?.Type ?? "Unknown"}"));

                throw new InvalidOperationException(
                    $"Bulk operation failed. Index: {indexName}. Processed: {bulkResponse.Items.Count}, Errors: {bulkResponse.Errors}.\n" +
                    $"First few errors:\n{errors}",
                    bulkResponse.OriginalException);
            }

            // CR-L114: surface per-item failures inside an otherwise-valid bulk response, rather than
            // discarding them so the caller wrongly believes the whole batch succeeded. This matches the
            // single-item paths (which throw on failure) and the UnitOfWork commit path.
            // NOTE: ES bulk is not atomic — the successful items have already been persisted server-side
            // when this throws, so the exception signals "at least one item failed", not a full rollback.
            if (bulkResponse.Errors && bulkResponse.ItemsWithErrors.Any())
            {
                var errorCount = bulkResponse.ItemsWithErrors.Count();
                var partialErrors = string.Join("\n", bulkResponse.ItemsWithErrors.Take(10).Select(item =>
                    $"Index: {item.Index}, Id: {item.Id}, Error: {item.Error?.Reason ?? item.Error?.Type ?? "Unknown"}"));

                throw new InvalidOperationException(
                    $"Bulk operation completed with {errorCount} per-item error(s). Index: {indexName}.\n" +
                    $"First few errors:\n{partialErrors}");
            }
        }

        #endregion

        #region Query and Count Operations

        /// <inheritdoc />
        protected override long CountCore(Expression<Func<T, bool>>? filter = null)
        {
            return Count(filter != null ? Data.ElasticSearch.ElasticSearch.ParseExpression(filter) : null);
        }

        /// <summary>
        /// Counts documents matching the specified query.
        /// </summary>
        /// <param name="query">The query to match.</param>
        /// <returns>The count of matching documents.</returns>
        public long Count(QueryContainer? query)
        {
            var indexName = GetIndexName();
            var request = new CountRequest(indexName);
            if (query != null)
            {
                request.Query = query;
            }

            try
            {
                var response = Connector!.Count(request);
                if (!response.IsValid || response.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"ElasticSearch count failed. Index: {indexName}. DebugInfo: {response.DebugInformation}",
                        response.OriginalException);
                }
                return response.Count;
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to count documents in index {indexName}", ex);
            }
        }

        /// <summary>
        /// Streaming version of ReadAsync for large result sets.
        /// Use this for memory-efficient processing of large datasets.
        /// </summary>
        public IEnumerable<T> ReadStream(
            Expression<Func<T, bool>>? filter = null,
            OrderBy<T>? orderBy = null,
            int? limit = null,
            int? offset = null)
        {
            var query = Data.ElasticSearch.ElasticSearch.ParseExpression(filter);
            foreach (var item in ReadStream(query, orderBy, limit, offset))
            {
                yield return item;
            }
        }

        /// <summary>
        /// Asynchronously reads documents matching the specified query container as a stream.
        /// </summary>
        /// <param name="query">The query container.</param>
        /// <param name="limit">Maximum number of results.</param>
        /// <param name="offset">Number of results to skip.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>An async stream of matching documents.</returns>
        public IEnumerable<T> ReadStream(
            QueryContainer? query,
            OrderBy<T>? orderBy = null,
            int? limit = null,
            int? offset = null)
        {
            if (Connector == null) yield break;

            var indexName = GetIndexName();
            SearchRequest request = new SearchRequest(indexName)
            {
                Size = limit,
                From = offset
            };
            if (query != null)
            {
                request.Query = query;
            }
            if (orderBy != null && orderBy.Fields.Count > 0)
            {
                var sorts = new List<ISort>();
                foreach (var field in orderBy.Fields)
                {
                    sorts.Add(new FieldSort
                    {
                        Field = field.PropertyName,
                        Order = field.Descending ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
                request.Sort = sorts;
            }

            foreach (var item in ReadStream(request))
            {
                yield return item;
            }
        }

        /// <summary>
        /// Asynchronously reads documents using a custom search request with automatic scrolling.
        /// </summary>
        /// <param name="request">The search request.</param>
        /// <param name="ct">Cancellation token.</param>
        /// <returns>An async stream of documents matching the query.</returns>
        public IEnumerable<T> ReadStream(SearchRequest request)
        {
            if (request == null || Connector == null) yield break;

            string? scrollId = null;
            Nest.Time? scrollTime = null;
            int count = request.From ?? 0;
            int? size = request.Size;
            int skip = 0;

            try
            {
                var indexName = GetIndexName();
                var maxResultWindow = _settings?.IndexSettings
                    ?.FirstOrDefault(x => x.TypeName == typeof(T).FullName)?.MaxResultWindow ?? Data.ElasticSearch.ElasticSearch.MaxResultWindow;

                // Determine if we need to use scrolling
                if ((request.From == null && request.Size == null)
                   || ((request.Size ?? 0) + count) >= maxResultWindow)
                {
                    scrollTime = new Nest.Time(Data.ElasticSearch.ElasticSearch.DefaultScrollTime);
                    request.Scroll = scrollTime;
                    request.Size = Math.Min(request.Size ?? 1000, 1000);
                    request.From = null;
                    if (count != 0)
                    {
                        skip = count;
                    }
                }

                var searchResponse = Connector!.Search<T>(request);

                if (!searchResponse.IsValid || searchResponse.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"ElasticSearch search failed. Index: {indexName}. DebugInfo: {searchResponse.DebugInformation}",
                        searchResponse.OriginalException);
                }

                scrollId = searchResponse.ScrollId;
                long end = (size != null) ? (count + size.Value) : searchResponse.Total;
                if (end > searchResponse.Total)
                {
                    end = searchResponse.Total;
                }

                while (count < end)
                {
                    if (searchResponse.Documents.Count >= skip)
                    {
                        foreach (var document in searchResponse.Documents)
                        {
                            if (skip > 0)
                            {
                                skip--;
                                continue;
                            }
                            if (count >= end)
                            {
                                yield break;
                            }
                            yield return document;
                            count++;
                        }
                    }
                    else
                    {
                        skip -= Math.Min(skip, searchResponse.Documents.Count);
                    }

                    if (count >= end)
                    {
                        yield break;
                    }

                    // Fetch next page
                    if (!string.IsNullOrEmpty(scrollId) && scrollTime != null)
                    {
                        searchResponse = Connector!.Scroll<T>(new Nest.ScrollRequest(scrollId, scrollTime));

                        if (!searchResponse.IsValid || searchResponse.OriginalException != null)
                        {
                            throw new InvalidOperationException(
                                $"ElasticSearch scroll failed. Index: {indexName}. DebugInfo: {searchResponse.DebugInformation}",
                                searchResponse.OriginalException);
                        }

                        scrollId = searchResponse.ScrollId;
                    }
                    else
                    {
                        request.From = count;
                        searchResponse = Connector!.Search<T>(request);

                        if (!searchResponse.IsValid || searchResponse.OriginalException != null)
                        {
                            throw new InvalidOperationException(
                                $"ElasticSearch search failed. Index: {indexName}. DebugInfo: {searchResponse.DebugInformation}",
                                searchResponse.OriginalException);
                        }
                    }

                    if (searchResponse.Total <= 0 && !searchResponse.Documents.Any())
                    {
                        break;
                    }
                }
            }
            finally
            {
                // Always clean up scroll context
                if (!string.IsNullOrEmpty(scrollId) && Connector != null)
                {
                    try
                    {
                        Connector!.ClearScroll(new Nest.ClearScrollRequest(scrollId));
                    }
                    catch
                    {
                        // Log warning but don't throw
                    }
                }
            }
        }

        #endregion

        #region Highlighted Search

        /// <summary>
        /// Searches documents with highlight support, returning matched fragments for specified fields.
        /// </summary>
        /// <param name="filter">Optional filter expression.</param>
        /// <param name="highlightOptions">Options controlling which fields to highlight and how.</param>
        /// <param name="orderBy">Optional ordering.</param>
        /// <param name="limit">Maximum number of results.</param>
        /// <param name="offset">Number of results to skip.</param>
        /// <returns>Search results with highlighting information.</returns>
        public HighlightedSearchResults<T> SearchWithHighlights(
            Expression<Func<T, bool>>? filter,
            HighlightOptions highlightOptions,
            OrderBy<T>? orderBy = null,
            int? limit = null,
            int? offset = null)
        {
            if (highlightOptions == null) throw new ArgumentNullException(nameof(highlightOptions));
            if (Connector == null) return new HighlightedSearchResults<T>(Array.Empty<SearchResult<T>>(), 0);

            var query = Data.ElasticSearch.ElasticSearch.ParseExpression(filter);
            return SearchWithHighlights(query, highlightOptions, orderBy, limit, offset);
        }

        /// <summary>
        /// Searches documents with highlight support using a query container.
        /// </summary>
        /// <param name="query">The query container.</param>
        /// <param name="highlightOptions">Options controlling which fields to highlight and how.</param>
        /// <param name="orderBy">Optional ordering.</param>
        /// <param name="limit">Maximum number of results.</param>
        /// <param name="offset">Number of results to skip.</param>
        /// <returns>Search results with highlighting information.</returns>
        public HighlightedSearchResults<T> SearchWithHighlights(
            QueryContainer? query,
            HighlightOptions highlightOptions,
            OrderBy<T>? orderBy = null,
            int? limit = null,
            int? offset = null)
        {
            if (highlightOptions == null) throw new ArgumentNullException(nameof(highlightOptions));
            if (Connector == null) return new HighlightedSearchResults<T>(Array.Empty<SearchResult<T>>(), 0);

            var indexName = GetIndexName();
            var request = new SearchRequest(indexName)
            {
                Size = limit,
                From = offset
            };

            if (query != null)
            {
                request.Query = query;
            }

            if (orderBy != null && orderBy.Fields.Count > 0)
            {
                var sorts = new List<ISort>();
                foreach (var field in orderBy.Fields)
                {
                    sorts.Add(new FieldSort
                    {
                        Field = field.PropertyName,
                        Order = field.Descending ? SortOrder.Descending : SortOrder.Ascending
                    });
                }
                request.Sort = sorts;
            }

            // Build highlight configuration
            var highlightFields = new Dictionary<Field, IHighlightField>();
            foreach (var fieldName in highlightOptions.Fields)
            {
                highlightFields[fieldName] = new HighlightField
                {
                    FragmentSize = highlightOptions.FragmentSize,
                    NumberOfFragments = highlightOptions.NumberOfFragments
                };
            }

            request.Highlight = new Highlight
            {
                PreTags = new[] { highlightOptions.PreTag },
                PostTags = new[] { highlightOptions.PostTag },
                Fields = highlightFields
            };

            try
            {
                var searchResponse = Connector!.Search<T>(request);

                if (!searchResponse.IsValid || searchResponse.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"ElasticSearch highlighted search failed. Index: {indexName}. DebugInfo: {searchResponse.DebugInformation}",
                        searchResponse.OriginalException);
                }

                var hits = new List<SearchResult<T>>();
                foreach (var hit in searchResponse.Hits)
                {
                    var highlights = new Dictionary<string, IReadOnlyList<string>>();
                    if (hit.Highlight != null)
                    {
                        foreach (var kvp in hit.Highlight)
                        {
                            highlights[kvp.Key] = kvp.Value.ToList().AsReadOnly();
                        }
                    }

                    hits.Add(new SearchResult<T>(hit.Source, highlights, hit.Score));
                }

                return new HighlightedSearchResults<T>(hits.AsReadOnly(), searchResponse.Total);
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to execute highlighted search in ElasticSearch", ex);
            }
        }

        #endregion

        #region Index Management

        /// <summary>
        /// Gets the index name for this store.
        /// </summary>
        /// <returns>The validated and sanitized index name.</returns>
        public string GetIndexName()
        {
            // CR-L113: index-name resolution + sanitization is shared with the async store via the helper.
            return ElasticSearchStoreHelper.ResolveIndexName(_settings, typeof(T));
        }

        /// <summary>
        /// Deletes the index for this store.
        /// </summary>
        public void DeleteIndex()
        {
            DeleteIndex(GetIndexName());
        }

        /// <summary>
        /// Deletes the specified index.
        /// </summary>
        /// <param name="indexName">The name of the index to delete.</param>
        public void DeleteIndex(string indexName)
        {
            if (string.IsNullOrEmpty(indexName)) return;

            try
            {
                var response = Connector!.Indices.Delete(indexName);

                if (!response.IsValid || response.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"Failed to delete index {indexName}. DebugInfo: {response.DebugInformation}",
                        response.OriginalException);
                }
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to delete index {indexName}", ex);
            }
        }

        /// <summary>
        /// Clears the cache for the index.
        /// </summary>
        public void ClearCache()
        {
            var indexName = GetIndexName();
            try
            {
                var response = Connector!.Indices.ClearCache(indexName);

                if (!response.IsValid && response.OriginalException != null)
                {
                    throw new InvalidOperationException(
                        $"Failed to clear cache for index {indexName}. DebugInfo: {response.DebugInformation}",
                        response.OriginalException);
                }
            }
            catch (Exception ex) when (ex is InvalidOperationException)
            {
                throw;
            }
            catch (Exception ex)
            {
                throw new InvalidOperationException($"Failed to clear cache for index {indexName}", ex);
            }
        }

        /// <inheritdoc />
        public override void Destroy()
        {
            DeleteIndex();
        }

        #endregion

        #region Health and Utility

        /// <summary>
        /// Checks if the ElasticSearch cluster is healthy.
        /// </summary>
        /// <returns>True if the cluster is healthy, false otherwise.</returns>
        public bool IsHealthy()
        {
            try
            {
                var healthResponse = Connector!.Cluster.Health();
                return healthResponse.IsValid;
            }
            catch
            {
                return false;
            }
        }

        #endregion

        #region Aggregation

        /// <summary>
        /// Executes a synchronous aggregation query using native Elasticsearch aggregation API.
        /// Uses Terms/Composite aggregation for GROUP BY and metric aggregations for Sum/Avg/Min/Max/Count.
        /// </summary>
        public IReadOnlyList<AggregateResult> Aggregate(AggregateQuery<T> query)
        {
            if (Connector == null) return Array.Empty<AggregateResult>();

            var indexName = GetIndexName();

            QueryContainer? filterQuery = null;
            if (query.Filter != null)
            {
                filterQuery = Data.ElasticSearch.ElasticSearch.ParseExpression(query.Filter);
            }

            var metricAggregations = StoreAggregationHelper.BuildMetricAggregations(query.Aggregates);

            AggregationDictionary topLevelAggregations;
            bool hasGroupBy = query.GroupByFields.Count > 0;
            bool hasTimeBucket = !string.IsNullOrEmpty(query.TimeBucketInterval) && !string.IsNullOrEmpty(query.TimeColumn);

            if (hasTimeBucket)
            {
                var dateHistAgg = new DateHistogramAggregation("time_bucket")
                {
                    Field = query.TimeColumn,
                    FixedInterval = StoreAggregationHelper.ParseToTime(query.TimeBucketInterval!),
                    Aggregations = hasGroupBy
                        ? new AggregationDictionary { { "group_by", StoreAggregationHelper.BuildGroupByAggregation(query, metricAggregations) } }
                        : metricAggregations
                };
                topLevelAggregations = new AggregationDictionary
                {
                    { "time_bucket", new AggregationContainer { DateHistogram = dateHistAgg } }
                };
            }
            else if (hasGroupBy)
            {
                topLevelAggregations = new AggregationDictionary
                {
                    { "group_by", StoreAggregationHelper.BuildGroupByAggregation(query, metricAggregations) }
                };
            }
            else
            {
                topLevelAggregations = metricAggregations;
            }

            var searchRequest = new SearchRequest(indexName)
            {
                Size = 0,
                Query = filterQuery,
                Aggregations = topLevelAggregations
            };

            var response = Connector!.Search<T>(searchRequest);

            if (!response.IsValid || response.OriginalException != null)
            {
                throw new InvalidOperationException(
                    $"ElasticSearch aggregation query failed. Index: {indexName}. DebugInfo: {response.DebugInformation}",
                    response.OriginalException);
            }

            var results = StoreAggregationHelper.ParseAggregateResponse(response, query, hasGroupBy, hasTimeBucket);
            results = AggregateHelper.ApplyOrderingAndPaging(results, query.OrderBy, query.Offset, query.Limit);

            return results.AsReadOnly();
        }

        #endregion
    }
}
