using Birko.Data.Stores;
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
        public override void Init()
        {
            Init(cid =>
                cid.Map<T>(m => m.AutoMap())
            );
        }

        #endregion

        #region Core CRUD Operations - Single Item

        /// <inheritdoc />
        public override Guid Create(T data, StoreDataDelegate<T>? storeDelegate = null)
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
        public override T? Read(Expression<Func<T, bool>>? filter = null)
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
        public override void Update(T data, StoreDataDelegate<T>? storeDelegate = null)
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
        public override void Delete(T data)
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

        public override void Create(IEnumerable<T> data, StoreDataDelegate<T>? storeDelegate = null)
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

        public override IEnumerable<T> Read(Expression<Func<T, bool>>? filter = null, OrderBy<T>? orderBy = null, int? limit = null, int? offset = null)
        {
            foreach (var item in ReadStream(filter, orderBy, limit, offset))
            {
                yield return item;
            }
        }

        public override void Update(IEnumerable<T> data, StoreDataDelegate<T>? storeDelegate = null)
        {
            if (data == null || Connector == null) return;

            var itemsToUpdate = data.Where(x => x != null && x.Guid != null && x.Guid != Guid.Empty).Select(x =>
            {
                storeDelegate?.Invoke(x);
                return x;
            });

            Bulk(null, itemsToUpdate, null);
        }

        public override void Delete(IEnumerable<T> data)
        {
            if (data == null || Connector == null) return;

            var itemsToDelete = data.Where(x => x != null && x.Guid != null && x.Guid != Guid.Empty);
            Bulk(null, null, itemsToDelete);
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

            if (bulkResponse.Errors && bulkResponse.ItemsWithErrors.Any())
            {
                var errorCount = bulkResponse.ItemsWithErrors.Count();
                // TODO: Add logging here
            }
        }

        #endregion

        #region Query and Count Operations

        /// <inheritdoc />
        public override long Count(Expression<Func<T, bool>>? filter = null)
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

        #region Index Management

        /// <summary>
        /// Gets the index name for this store.
        /// </summary>
        /// <returns>The validated and sanitized index name.</returns>
        public string GetIndexName()
        {
            if (_settings == null)
            {
                throw new InvalidOperationException("Settings not initialized. Call SetSettings() first.");
            }

            if (string.IsNullOrWhiteSpace(_settings.Name))
            {
                throw new InvalidOperationException("Settings.Name cannot be empty");
            }

            var type = typeof(T);
            string indexName = _settings.IndexSettings?.FirstOrDefault(x => x.TypeName == type.FullName)?.Name ?? type.Name;

            // Sanitize index name according to ElasticSearch rules:
            // - Must be lowercase
            // - Cannot start with _, -, +
            // - Cannot contain spaces, #, \, /, *, ?, ", <, >, |, `, commas
            var sanitizedIndexName = $"{_settings.Name}_{indexName}"
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
    }
}
