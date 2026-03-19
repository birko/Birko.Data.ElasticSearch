using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Birko.Data.ElasticSearch.IndexManagement
{
    /// <summary>
    /// Provides index management utilities for Elasticsearch.
    /// Supports creating, deleting, configuring indices, managing aliases, and templates.
    /// </summary>
    public class IndexManager
    {
        private readonly ElasticClient _client;

        /// <summary>
        /// Initializes a new instance of <see cref="IndexManager"/>.
        /// </summary>
        /// <param name="client">The Elasticsearch client.</param>
        public IndexManager(ElasticClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
        }

        #region Index CRUD

        /// <summary>
        /// Checks if an index exists.
        /// </summary>
        public bool IndexExists(string indexName)
        {
            ValidateIndexName(indexName);
            return _client.Indices.Exists(indexName).Exists;
        }

        /// <summary>
        /// Checks if an index exists asynchronously.
        /// </summary>
        public async Task<bool> IndexExistsAsync(string indexName, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);
            var response = await _client.Indices.ExistsAsync(indexName, null, ct).ConfigureAwait(false);
            return response.Exists;
        }

        /// <summary>
        /// Creates an index with optional custom configuration.
        /// </summary>
        /// <param name="indexName">The name of the index to create.</param>
        /// <param name="descriptor">Optional index creation descriptor for mappings, settings, etc.</param>
        public void CreateIndex(string indexName, Func<CreateIndexDescriptor, ICreateIndexRequest>? descriptor = null)
        {
            ValidateIndexName(indexName);

            if (_client.Indices.Exists(indexName).Exists)
            {
                throw new InvalidOperationException($"Index '{indexName}' already exists.");
            }

            var response = descriptor != null
                ? _client.Indices.Create(indexName, descriptor)
                : _client.Indices.Create(indexName);

            ValidateResponse(response, $"create index '{indexName}'");
        }

        /// <summary>
        /// Creates an index asynchronously with optional custom configuration.
        /// </summary>
        public async Task CreateIndexAsync(string indexName, Func<CreateIndexDescriptor, ICreateIndexRequest>? descriptor = null, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);

            var exists = await _client.Indices.ExistsAsync(indexName, null, ct).ConfigureAwait(false);
            if (exists.Exists)
            {
                throw new InvalidOperationException($"Index '{indexName}' already exists.");
            }

            var response = descriptor != null
                ? await _client.Indices.CreateAsync(indexName, descriptor, ct).ConfigureAwait(false)
                : await _client.Indices.CreateAsync(indexName, null, ct).ConfigureAwait(false);

            ValidateResponse(response, $"create index '{indexName}'");
        }

        /// <summary>
        /// Creates an index with auto-mapping for the specified type.
        /// </summary>
        public void CreateIndex<T>(string indexName, int? numberOfShards = null, int? numberOfReplicas = null) where T : class
        {
            CreateIndex(indexName, c => c
                .Settings(s =>
                {
                    if (numberOfShards.HasValue)
                        s = s.NumberOfShards(numberOfShards.Value);
                    if (numberOfReplicas.HasValue)
                        s = s.NumberOfReplicas(numberOfReplicas.Value);
                    return s;
                })
                .Map<T>(m => m.AutoMap()));
        }

        /// <summary>
        /// Creates an index with auto-mapping asynchronously.
        /// </summary>
        public Task CreateIndexAsync<T>(string indexName, int? numberOfShards = null, int? numberOfReplicas = null, CancellationToken ct = default) where T : class
        {
            return CreateIndexAsync(indexName, c => c
                .Settings(s =>
                {
                    if (numberOfShards.HasValue)
                        s = s.NumberOfShards(numberOfShards.Value);
                    if (numberOfReplicas.HasValue)
                        s = s.NumberOfReplicas(numberOfReplicas.Value);
                    return s;
                })
                .Map<T>(m => m.AutoMap()), ct);
        }

        /// <summary>
        /// Deletes an index.
        /// </summary>
        public void DeleteIndex(string indexName)
        {
            ValidateIndexName(indexName);

            if (!_client.Indices.Exists(indexName).Exists)
                return;

            var response = _client.Indices.Delete(indexName);
            ValidateResponse(response, $"delete index '{indexName}'");
        }

        /// <summary>
        /// Deletes an index asynchronously.
        /// </summary>
        public async Task DeleteIndexAsync(string indexName, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);

            var exists = await _client.Indices.ExistsAsync(indexName, null, ct).ConfigureAwait(false);
            if (!exists.Exists)
                return;

            var response = await _client.Indices.DeleteAsync(indexName, null, ct).ConfigureAwait(false);
            ValidateResponse(response, $"delete index '{indexName}'");
        }

        #endregion

        #region Index State

        /// <summary>
        /// Opens a closed index.
        /// </summary>
        public void OpenIndex(string indexName)
        {
            ValidateIndexName(indexName);
            var response = _client.Indices.Open(indexName);
            ValidateResponse(response, $"open index '{indexName}'");
        }

        /// <summary>
        /// Opens a closed index asynchronously.
        /// </summary>
        public async Task OpenIndexAsync(string indexName, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);
            var response = await _client.Indices.OpenAsync(indexName, null, ct).ConfigureAwait(false);
            ValidateResponse(response, $"open index '{indexName}'");
        }

        /// <summary>
        /// Closes an open index. Closed indices consume minimal resources but cannot be read or written to.
        /// </summary>
        public void CloseIndex(string indexName)
        {
            ValidateIndexName(indexName);
            var response = _client.Indices.Close(indexName);
            ValidateResponse(response, $"close index '{indexName}'");
        }

        /// <summary>
        /// Closes an index asynchronously.
        /// </summary>
        public async Task CloseIndexAsync(string indexName, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);
            var response = await _client.Indices.CloseAsync(indexName, null, ct).ConfigureAwait(false);
            ValidateResponse(response, $"close index '{indexName}'");
        }

        #endregion

        #region Index Settings

        /// <summary>
        /// Updates dynamic index settings. The index must be open for most settings,
        /// or closed for static settings like number_of_shards.
        /// </summary>
        public void UpdateSettings(string indexName, Func<UpdateIndexSettingsDescriptor, IUpdateIndexSettingsRequest> settingsDescriptor)
        {
            ValidateIndexName(indexName);
            if (settingsDescriptor == null) throw new ArgumentNullException(nameof(settingsDescriptor));

            var response = _client.Indices.UpdateSettings(indexName, settingsDescriptor);
            ValidateResponse(response, $"update settings for '{indexName}'");
        }

        /// <summary>
        /// Updates dynamic index settings asynchronously.
        /// </summary>
        public async Task UpdateSettingsAsync(string indexName, Func<UpdateIndexSettingsDescriptor, IUpdateIndexSettingsRequest> settingsDescriptor, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);
            if (settingsDescriptor == null) throw new ArgumentNullException(nameof(settingsDescriptor));

            var response = await _client.Indices.UpdateSettingsAsync(indexName, settingsDescriptor, ct).ConfigureAwait(false);
            ValidateResponse(response, $"update settings for '{indexName}'");
        }

        /// <summary>
        /// Gets information about an index including document count, size, shards, replicas, and aliases.
        /// </summary>
        public IndexInfo GetIndexInfo(string indexName)
        {
            ValidateIndexName(indexName);

            var statsResponse = _client.Indices.Stats(indexName);
            ValidateResponse(statsResponse, $"get stats for '{indexName}'");

            var settingsResponse = _client.Indices.GetSettings(indexName);
            ValidateResponse(settingsResponse, $"get settings for '{indexName}'");

            var aliasResponse = _client.Indices.GetAlias(indexName);

            return BuildIndexInfo(indexName, statsResponse, settingsResponse, aliasResponse);
        }

        /// <summary>
        /// Gets information about an index asynchronously.
        /// </summary>
        public async Task<IndexInfo> GetIndexInfoAsync(string indexName, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);

            var statsTask = _client.Indices.StatsAsync(indexName, null, ct);
            var settingsTask = _client.Indices.GetSettingsAsync(indexName, null, ct);
            var aliasTask = _client.Indices.GetAliasAsync(indexName, null, ct);

            await Task.WhenAll(statsTask, settingsTask, aliasTask).ConfigureAwait(false);

            var statsResponse = await statsTask.ConfigureAwait(false);
            ValidateResponse(statsResponse, $"get stats for '{indexName}'");

            var settingsResponse = await settingsTask.ConfigureAwait(false);
            ValidateResponse(settingsResponse, $"get settings for '{indexName}'");

            var aliasResponse = await aliasTask.ConfigureAwait(false);

            return BuildIndexInfo(indexName, statsResponse, settingsResponse, aliasResponse);
        }

        private static IndexInfo BuildIndexInfo(string indexName, IndicesStatsResponse statsResponse, GetIndexSettingsResponse settingsResponse, GetAliasResponse aliasResponse)
        {
            var info = new IndexInfo { Name = indexName };

            if (statsResponse.Indices.TryGetValue(indexName, out var indexStats))
            {
                info.DocumentCount = indexStats.Primaries?.Documents?.Count ?? 0;
                info.SizeInBytes = (long)(indexStats.Primaries?.Store?.SizeInBytes ?? 0);
            }

            if (settingsResponse.Indices.TryGetValue(indexName, out var indexSettings))
            {
                info.NumberOfShards = indexSettings.Settings?.NumberOfShards ?? 0;
                info.NumberOfReplicas = indexSettings.Settings?.NumberOfReplicas ?? 0;
                info.RefreshInterval = indexSettings.Settings?.RefreshInterval?.ToString();
            }

            if (aliasResponse.IsValid && aliasResponse.Indices.TryGetValue(indexName, out var aliasInfo))
            {
                info.Aliases = aliasInfo.Aliases.Keys.ToList();
            }

            return info;
        }

        #endregion

        #region Mapping

        /// <summary>
        /// Updates the mapping for an index. Only additive changes are supported (new fields).
        /// </summary>
        public void UpdateMapping<T>(string indexName, Func<PutMappingDescriptor<T>, IPutMappingRequest> mappingDescriptor) where T : class
        {
            ValidateIndexName(indexName);
            if (mappingDescriptor == null) throw new ArgumentNullException(nameof(mappingDescriptor));

            var response = _client.Map(mappingDescriptor);
            ValidateResponse(response, $"update mapping for '{indexName}'");
        }

        /// <summary>
        /// Updates the mapping for an index asynchronously.
        /// </summary>
        public async Task UpdateMappingAsync<T>(string indexName, Func<PutMappingDescriptor<T>, IPutMappingRequest> mappingDescriptor, CancellationToken ct = default) where T : class
        {
            ValidateIndexName(indexName);
            if (mappingDescriptor == null) throw new ArgumentNullException(nameof(mappingDescriptor));

            var response = await _client.MapAsync(mappingDescriptor, ct).ConfigureAwait(false);
            ValidateResponse(response, $"update mapping for '{indexName}'");
        }

        #endregion

        #region Aliases

        /// <summary>
        /// Creates an alias pointing to an index.
        /// </summary>
        public void CreateAlias(string indexName, string aliasName)
        {
            ValidateIndexName(indexName);
            if (string.IsNullOrWhiteSpace(aliasName)) throw new ArgumentException("Alias name cannot be null or empty.", nameof(aliasName));

            var response = _client.Indices.PutAlias(indexName, aliasName);
            ValidateResponse(response, $"create alias '{aliasName}' on '{indexName}'");
        }

        /// <summary>
        /// Creates an alias asynchronously.
        /// </summary>
        public async Task CreateAliasAsync(string indexName, string aliasName, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);
            if (string.IsNullOrWhiteSpace(aliasName)) throw new ArgumentException("Alias name cannot be null or empty.", nameof(aliasName));

            var response = await _client.Indices.PutAliasAsync(indexName, aliasName, null, ct).ConfigureAwait(false);
            ValidateResponse(response, $"create alias '{aliasName}' on '{indexName}'");
        }

        /// <summary>
        /// Deletes an alias from an index.
        /// </summary>
        public void DeleteAlias(string indexName, string aliasName)
        {
            ValidateIndexName(indexName);
            if (string.IsNullOrWhiteSpace(aliasName)) throw new ArgumentException("Alias name cannot be null or empty.", nameof(aliasName));

            var response = _client.Indices.DeleteAlias(indexName, aliasName);
            if (!response.IsValid && response.ServerError?.Status != 404)
            {
                throw new InvalidOperationException($"Failed to delete alias '{aliasName}' from '{indexName}': {response.DebugInformation}", response.OriginalException);
            }
        }

        /// <summary>
        /// Deletes an alias asynchronously.
        /// </summary>
        public async Task DeleteAliasAsync(string indexName, string aliasName, CancellationToken ct = default)
        {
            ValidateIndexName(indexName);
            if (string.IsNullOrWhiteSpace(aliasName)) throw new ArgumentException("Alias name cannot be null or empty.", nameof(aliasName));

            var response = await _client.Indices.DeleteAliasAsync(indexName, aliasName, null, ct).ConfigureAwait(false);
            if (!response.IsValid && response.ServerError?.Status != 404)
            {
                throw new InvalidOperationException($"Failed to delete alias '{aliasName}' from '{indexName}': {response.DebugInformation}", response.OriginalException);
            }
        }

        /// <summary>
        /// Gets all aliases, optionally filtered by index name.
        /// Returns a dictionary of index name -> list of alias names.
        /// </summary>
        public IReadOnlyDictionary<string, IReadOnlyList<string>> GetAliases(string? indexName = null)
        {
            var response = indexName != null
                ? _client.Indices.GetAlias(indexName)
                : _client.Indices.GetAlias();

            if (!response.IsValid && response.ServerError?.Status != 404)
            {
                throw new InvalidOperationException($"Failed to get aliases: {response.DebugInformation}", response.OriginalException);
            }

            return BuildAliasMap(response);
        }

        /// <summary>
        /// Gets all aliases asynchronously.
        /// </summary>
        public async Task<IReadOnlyDictionary<string, IReadOnlyList<string>>> GetAliasesAsync(string? indexName = null, CancellationToken ct = default)
        {
            var response = indexName != null
                ? await _client.Indices.GetAliasAsync(indexName, null, ct).ConfigureAwait(false)
                : await _client.Indices.GetAliasAsync(null, null, ct).ConfigureAwait(false);

            if (!response.IsValid && response.ServerError?.Status != 404)
            {
                throw new InvalidOperationException($"Failed to get aliases: {response.DebugInformation}", response.OriginalException);
            }

            return BuildAliasMap(response);
        }

        /// <summary>
        /// Atomically swaps an alias from one index to another in a single operation.
        /// This enables zero-downtime index migrations.
        /// </summary>
        public void SwapAlias(string aliasName, string oldIndexName, string newIndexName)
        {
            if (string.IsNullOrWhiteSpace(aliasName)) throw new ArgumentException("Alias name cannot be null or empty.", nameof(aliasName));
            if (string.IsNullOrWhiteSpace(oldIndexName)) throw new ArgumentException("Index name cannot be null or empty.", nameof(oldIndexName));
            if (string.IsNullOrWhiteSpace(newIndexName)) throw new ArgumentException("Index name cannot be null or empty.", nameof(newIndexName));

            var response = _client.Indices.BulkAlias(b => b
                .Remove(r => r.Index(oldIndexName).Alias(aliasName))
                .Add(a => a.Index(newIndexName).Alias(aliasName)));

            ValidateResponse(response, $"swap alias '{aliasName}' from '{oldIndexName}' to '{newIndexName}'");
        }

        /// <summary>
        /// Atomically swaps an alias asynchronously.
        /// </summary>
        public async Task SwapAliasAsync(string aliasName, string oldIndexName, string newIndexName, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(aliasName)) throw new ArgumentException("Alias name cannot be null or empty.", nameof(aliasName));
            if (string.IsNullOrWhiteSpace(oldIndexName)) throw new ArgumentException("Index name cannot be null or empty.", nameof(oldIndexName));
            if (string.IsNullOrWhiteSpace(newIndexName)) throw new ArgumentException("Index name cannot be null or empty.", nameof(newIndexName));

            var response = await _client.Indices.BulkAliasAsync(b => b
                .Remove(r => r.Index(oldIndexName).Alias(aliasName))
                .Add(a => a.Index(newIndexName).Alias(aliasName)), ct).ConfigureAwait(false);

            ValidateResponse(response, $"swap alias '{aliasName}' from '{oldIndexName}' to '{newIndexName}'");
        }

        private static IReadOnlyDictionary<string, IReadOnlyList<string>> BuildAliasMap(GetAliasResponse response)
        {
            var result = new Dictionary<string, IReadOnlyList<string>>();

            if (response.IsValid && response.Indices != null)
            {
                foreach (var index in response.Indices)
                {
                    result[index.Key.ToString()] = index.Value.Aliases.Keys.ToList();
                }
            }

            return result;
        }

        #endregion

        #region Templates

        /// <summary>
        /// Creates or updates an index template.
        /// </summary>
        public void PutTemplate(string templateName, Func<PutIndexTemplateDescriptor, IPutIndexTemplateRequest> descriptor)
        {
            if (string.IsNullOrWhiteSpace(templateName)) throw new ArgumentException("Template name cannot be null or empty.", nameof(templateName));
            if (descriptor == null) throw new ArgumentNullException(nameof(descriptor));

            var response = _client.Indices.PutTemplate(templateName, descriptor);
            ValidateResponse(response, $"create template '{templateName}'");
        }

        /// <summary>
        /// Creates or updates an index template asynchronously.
        /// </summary>
        public async Task PutTemplateAsync(string templateName, Func<PutIndexTemplateDescriptor, IPutIndexTemplateRequest> descriptor, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(templateName)) throw new ArgumentException("Template name cannot be null or empty.", nameof(templateName));
            if (descriptor == null) throw new ArgumentNullException(nameof(descriptor));

            var response = await _client.Indices.PutTemplateAsync(templateName, descriptor, ct).ConfigureAwait(false);
            ValidateResponse(response, $"create template '{templateName}'");
        }

        /// <summary>
        /// Deletes an index template.
        /// </summary>
        public void DeleteTemplate(string templateName)
        {
            if (string.IsNullOrWhiteSpace(templateName)) throw new ArgumentException("Template name cannot be null or empty.", nameof(templateName));

            var response = _client.Indices.DeleteTemplate(templateName);
            if (!response.IsValid && response.ServerError?.Status != 404)
            {
                throw new InvalidOperationException($"Failed to delete template '{templateName}': {response.DebugInformation}", response.OriginalException);
            }
        }

        /// <summary>
        /// Deletes an index template asynchronously.
        /// </summary>
        public async Task DeleteTemplateAsync(string templateName, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(templateName)) throw new ArgumentException("Template name cannot be null or empty.", nameof(templateName));

            var response = await _client.Indices.DeleteTemplateAsync(templateName, null, ct).ConfigureAwait(false);
            if (!response.IsValid && response.ServerError?.Status != 404)
            {
                throw new InvalidOperationException($"Failed to delete template '{templateName}': {response.DebugInformation}", response.OriginalException);
            }
        }

        #endregion

        #region Cache, Refresh, Flush

        /// <summary>
        /// Clears the cache for one or all indices.
        /// </summary>
        public void ClearCache(string? indexName = null)
        {
            var response = indexName != null
                ? _client.Indices.ClearCache(indexName)
                : _client.Indices.ClearCache();

            ValidateResponse(response, "clear cache");
        }

        /// <summary>
        /// Clears the cache asynchronously.
        /// </summary>
        public async Task ClearCacheAsync(string? indexName = null, CancellationToken ct = default)
        {
            var response = indexName != null
                ? await _client.Indices.ClearCacheAsync(indexName, null, ct).ConfigureAwait(false)
                : await _client.Indices.ClearCacheAsync(null, null, ct).ConfigureAwait(false);

            ValidateResponse(response, "clear cache");
        }

        /// <summary>
        /// Refreshes an index, making recent operations visible to search.
        /// </summary>
        public void Refresh(string? indexName = null)
        {
            var response = indexName != null
                ? _client.Indices.Refresh(indexName)
                : _client.Indices.Refresh(Indices.All);

            ValidateResponse(response, "refresh index");
        }

        /// <summary>
        /// Refreshes an index asynchronously.
        /// </summary>
        public async Task RefreshAsync(string? indexName = null, CancellationToken ct = default)
        {
            var response = indexName != null
                ? await _client.Indices.RefreshAsync(indexName, null, ct).ConfigureAwait(false)
                : await _client.Indices.RefreshAsync(Indices.All, null, ct).ConfigureAwait(false);

            ValidateResponse(response, "refresh index");
        }

        /// <summary>
        /// Flushes an index, syncing the transaction log to permanent storage.
        /// </summary>
        public void Flush(string? indexName = null)
        {
            var response = indexName != null
                ? _client.Indices.Flush(indexName)
                : _client.Indices.Flush(Indices.All);

            ValidateResponse(response, "flush index");
        }

        /// <summary>
        /// Flushes an index asynchronously.
        /// </summary>
        public async Task FlushAsync(string? indexName = null, CancellationToken ct = default)
        {
            var response = indexName != null
                ? await _client.Indices.FlushAsync(indexName, null, ct).ConfigureAwait(false)
                : await _client.Indices.FlushAsync(Indices.All, null, ct).ConfigureAwait(false);

            ValidateResponse(response, "flush index");
        }

        #endregion

        #region Helpers

        private static void ValidateIndexName(string indexName)
        {
            if (string.IsNullOrWhiteSpace(indexName))
                throw new ArgumentException("Index name cannot be null or empty.", nameof(indexName));
        }

        private static void ValidateResponse(IResponse response, string operation)
        {
            if (!response.IsValid)
            {
                throw new InvalidOperationException(
                    $"Failed to {operation}: {response.DebugInformation}",
                    response.OriginalException);
            }
        }

        #endregion
    }
}
