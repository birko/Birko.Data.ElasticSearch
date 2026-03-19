using Nest;
using System;
using System.Diagnostics;
using System.Threading;
using System.Threading.Tasks;

namespace Birko.Data.ElasticSearch.IndexManagement
{
    /// <summary>
    /// Provides re-indexing utilities for Elasticsearch, including zero-downtime
    /// reindex via alias swap and server-side reindex operations.
    /// </summary>
    public class ReindexHelper
    {
        private readonly ElasticClient _client;
        private readonly IndexManager _indexManager;

        /// <summary>
        /// Initializes a new instance of <see cref="ReindexHelper"/>.
        /// </summary>
        /// <param name="client">The Elasticsearch client.</param>
        public ReindexHelper(ElasticClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _indexManager = new IndexManager(client);
        }

        /// <summary>
        /// Initializes a new instance of <see cref="ReindexHelper"/> with an existing <see cref="IndexManager"/>.
        /// </summary>
        public ReindexHelper(ElasticClient client, IndexManager indexManager)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
        }

        #region Basic Reindex

        /// <summary>
        /// Reindexes all documents from a source index to a target index using Elasticsearch's server-side reindex API.
        /// The target index must already exist.
        /// </summary>
        /// <param name="sourceIndex">The source index name.</param>
        /// <param name="targetIndex">The target index name.</param>
        /// <param name="waitForCompletion">Whether to wait for the reindex to complete. Default is true.</param>
        public ReindexResult Reindex(string sourceIndex, string targetIndex, bool waitForCompletion = true)
        {
            ValidateReindexArgs(sourceIndex, targetIndex);

            var sw = Stopwatch.StartNew();

            try
            {
                var response = _client.ReindexOnServer(r => r
                    .Source(s => s.Index(sourceIndex))
                    .Destination(d => d.Index(targetIndex))
                    .WaitForCompletion(waitForCompletion));

                sw.Stop();

                if (!response.IsValid)
                {
                    return ReindexResult.Failed(sourceIndex, targetIndex,
                        response.DebugInformation,
                        response.Created,
                        response.Failures?.Count ?? 0);
                }

                _client.Indices.Refresh(targetIndex);

                return ReindexResult.Successful(sourceIndex, targetIndex, response.Created, sw.Elapsed);
            }
            catch (Exception ex)
            {
                sw.Stop();
                return ReindexResult.Failed(sourceIndex, targetIndex, ex.Message);
            }
        }

        /// <summary>
        /// Reindexes all documents asynchronously.
        /// </summary>
        public async Task<ReindexResult> ReindexAsync(string sourceIndex, string targetIndex, bool waitForCompletion = true, CancellationToken ct = default)
        {
            ValidateReindexArgs(sourceIndex, targetIndex);

            var sw = Stopwatch.StartNew();

            try
            {
                var response = await _client.ReindexOnServerAsync(r => r
                    .Source(s => s.Index(sourceIndex))
                    .Destination(d => d.Index(targetIndex))
                    .WaitForCompletion(waitForCompletion), ct).ConfigureAwait(false);

                sw.Stop();

                if (!response.IsValid)
                {
                    return ReindexResult.Failed(sourceIndex, targetIndex,
                        response.DebugInformation,
                        response.Created,
                        response.Failures?.Count ?? 0);
                }

                await _client.Indices.RefreshAsync(targetIndex, null, ct).ConfigureAwait(false);

                return ReindexResult.Successful(sourceIndex, targetIndex, response.Created, sw.Elapsed);
            }
            catch (Exception ex)
            {
                sw.Stop();
                return ReindexResult.Failed(sourceIndex, targetIndex, ex.Message);
            }
        }

        /// <summary>
        /// Reindexes documents with a server-side script for transformation.
        /// </summary>
        /// <param name="sourceIndex">The source index name.</param>
        /// <param name="targetIndex">The target index name.</param>
        /// <param name="scriptSource">The Painless script source for transformation (e.g., "ctx._source.newField = ctx._source.oldField").</param>
        public ReindexResult ReindexWithScript(string sourceIndex, string targetIndex, string scriptSource)
        {
            ValidateReindexArgs(sourceIndex, targetIndex);
            if (string.IsNullOrWhiteSpace(scriptSource)) throw new ArgumentException("Script source cannot be null or empty.", nameof(scriptSource));

            var sw = Stopwatch.StartNew();

            try
            {
                var response = _client.ReindexOnServer(r => r
                    .Source(s => s.Index(sourceIndex))
                    .Destination(d => d.Index(targetIndex))
                    .Script(sc => sc.Source(scriptSource))
                    .WaitForCompletion(true));

                sw.Stop();

                if (!response.IsValid)
                {
                    return ReindexResult.Failed(sourceIndex, targetIndex,
                        response.DebugInformation,
                        response.Created,
                        response.Failures?.Count ?? 0);
                }

                _client.Indices.Refresh(targetIndex);

                return ReindexResult.Successful(sourceIndex, targetIndex, response.Created, sw.Elapsed);
            }
            catch (Exception ex)
            {
                sw.Stop();
                return ReindexResult.Failed(sourceIndex, targetIndex, ex.Message);
            }
        }

        /// <summary>
        /// Reindexes documents with a server-side script asynchronously.
        /// </summary>
        public async Task<ReindexResult> ReindexWithScriptAsync(string sourceIndex, string targetIndex, string scriptSource, CancellationToken ct = default)
        {
            ValidateReindexArgs(sourceIndex, targetIndex);
            if (string.IsNullOrWhiteSpace(scriptSource)) throw new ArgumentException("Script source cannot be null or empty.", nameof(scriptSource));

            var sw = Stopwatch.StartNew();

            try
            {
                var response = await _client.ReindexOnServerAsync(r => r
                    .Source(s => s.Index(sourceIndex))
                    .Destination(d => d.Index(targetIndex))
                    .Script(sc => sc.Source(scriptSource))
                    .WaitForCompletion(true), ct).ConfigureAwait(false);

                sw.Stop();

                if (!response.IsValid)
                {
                    return ReindexResult.Failed(sourceIndex, targetIndex,
                        response.DebugInformation,
                        response.Created,
                        response.Failures?.Count ?? 0);
                }

                await _client.Indices.RefreshAsync(targetIndex, null, ct).ConfigureAwait(false);

                return ReindexResult.Successful(sourceIndex, targetIndex, response.Created, sw.Elapsed);
            }
            catch (Exception ex)
            {
                sw.Stop();
                return ReindexResult.Failed(sourceIndex, targetIndex, ex.Message);
            }
        }

        #endregion

        #region Zero-Downtime Reindex

        /// <summary>
        /// Performs a zero-downtime reindex using the alias swap pattern:
        /// 1. Creates a new index with the specified mapping/settings.
        /// 2. Reindexes all documents from the current index behind the alias to the new index.
        /// 3. Atomically swaps the alias from the old index to the new index.
        /// 4. Optionally deletes the old index.
        /// </summary>
        /// <param name="aliasName">The alias that clients use to query. Must currently point to exactly one index.</param>
        /// <param name="newIndexName">The name for the new index.</param>
        /// <param name="newIndexDescriptor">The descriptor for the new index (mappings, settings).</param>
        /// <param name="deleteOldIndex">Whether to delete the old index after the swap. Default is false.</param>
        /// <param name="scriptSource">Optional Painless script for data transformation during reindex.</param>
        public ReindexResult ReindexWithAlias(
            string aliasName,
            string newIndexName,
            Func<CreateIndexDescriptor, ICreateIndexRequest>? newIndexDescriptor = null,
            bool deleteOldIndex = false,
            string? scriptSource = null)
        {
            ValidateAliasReindexArgs(aliasName, newIndexName);

            var oldIndexName = ResolveAliasToSingleIndex(aliasName);
            var sw = Stopwatch.StartNew();

            try
            {
                // Step 1: Create the new index
                _indexManager.CreateIndex(newIndexName, newIndexDescriptor);

                // Step 2: Reindex data
                ReindexResult reindexResult;
                if (!string.IsNullOrWhiteSpace(scriptSource))
                {
                    reindexResult = ReindexWithScript(oldIndexName, newIndexName, scriptSource!);
                }
                else
                {
                    reindexResult = Reindex(oldIndexName, newIndexName);
                }

                if (!reindexResult.Success)
                {
                    // Cleanup: delete the new index on failure
                    _indexManager.DeleteIndex(newIndexName);
                    return reindexResult;
                }

                // Step 3: Atomic alias swap
                _indexManager.SwapAlias(aliasName, oldIndexName, newIndexName);

                // Step 4: Optionally delete old index
                if (deleteOldIndex)
                {
                    _indexManager.DeleteIndex(oldIndexName);
                }

                sw.Stop();
                return ReindexResult.Successful(oldIndexName, newIndexName, reindexResult.DocumentsProcessed, sw.Elapsed);
            }
            catch (Exception ex)
            {
                sw.Stop();
                return ReindexResult.Failed(oldIndexName, newIndexName, ex.Message);
            }
        }

        /// <summary>
        /// Performs a zero-downtime reindex asynchronously using the alias swap pattern.
        /// </summary>
        public async Task<ReindexResult> ReindexWithAliasAsync(
            string aliasName,
            string newIndexName,
            Func<CreateIndexDescriptor, ICreateIndexRequest>? newIndexDescriptor = null,
            bool deleteOldIndex = false,
            string? scriptSource = null,
            CancellationToken ct = default)
        {
            ValidateAliasReindexArgs(aliasName, newIndexName);

            var oldIndexName = await ResolveAliasToSingleIndexAsync(aliasName, ct).ConfigureAwait(false);
            var sw = Stopwatch.StartNew();

            try
            {
                // Step 1: Create the new index
                await _indexManager.CreateIndexAsync(newIndexName, newIndexDescriptor, ct).ConfigureAwait(false);

                // Step 2: Reindex data
                ReindexResult reindexResult;
                if (!string.IsNullOrWhiteSpace(scriptSource))
                {
                    reindexResult = await ReindexWithScriptAsync(oldIndexName, newIndexName, scriptSource!, ct).ConfigureAwait(false);
                }
                else
                {
                    reindexResult = await ReindexAsync(oldIndexName, newIndexName, true, ct).ConfigureAwait(false);
                }

                if (!reindexResult.Success)
                {
                    await _indexManager.DeleteIndexAsync(newIndexName, ct).ConfigureAwait(false);
                    return reindexResult;
                }

                // Step 3: Atomic alias swap
                await _indexManager.SwapAliasAsync(aliasName, oldIndexName, newIndexName, ct).ConfigureAwait(false);

                // Step 4: Optionally delete old index
                if (deleteOldIndex)
                {
                    await _indexManager.DeleteIndexAsync(oldIndexName, ct).ConfigureAwait(false);
                }

                sw.Stop();
                return ReindexResult.Successful(oldIndexName, newIndexName, reindexResult.DocumentsProcessed, sw.Elapsed);
            }
            catch (Exception ex)
            {
                sw.Stop();
                return ReindexResult.Failed(oldIndexName, newIndexName, ex.Message);
            }
        }

        /// <summary>
        /// Performs a zero-downtime reindex with auto-mapping for a typed index.
        /// Creates the new index with auto-mapped settings for type T.
        /// </summary>
        public ReindexResult ReindexWithAlias<T>(
            string aliasName,
            string newIndexName,
            int? numberOfShards = null,
            int? numberOfReplicas = null,
            bool deleteOldIndex = false,
            string? scriptSource = null) where T : class
        {
            return ReindexWithAlias(
                aliasName,
                newIndexName,
                c => c
                    .Settings(s =>
                    {
                        if (numberOfShards.HasValue)
                            s = s.NumberOfShards(numberOfShards.Value);
                        if (numberOfReplicas.HasValue)
                            s = s.NumberOfReplicas(numberOfReplicas.Value);
                        return s;
                    })
                    .Map<T>(m => m.AutoMap()),
                deleteOldIndex,
                scriptSource);
        }

        /// <summary>
        /// Performs a zero-downtime reindex with auto-mapping asynchronously.
        /// </summary>
        public Task<ReindexResult> ReindexWithAliasAsync<T>(
            string aliasName,
            string newIndexName,
            int? numberOfShards = null,
            int? numberOfReplicas = null,
            bool deleteOldIndex = false,
            string? scriptSource = null,
            CancellationToken ct = default) where T : class
        {
            return ReindexWithAliasAsync(
                aliasName,
                newIndexName,
                c => c
                    .Settings(s =>
                    {
                        if (numberOfShards.HasValue)
                            s = s.NumberOfShards(numberOfShards.Value);
                        if (numberOfReplicas.HasValue)
                            s = s.NumberOfReplicas(numberOfReplicas.Value);
                        return s;
                    })
                    .Map<T>(m => m.AutoMap()),
                deleteOldIndex,
                scriptSource,
                ct);
        }

        #endregion

        #region Helpers

        private string ResolveAliasToSingleIndex(string aliasName)
        {
            var aliases = _indexManager.GetAliases();
            string? foundIndex = null;
            int count = 0;

            foreach (var kvp in aliases)
            {
                foreach (var alias in kvp.Value)
                {
                    if (string.Equals(alias, aliasName, StringComparison.OrdinalIgnoreCase))
                    {
                        foundIndex = kvp.Key;
                        count++;
                    }
                }
            }

            if (count == 0)
                throw new InvalidOperationException($"Alias '{aliasName}' does not exist or points to no index.");
            if (count > 1)
                throw new InvalidOperationException($"Alias '{aliasName}' points to {count} indices. Zero-downtime reindex requires the alias to point to exactly one index.");

            return foundIndex!;
        }

        private async Task<string> ResolveAliasToSingleIndexAsync(string aliasName, CancellationToken ct)
        {
            var aliases = await _indexManager.GetAliasesAsync(null, ct).ConfigureAwait(false);
            string? foundIndex = null;
            int count = 0;

            foreach (var kvp in aliases)
            {
                foreach (var alias in kvp.Value)
                {
                    if (string.Equals(alias, aliasName, StringComparison.OrdinalIgnoreCase))
                    {
                        foundIndex = kvp.Key;
                        count++;
                    }
                }
            }

            if (count == 0)
                throw new InvalidOperationException($"Alias '{aliasName}' does not exist or points to no index.");
            if (count > 1)
                throw new InvalidOperationException($"Alias '{aliasName}' points to {count} indices. Zero-downtime reindex requires the alias to point to exactly one index.");

            return foundIndex!;
        }

        private static void ValidateReindexArgs(string sourceIndex, string targetIndex)
        {
            if (string.IsNullOrWhiteSpace(sourceIndex)) throw new ArgumentException("Source index name cannot be null or empty.", nameof(sourceIndex));
            if (string.IsNullOrWhiteSpace(targetIndex)) throw new ArgumentException("Target index name cannot be null or empty.", nameof(targetIndex));
            if (string.Equals(sourceIndex, targetIndex, StringComparison.OrdinalIgnoreCase))
                throw new ArgumentException("Source and target index names must be different.");
        }

        private static void ValidateAliasReindexArgs(string aliasName, string newIndexName)
        {
            if (string.IsNullOrWhiteSpace(aliasName)) throw new ArgumentException("Alias name cannot be null or empty.", nameof(aliasName));
            if (string.IsNullOrWhiteSpace(newIndexName)) throw new ArgumentException("New index name cannot be null or empty.", nameof(newIndexName));
        }

        #endregion
    }
}
