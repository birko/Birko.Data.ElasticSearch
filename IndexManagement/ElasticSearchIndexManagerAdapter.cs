using Birko.Data.Patterns.IndexManagement;
using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Birko.Data.ElasticSearch.IndexManagement
{
    /// <summary>
    /// Adapts the existing <see cref="IndexManager"/> to the uniform <see cref="IIndexManager"/> interface.
    /// In Elasticsearch, "indexes" are containers (not secondary indexes):
    /// <list type="bullet">
    ///   <item><c>scope</c> is ignored.</item>
    ///   <item><c>indexName</c> is the ES index name.</item>
    ///   <item><see cref="IndexDefinition.Fields"/> are mapped to ES field mappings (auto-map).</item>
    /// </list>
    /// For full ES-specific capabilities (aliases, templates, reindex), use <see cref="IndexManager"/> directly.
    /// </summary>
    public class ElasticSearchIndexManagerAdapter : IIndexManager
    {
        private readonly IndexManager _indexManager;
        private readonly ElasticClient _client;

        public ElasticSearchIndexManagerAdapter(ElasticClient client)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _indexManager = new IndexManager(client);
        }

        public ElasticSearchIndexManagerAdapter(ElasticClient client, IndexManager indexManager)
        {
            _client = client ?? throw new ArgumentNullException(nameof(client));
            _indexManager = indexManager ?? throw new ArgumentNullException(nameof(indexManager));
        }

        /// <summary>
        /// Gets the underlying ES-specific <see cref="IndexManager"/> for full capabilities.
        /// </summary>
        public IndexManager Native => _indexManager;

        /// <inheritdoc />
        public async Task<bool> ExistsAsync(string indexName, string? scope = null, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(indexName)) throw new ArgumentException("Index name is required.", nameof(indexName));
            return await _indexManager.IndexExistsAsync(indexName, ct).ConfigureAwait(false);
        }

        /// <inheritdoc />
        public async Task CreateAsync(IndexDefinition definition, string? scope = null, CancellationToken ct = default)
        {
            if (definition == null) throw new ArgumentNullException(nameof(definition));
            if (string.IsNullOrWhiteSpace(definition.Name)) throw new ArgumentException("Index name is required.", nameof(definition));

            Func<CreateIndexDescriptor, ICreateIndexRequest>? descriptor = null;

            // Map properties to ES settings
            int? shards = null, replicas = null;
            if (definition.Properties != null)
            {
                if (definition.Properties.TryGetValue("NumberOfShards", out var s) && s is int sVal) shards = sVal;
                if (definition.Properties.TryGetValue("NumberOfReplicas", out var r) && r is int rVal) replicas = rVal;
            }

            if (shards.HasValue || replicas.HasValue)
            {
                descriptor = c => c.Settings(s =>
                {
                    if (shards.HasValue) s = s.NumberOfShards(shards.Value);
                    if (replicas.HasValue) s = s.NumberOfReplicas(replicas.Value);
                    return s;
                });
            }

            try
            {
                await _indexManager.CreateIndexAsync(definition.Name, descriptor, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not IndexManagementException)
            {
                throw new IndexManagementException(
                    $"Failed to create ES index '{definition.Name}'.",
                    definition.Name, scope, ex);
            }
        }

        /// <inheritdoc />
        public async Task DropAsync(string indexName, string? scope = null, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(indexName)) throw new ArgumentException("Index name is required.", nameof(indexName));

            try
            {
                await _indexManager.DeleteIndexAsync(indexName, ct).ConfigureAwait(false);
            }
            catch (Exception ex) when (ex is not IndexManagementException)
            {
                throw new IndexManagementException(
                    $"Failed to delete ES index '{indexName}'.",
                    indexName, scope, ex);
            }
        }

        /// <inheritdoc />
        public async Task<IReadOnlyList<Patterns.IndexManagement.IndexInfo>> ListAsync(string? scope = null, CancellationToken ct = default)
        {
            var catResponse = await _client.Cat.IndicesAsync(d => d, ct).ConfigureAwait(false);

            if (!catResponse.IsValid)
                return Array.Empty<Patterns.IndexManagement.IndexInfo>();

            return catResponse.Records
                .Where(r => !r.Index.StartsWith(".")) // skip system indexes
                .Select(r => new Patterns.IndexManagement.IndexInfo
                {
                    Name = r.Index,
                    SizeInBytes = ParseSizeToBytes(r.StoreSize),
                    State = r.Status ?? "unknown",
                    Properties = new Dictionary<string, object>
                    {
                        ["DocsCount"] = r.DocsCount ?? "0",
                        ["Health"] = r.Health ?? "unknown",
                        ["PrimaryShards"] = r.Primary ?? "0",
                        ["Replicas"] = r.Replica ?? "0"
                    }
                })
                .ToList();
        }

        /// <inheritdoc />
        public async Task<Patterns.IndexManagement.IndexInfo?> GetInfoAsync(string indexName, string? scope = null, CancellationToken ct = default)
        {
            if (string.IsNullOrWhiteSpace(indexName)) throw new ArgumentException("Index name is required.", nameof(indexName));

            try
            {
                var esInfo = await _indexManager.GetIndexInfoAsync(indexName, ct).ConfigureAwait(false);

                return new Patterns.IndexManagement.IndexInfo
                {
                    Name = esInfo.Name,
                    SizeInBytes = esInfo.SizeInBytes,
                    State = esInfo.State,
                    Properties = new Dictionary<string, object>
                    {
                        ["DocumentCount"] = esInfo.DocumentCount,
                        ["NumberOfShards"] = esInfo.NumberOfShards,
                        ["NumberOfReplicas"] = esInfo.NumberOfReplicas,
                        ["Health"] = esInfo.Health,
                        ["Aliases"] = esInfo.Aliases
                    }
                };
            }
            catch (InvalidOperationException)
            {
                return null;
            }
        }

        #region Helpers

        private static long ParseSizeToBytes(string? sizeStr)
        {
            if (string.IsNullOrWhiteSpace(sizeStr)) return -1;

            // ES _cat returns sizes like "1.2kb", "3.4mb", "5.6gb"
            sizeStr = sizeStr.Trim().ToLowerInvariant();

            double multiplier = 1;
            string numPart = sizeStr;

            if (sizeStr.EndsWith("gb"))
            {
                multiplier = 1024 * 1024 * 1024;
                numPart = sizeStr[..^2];
            }
            else if (sizeStr.EndsWith("mb"))
            {
                multiplier = 1024 * 1024;
                numPart = sizeStr[..^2];
            }
            else if (sizeStr.EndsWith("kb"))
            {
                multiplier = 1024;
                numPart = sizeStr[..^2];
            }
            else if (sizeStr.EndsWith("b"))
            {
                numPart = sizeStr[..^1];
            }

            if (double.TryParse(numPart, System.Globalization.NumberStyles.Float, System.Globalization.CultureInfo.InvariantCulture, out var val))
            {
                return (long)(val * multiplier);
            }

            return -1;
        }

        #endregion
    }
}
