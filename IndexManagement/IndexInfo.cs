using System;
using System.Collections.Generic;

namespace Birko.Data.ElasticSearch.IndexManagement
{
    /// <summary>
    /// Information about an Elasticsearch index.
    /// </summary>
    public class IndexInfo
    {
        /// <summary>
        /// Gets or sets the index name.
        /// </summary>
        public string Name { get; set; } = null!;

        /// <summary>
        /// Gets or sets the total number of documents in the index.
        /// </summary>
        public long DocumentCount { get; set; }

        /// <summary>
        /// Gets or sets the total size of the index in bytes.
        /// </summary>
        public long SizeInBytes { get; set; }

        /// <summary>
        /// Gets or sets the number of primary shards.
        /// </summary>
        public int NumberOfShards { get; set; }

        /// <summary>
        /// Gets or sets the number of replicas.
        /// </summary>
        public int NumberOfReplicas { get; set; }

        /// <summary>
        /// Gets or sets the list of aliases pointing to this index.
        /// </summary>
        public IReadOnlyList<string> Aliases { get; set; } = Array.Empty<string>();

        /// <summary>
        /// Gets or sets the health status of the index (green, yellow, red).
        /// </summary>
        public string Health { get; set; } = "unknown";

        /// <summary>
        /// Gets or sets the index state (open or close).
        /// </summary>
        public string State { get; set; } = "open";

        /// <summary>
        /// Gets or sets the refresh interval for the index.
        /// </summary>
        public string? RefreshInterval { get; set; }
    }

    /// <summary>
    /// Result of a reindex operation.
    /// </summary>
    public class ReindexResult
    {
        /// <summary>
        /// Gets or sets whether the reindex operation succeeded.
        /// </summary>
        public bool Success { get; set; }

        /// <summary>
        /// Gets or sets the number of documents processed.
        /// </summary>
        public long DocumentsProcessed { get; set; }

        /// <summary>
        /// Gets or sets the number of failures during reindexing.
        /// </summary>
        public long Failures { get; set; }

        /// <summary>
        /// Gets or sets the duration of the reindex operation.
        /// </summary>
        public TimeSpan Duration { get; set; }

        /// <summary>
        /// Gets or sets the source index name.
        /// </summary>
        public string SourceIndex { get; set; } = null!;

        /// <summary>
        /// Gets or sets the target index name.
        /// </summary>
        public string TargetIndex { get; set; } = null!;

        /// <summary>
        /// Gets or sets the error message if the operation failed.
        /// </summary>
        public string? ErrorMessage { get; set; }

        /// <summary>
        /// Creates a successful result.
        /// </summary>
        public static ReindexResult Successful(string source, string target, long documentsProcessed, TimeSpan duration)
        {
            return new ReindexResult
            {
                Success = true,
                SourceIndex = source,
                TargetIndex = target,
                DocumentsProcessed = documentsProcessed,
                Duration = duration
            };
        }

        /// <summary>
        /// Creates a failed result.
        /// </summary>
        public static ReindexResult Failed(string source, string target, string errorMessage, long documentsProcessed = 0, long failures = 0)
        {
            return new ReindexResult
            {
                Success = false,
                SourceIndex = source,
                TargetIndex = target,
                ErrorMessage = errorMessage,
                DocumentsProcessed = documentsProcessed,
                Failures = failures
            };
        }
    }
}
