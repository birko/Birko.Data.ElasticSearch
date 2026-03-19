using System;
using System.Collections.Generic;

namespace Birko.Data.ElasticSearch.Stores
{
    /// <summary>
    /// ElasticSearch-specific settings for store configuration.
    /// Extends the base <see cref="Birko.Configuration.Settings"/> with index configuration.
    /// </summary>
    public class Settings
        : Birko.Configuration.Settings
    {
        #region Properties

        /// <summary>
        /// Gets or sets the index-specific settings for different entity types.
        /// </summary>
        public IEnumerable<IndexSettings> IndexSettings { get; set; } = null!;

        #endregion
    }

    /// <summary>
    /// Configuration settings for a specific ElasticSearch index.
    /// </summary>
    public class IndexSettings
    {
        #region Properties

        /// <summary>
        /// Gets or sets the full type name for the entity.
        /// Used to map CLR types to ElasticSearch indices.
        /// </summary>
        public string TypeName { get; set; } = null!;

        /// <summary>
        /// Gets or sets the custom name for the index.
        /// If not specified, the type name will be used.
        /// </summary>
        public string Name { get; set; } = null!;

        /// <summary>
        /// Gets or sets the maximum result window for the index.
        /// This controls the maximum value of <c>from + size</c> for searches.
        /// Default is 10,000.
        /// </summary>
        public int? MaxResultWindow { get; set; }

        #endregion
    }
}
