using System.Collections.Generic;

namespace Birko.Data.ElasticSearch.Highlighting
{
    /// <summary>
    /// Options for configuring search result highlighting in ElasticSearch queries.
    /// </summary>
    public class HighlightOptions
    {
        /// <summary>
        /// Gets or sets the field names to highlight.
        /// </summary>
        public List<string> Fields { get; set; } = new List<string>();

        /// <summary>
        /// Gets or sets the pre-tag used to wrap highlighted terms. Defaults to "&lt;em&gt;".
        /// </summary>
        public string PreTag { get; set; } = "<em>";

        /// <summary>
        /// Gets or sets the post-tag used to wrap highlighted terms. Defaults to "&lt;/em&gt;".
        /// </summary>
        public string PostTag { get; set; } = "</em>";

        /// <summary>
        /// Gets or sets the size of each highlight fragment in characters. Defaults to 150.
        /// </summary>
        public int? FragmentSize { get; set; } = 150;

        /// <summary>
        /// Gets or sets the maximum number of fragments to return per field. Defaults to 3.
        /// </summary>
        public int? NumberOfFragments { get; set; } = 3;
    }
}
