using System.Collections.Generic;

namespace Birko.Data.ElasticSearch.Highlighting
{
    /// <summary>
    /// Represents a single search result with highlighting and relevance score.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    public class SearchResult<T> where T : class
    {
        /// <summary>
        /// Gets the matched document.
        /// </summary>
        public T Document { get; }

        /// <summary>
        /// Gets the highlighted fragments per field name.
        /// Each key is a field name, and the value is a list of highlighted text fragments.
        /// </summary>
        public IReadOnlyDictionary<string, IReadOnlyList<string>> Highlights { get; }

        /// <summary>
        /// Gets the relevance score of this result, if available.
        /// </summary>
        public double? Score { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="SearchResult{T}"/> class.
        /// </summary>
        /// <param name="document">The matched document.</param>
        /// <param name="highlights">The highlighted fragments per field.</param>
        /// <param name="score">The relevance score.</param>
        public SearchResult(T document, IReadOnlyDictionary<string, IReadOnlyList<string>> highlights, double? score)
        {
            Document = document;
            Highlights = highlights;
            Score = score;
        }
    }

    /// <summary>
    /// Represents the complete result set of a highlighted search query.
    /// </summary>
    /// <typeparam name="T">The document type.</typeparam>
    public class HighlightedSearchResults<T> where T : class
    {
        /// <summary>
        /// Gets the list of search result hits with highlighting.
        /// </summary>
        public IReadOnlyList<SearchResult<T>> Hits { get; }

        /// <summary>
        /// Gets the total number of documents matching the query.
        /// </summary>
        public long TotalCount { get; }

        /// <summary>
        /// Initializes a new instance of the <see cref="HighlightedSearchResults{T}"/> class.
        /// </summary>
        /// <param name="hits">The search result hits.</param>
        /// <param name="totalCount">The total matching document count.</param>
        public HighlightedSearchResults(IReadOnlyList<SearchResult<T>> hits, long totalCount)
        {
            Hits = hits;
            TotalCount = totalCount;
        }
    }
}
