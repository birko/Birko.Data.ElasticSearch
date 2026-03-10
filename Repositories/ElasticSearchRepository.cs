using Birko.Data.ElasticSearch.Stores;
using Birko.Data.Repositories;
using Birko.Data.Stores;
using System;
using System.Collections.Generic;

namespace Birko.Data.ElasticSearch.Repositories
{
    /// <summary>
    /// ElasticSearch repository with bulk operations support.
    /// Inherits from AbstractBulkViewModelRepository to provide bulk operations via ElasticSearch's _bulk API.
    /// </summary>
    /// <typeparam name="TViewModel">The type of view model.</typeparam>
    /// <typeparam name="TModel">The type of data model.</typeparam>
    public abstract class ElasticSearchRepository<TViewModel, TModel> : AbstractBulkViewModelRepository<TViewModel, TModel>
        where TModel : Models.AbstractModel, Models.ILoadable<TViewModel>
        where TViewModel : Models.ILoadable<TModel>
    {
        #region Properties

        /// <summary>
        /// Gets the ElasticSearch store.
        /// This works with wrapped stores (e.g., tenant wrappers).
        /// </summary>
        public ElasticSearchStore<TModel>? ElasticSearchStore => Store?.GetUnwrappedStore<TModel, ElasticSearchStore<TModel>>();

        #endregion

        #region Constructors and Initialization
        /// <summary>
        /// Initializes a new instance with dependency injection support.
        /// </summary>
        /// <param name="store">The ElasticSearch store to use. Can be wrapped (e.g., by tenant wrappers).</param>
        public ElasticSearchRepository(IStore<TModel>? store)
            : base(null)
        {
            if (store != null && !store.IsStoreOfType<TModel, ElasticSearchStore<TModel>>())
            {
                throw new ArgumentException(
                    "Store must be of type ElasticSearchStore<TModel> or a wrapper around it (e.g., TenantStoreWrapper).",
                    nameof(store));
            }
            // Set the store after validation - base constructor handles null by creating default
            if (store != null)
            {
                Store = store;
            }
        }

        #endregion

        #region Query and Count Operations

        /// <summary>
        /// Counts documents matching the specified query container.
        /// </summary>
        /// <param name="query">The query container to match.</param>
        /// <returns>The count of matching documents.</returns>
        public virtual long Count(Nest.QueryContainer query)
        {
            var _store = Store;
            return (_store as ElasticSearchStore<TModel>)?.Count(query) ?? 0;
        }

        #endregion

        #region Index Management

        /// <summary>
        /// Clears the cache for the ElasticSearch index.
        /// </summary>
        public void ClearCache()
        {
            var _store = Store;
            (_store as ElasticSearchStore<TModel>)?.ClearCache();
        }

        #endregion

        #region Advanced Read Operations

        /// <summary>
        /// Reads documents using a custom search request.
        /// Note: This method requires implementation using ReadStream from the store.
        /// </summary>
        /// <param name="request">The search request.</param>
        /// <returns>A collection of matching view models.</returns>
        public virtual IEnumerable<TViewModel> Read(Nest.SearchRequest request)
        {
            if (ElasticSearchStore == null)
            {
                yield break;
            }

            foreach (var item in ElasticSearchStore.ReadStream(request))
            {
                yield return LoadInstance(item);
            }
        }

        #endregion
    }
}
