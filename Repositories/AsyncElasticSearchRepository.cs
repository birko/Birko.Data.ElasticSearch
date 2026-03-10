using Birko.Data.ElasticSearch.Stores;
using Birko.Data.Stores;
using System;

namespace Birko.Data.ElasticSearch.Repositories
{
    /// <summary>
    /// Async ElasticSearch repository with bulk operations support.
    /// Inherits from AbstractAsyncBulkViewModelRepository to provide bulk operations via ElasticSearch's async _bulk API.
    /// </summary>
    /// <typeparam name="TViewModel">The type of view model.</typeparam>
    /// <typeparam name="TModel">The type of data model.</typeparam>
    public class AsyncElasticSearchRepository<TViewModel, TModel> : Data.Repositories.AbstractAsyncBulkViewModelRepository<TViewModel, TModel>
        where TModel : Data.Models.AbstractModel, Data.Models.ILoadable<TViewModel>
        where TViewModel : Data.Models.ILoadable<TModel>
    {
        #region Properties

        /// <summary>
        /// Gets the ElasticSearch store with bulk operations support.
        /// This works with wrapped stores (e.g., tenant wrappers).
        /// </summary>
        public Stores.AsyncElasticSearchStore<TModel>? ElasticSearchStore => Store?.GetUnwrappedStore<TModel, Stores.AsyncElasticSearchStore<TModel>>();

        #endregion

        #region Constructors and Initialization
        /// <summary>
        /// Initializes a new instance with dependency injection support.
        /// </summary>
        /// <param name="store">The async ElasticSearch store to use. Can be wrapped (e.g., by tenant wrappers).</param>

        public AsyncElasticSearchRepository(IAsyncStore<TModel>? store)
            : base(null)
        {
            if (store != null && !store.IsStoreOfType<TModel, AsyncElasticSearchStore<TModel>>())
            {
                throw new ArgumentException(
                    "Store must be of type AsyncElasticSearchStore<TModel> or a wrapper around it (e.g., AsyncTenantStoreWrapper).",
                    nameof(store));
            }
            // Set the store after validation - base constructor handles null by creating default
            if (store != null)
            {
                Store = store;
            }
        }

        #endregion
    }
}
