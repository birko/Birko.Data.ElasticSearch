using Birko.Data.ElasticSearch.Stores;
using Birko.Data.Stores;
using System;

namespace Birko.Data.ElasticSearch.Repositories
{
    /// <summary>
    /// Async ElasticSearch repository with bulk operations support.
    /// Inherits from AbstractAsyncBulkRepository to provide bulk operations via ElasticSearch's async _bulk API.
    /// </summary>
    /// <typeparam name="TViewModel">The type of view model.</typeparam>
    /// <typeparam name="TModel">The type of data model.</typeparam>
    public class AsyncElasticSearchRepository<TViewModel, TModel> : Data.Repositories.AbstractAsyncBulkRepository<TViewModel, TModel>
        where TModel : Data.Models.AbstractModel, Data.Models.ILoadable<TViewModel>
        where TViewModel : Data.Models.ILoadable<TModel>
    {
        #region Properties

        /// <summary>
        /// Gets the ElasticSearch store with bulk operations support.
        /// </summary>
        public Stores.AsyncElasticSearchStore<TModel>? ElasticSearchStore => BulkStore as Stores.AsyncElasticSearchStore<TModel>;

        #endregion

        #region Constructors and Initialization
        /// <summary>
        /// Initializes a new instance with dependency injection support.
        /// </summary>
        /// <param name="store">The async ElasticSearch store to use for both regular and bulk operations.</param>

        public AsyncElasticSearchRepository(IAsyncStore<TModel>? store)
            : base((AsyncElasticSearchStore<TModel>?)store)
        {
            if (store is not null && store is not ElasticSearchStore<TModel>)
            {
                throw new ArgumentException(
                    "Store must be of type ElasticSearchStore<TModel> or null.",
                    nameof(store));
            }
        }

        #endregion
    }
}
