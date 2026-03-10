using Birko.Data.ElasticSearch.Stores;
using Birko.Data.Stores;
using System;

namespace Birko.Data.ElasticSearch.Repositories
{
    /// <summary>
    /// Async ElasticSearch repository for direct model access with bulk support.
    /// </summary>
    /// <typeparam name="T">The type of data model.</typeparam>
    public class AsyncElasticSearchModelRepository<T> : Data.Repositories.AbstractAsyncBulkRepository<T>
        where T : Data.Models.AbstractModel
    {
        /// <summary>
        /// Gets the ElasticSearch store.
        /// </summary>
        public Stores.AsyncElasticSearchStore<T>? ElasticSearchStore => Store?.GetUnwrappedStore<T, Stores.AsyncElasticSearchStore<T>>();

        public AsyncElasticSearchModelRepository(IAsyncStore<T>? store)
            : base(null)
        {
            if (store != null && !store.IsStoreOfType<T, AsyncElasticSearchStore<T>>())
            {
                throw new ArgumentException(
                    "Store must be of type AsyncElasticSearchStore<T> or a wrapper around it.",
                    nameof(store));
            }
            if (store != null)
            {
                Store = store;
            }
        }
    }
}
