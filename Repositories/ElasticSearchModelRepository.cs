using Birko.Data.ElasticSearch.Stores;
using Birko.Data.Repositories;
using Birko.Data.Stores;
using Birko.Configuration;
using System;
using System.Collections.Generic;

namespace Birko.Data.ElasticSearch.Repositories
{
    /// <summary>
    /// ElasticSearch repository for direct model access with bulk support.
    /// </summary>
    /// <typeparam name="T">The type of data model.</typeparam>
    public abstract class ElasticSearchModelRepository<T> : AbstractBulkRepository<T>
        where T : Models.AbstractModel
    {
        /// <summary>
        /// Gets the ElasticSearch store.
        /// </summary>
        public ElasticSearchStore<T>? ElasticSearchStore => Store?.GetUnwrappedStore<T, ElasticSearchStore<T>>();

        public ElasticSearchModelRepository(IStore<T>? store)
            : base(null)
        {
            if (store != null && !store.IsStoreOfType<T, ElasticSearchStore<T>>())
            {
                throw new ArgumentException(
                    "Store must be of type ElasticSearchStore<T> or a wrapper around it.",
                    nameof(store));
            }
            if (store != null)
            {
                Store = store;
            }
        }

        public virtual long Count(Nest.QueryContainer query)
        {
            return (Store as ElasticSearchStore<T>)?.Count(query) ?? 0;
        }

        public void ClearCache()
        {
            (Store as ElasticSearchStore<T>)?.ClearCache();
        }

        public virtual IEnumerable<T> Read(Nest.SearchRequest request)
        {
            if (ElasticSearchStore == null)
            {
                yield break;
            }

            foreach (var item in ElasticSearchStore.ReadStream(request))
            {
                yield return item;
            }
        }
    }
}
