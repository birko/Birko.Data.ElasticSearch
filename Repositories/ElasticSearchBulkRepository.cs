using Nest;
using System;
using System.Collections.Generic;
using System.Text;

namespace Birko.Data.Repositories
{
    public abstract class ElasticSearchBulkRepository<TViewModel, TModel> 
        : AbstractBulkStoreRepository<TViewModel, TModel>
        where TModel : Models.AbstractModel, Models.ILoadable<TViewModel>
        where TViewModel : Models.ILoadable<TModel>
    {
        public ElasticSearchBulkRepository() : base()
        {

        }

        public virtual void BaseSettings(Stores.ISettings settings)
        {
            if (settings is Stores.ElasticSearch.Settings setts)
            {
                base.SetSettings(setts);
                Store = Stores.StoreLocator.GetStore<Stores.ElasticSearchBulkStore<TModel>, Stores.ElasticSearch.Settings>(setts);
            }
        }

        public override void SetSettings(Stores.ISettings settings)
        {
            if (settings is Stores.Settings setts)
            {
                BaseSettings(setts);
                Store?.Init();
            }
        }

        public virtual long Count(Nest.QueryContainer query)
        {
            var _store = Store;
            return (_store as Stores.ElasticSearchStore<TModel>)?.Count(query) ?? 0;
        }

        public virtual IEnumerable<TViewModel> Read(Nest.QueryContainer query, int? limit = null, int? offset = null, int maxResultWindow = 10000)
        {
            var _store = Store;
            Stores.ElasticSearchStore<TModel>.MaxResultWindow = maxResultWindow;
            foreach (var item in (_store as Stores.ElasticSearchBulkStore<TModel>).Read(query, limit, offset))
            {
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), Array.Empty<object>());
                result.LoadFrom(item);
                StoreHash(item);
                yield return result;
            }
        }

        public virtual IEnumerable<TViewModel> Read(Nest.SearchRequest request, Action<TViewModel> readAction, int maxResultWindow = 10000)
        {
            var _store = Store;
            Stores.ElasticSearchStore<TModel>.MaxResultWindow = maxResultWindow;
            foreach (var item in (_store as Stores.ElasticSearchBulkStore<TModel>).Read(request))
            {
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), Array.Empty<object>());
                result.LoadFrom(item);
                StoreHash(item);
                yield return result;
            }
        }

        public void ClearCache() {
            var _store = Store;
            (_store as Stores.ElasticSearchStore<TModel>)?.ClearCache();
        }
    }
}
