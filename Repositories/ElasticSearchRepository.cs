using System;
using System.Collections.Generic;
using System.Text;

namespace Birko.Data.Repositories
{
    public abstract class ElasticSearchRepository<TViewModel, TModel> : AbstractStoreRepository<TViewModel, TModel>
        where TModel : Models.AbstractModel, Models.ILoadable<TViewModel>
        where TViewModel : Models.ILoadable<TModel>
    {
        public ElasticSearchRepository() : base()
        {

        }

        public virtual void BaseSettings(Stores.ISettings settings)
        {
            if (settings is Stores.ElasticSearch.Settings setts)
            {
                base.SetSettings(setts);
                Store = Stores.StoreLocator.GetStore<Stores.ElasticSearchStore<TModel>, Stores.ElasticSearch.Settings>(setts);
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

        public void ClearCache() {
            var _store = Store;
            (_store as Stores.ElasticSearchStore<TModel>)?.ClearCache();
        }
    }
}
