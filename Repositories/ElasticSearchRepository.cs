using System;
using System.Collections.Generic;
using System.Text;

namespace Birko.Data.Repositories
{
    public abstract class ElasticSearchRepository<TViewModel, TModel> : AbstractRepository<TViewModel, TModel>
        where TModel : Models.AbstractModel, Models.ILoadable<TViewModel>
        where TViewModel : Models.ILoadable<TModel>
    {
        public ElasticSearchRepository() : base()
        {

        }


        public virtual void BaseSettings(Stores.ISettings settings)
        {
            if (settings is Stores.Settings setts)
            {
                base.SetSettings(setts);
                Store = Stores.StoreLocator.GetStore<Stores.ElasticSearchStore<TModel>, Stores.Settings>(setts);
            }
        }
        public override void SetSettings(Stores.ISettings settings)
        {
            if (settings is Stores.Settings setts)
            {
                BaseSettings(setts);
                Store.Init();
            }
        }

        public virtual long Count(Nest.QueryContainer query)
        {
            var _store = Store;
            return (_store as Stores.ElasticSearchStore<TModel>).Count(query);
        }

        [Obsolete("Temporary solution until Expresion parser is build")]
        public override TViewModel Delete(Guid Id)
        {
            if (!ReadMode)
            {
                var _store = Store;
                var indexName = (_store as Stores.ElasticSearchStore<TModel>).GetIndexName();
                var item = (_store as Stores.ElasticSearchStore<TModel>).Connector.Get<TModel>(Id, i => i.Index(indexName));
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), new object[] { });
                if (item.Found)
                {
                    result.LoadFrom(item.Source);
                    (_store as Stores.ElasticSearchStore<TModel>).Delete(item.Source);
                    StoreChanges();
                    return result;
                }
            }
            return default(TViewModel);
        }

        [Obsolete("Temporary solution until Expresion parser is build")]
        public override TViewModel Read(Guid Id)
        {
            var _store = Store;
            var indexName = (_store as Stores.ElasticSearchStore<TModel>).GetIndexName();
            var item = (_store as Stores.ElasticSearchStore<TModel>).Connector.Get<TModel>(Id, i => i.Index(indexName));
            TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), new object[] { });
            if (item.Found)
            {
                result.LoadFrom(item.Source);
                StoreHash(item.Source);
                return result;
            }
            return default(TViewModel);
        }

        public virtual void Read(Nest.QueryContainer query, Action<TViewModel> readAction)
        {
            var _store = Store;
            (_store as Stores.ElasticSearchStore<TModel>).List(query, (item) =>
            {
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), new object[] { });
                result.LoadFrom(item);
                StoreHash(item);
                readAction?.Invoke(result);
            });
        }

        public virtual void Read(Nest.SearchRequest request, Action<TViewModel> readAction)
        {
            var _store = Store;
            (_store as Stores.ElasticSearchStore<TModel>).List(request, (item) =>
            {
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), new object[] { });
                result.LoadFrom(item);
                StoreHash(item);
                readAction?.Invoke(result);
            });
        }
    }
}
