using System;
using System.Collections.Generic;
using System.Text;

namespace Birko.Data.Repository
{
    public abstract class ElasticSearchRepository<TViewModel, TModel> : AbstractRepository<TViewModel, TModel>
        where TModel : Model.AbstractModel, Model.ILoadable<TViewModel>
        where TViewModel : Model.ILoadable<TModel>
    {
        public ElasticSearchRepository(string path, string name) : base(path)
        {
            _store = new Store.ElasticSearchStore<TModel>(new Store.Settings
            {
                Location = path,
                Name = name
            });
        }

        public virtual long Count(Nest.QueryContainer query)
        {
            return (_store as Store.ElasticSearchStore<TModel>).Count(query);
        }

        [Obsolete("Temporary solution until Expresion parser is build")]
        public override TViewModel Delete(Guid Id)
        {
            if (!ReadMode)
            {
                var indexName = (_store as Store.ElasticSearchStore<TModel>).GetIndexName();
                var item = (_store as Store.ElasticSearchStore<TModel>).Connector.Get<TModel>(Id, i => i.Index(indexName));
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), new object[] { });
                if (item.Found)
                {
                    result.LoadFrom(item.Source);
                    (_store as Store.ElasticSearchStore<TModel>).Delete(item.Source);
                    StoreChanges();
                    return result;
                }
            }
            return default(TViewModel);
        }

        [Obsolete("Temporary solution until Expresion parser is build")]
        public override TViewModel Read(Guid Id)
        {
            var indexName = (_store as Store.ElasticSearchStore<TModel>).GetIndexName();
            var item = (_store as Store.ElasticSearchStore<TModel>).Connector.Get<TModel>(Id, i => i.Index(indexName));
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
            (_store as Store.ElasticSearchStore<TModel>).List(query, (item) =>
            {
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), new object[] { });
                result.LoadFrom(item);
                StoreHash(item);
                readAction?.Invoke(result);
            });
        }

        public virtual void Read(Nest.SearchRequest request, Action<TViewModel> readAction)
        {
            (_store as Store.ElasticSearchStore<TModel>).List(request, (item) =>
            {
                TViewModel result = (TViewModel)Activator.CreateInstance(typeof(TViewModel), new object[] { });
                result.LoadFrom(item);
                StoreHash(item);
                readAction?.Invoke(result);
            });
        }
    }
}
