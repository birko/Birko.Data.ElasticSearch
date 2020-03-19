﻿using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Text;

namespace Birko.Data.Stores
{
    public class ElasticSearchStore<T> : AbstractStore<T, Settings>
         where T : Models.AbstractModel
    {
        public ElasticClient Connector { get; private set; }
        private Settings _settings = null;

        private Dictionary<Guid, T> _insertList = null;
        private Dictionary<Guid, T> _updateList = null;
        private Dictionary<Guid, T> _deleteList = null;

        public ElasticSearchStore() : base ()
        {
        }

        public override void SetSettings(Settings settings)
        {
            if (settings is Settings sets)
            {
                _settings = sets;
                Connector = ElasticSearch.ElasticSearch.GetClient(_settings);
                _insertList = new Dictionary<Guid, T>();
                _updateList = new Dictionary<Guid, T>();
                _deleteList = new Dictionary<Guid, T>();
            }
        }

        public override long Count(Expression<Func<T, bool>> filter)
        {
            if (filter != null)
            {
                throw new NotImplementedException();
            }
            return Count((QueryContainer)null);
        }

        public long Count(QueryContainer query)
        {
            string indexName = GetIndexName();
            var request = new CountRequest(indexName);
            if (query != null)
            {
                request.Query = query;
            }
            return Connector.Count<T>(request).Count;
        }

        public override void Delete(T data)
        {
            if (data != null && data.Guid != null && !_deleteList.ContainsKey(data.Guid.Value))
            {
                _deleteList.Add(data.Guid.Value, data);
            }
        }

        public void Init(Func<CreateIndexDescriptor, ICreateIndexRequest> indexDescriptor)
        {
            var indexName = GetIndexName();
            if (!Connector.IndexExists(indexName).Exists)
            {
                var response = Connector.CreateIndex(indexName, indexDescriptor);
            }
        }

        public override void Init()
        {
            Init(cid =>
                cid.Mappings(md =>
                    md.Map<T>(m => m.AutoMap())
            ));
        }

        public void DeleteIndex()
        {
            DeleteIndex(typeof(T));
        }

        public void DeleteIndex(Type type)
        {
            if (type != null)
            {
                DeleteIndex(type.Name);
            }
        }

        public void DeleteIndex(string name)
        {
            if (!string.IsNullOrEmpty(name))
            {
                var indexName = string.Format("{0}_{1}", _settings.Name, name).ToLower();
                var response = Connector.DeleteIndex(indexName);
            }
        }

        public override void List(Expression<Func<T, bool>> filter, Action<T> listAction)
        {
            SearchDescriptor<T> search = new SearchDescriptor<T>();
            if (filter != null)
            {
                throw new NotImplementedException();
            }
            List((QueryContainer)null, listAction);
        }

        public void List(SearchRequest request, Action<T> listAction)
        {
            if (request != null)
            {
                int count = request.From ?? 0;
                Time scrollTime = null;
                string scrollId;
                int? size = request.Size;
                if ((request.From == null && request.Size == null)
                   || request.Size >= 10000)
                {
                    scrollTime = new Time(new TimeSpan(0, 1, 0));
                    request.Scroll = scrollTime;
                    request.Size = 1000;
                }
                var searchResponse = Connector.Search<T>(request);
                scrollId = searchResponse.ScrollId;
                long end = (size != null) ? (count + size ?? 12 ) : searchResponse.Total;
                if(end > searchResponse.Total)
                {
                    end = searchResponse.Total;
                }
                while (count < end)
                {
                    var result = searchResponse.Documents;
                    foreach (var document in result)
                    {
                        listAction?.Invoke(document);
                        count++;
                    }
                    if (count < end)
                    {
                        if (!string.IsNullOrEmpty(scrollId) && scrollTime != null)
                        {
                            searchResponse = Connector.Scroll<T>(new Nest.ScrollRequest(scrollId, scrollTime));
                            scrollId = searchResponse.ScrollId;
                        }
                        else
                        {
                            request.From = count;
                            searchResponse = Connector.Search<T>(request);
                        }
                    }
                }
                if (!string.IsNullOrEmpty(scrollId) && scrollTime != null)
                {
                    Connector.ClearScroll(new Nest.ClearScrollRequest(scrollId));
                }
            }
        }

        public void List(QueryContainer query, Action<T> listAction)
        {
            string indexName = GetIndexName();
            SearchRequest request = new SearchRequest(indexName);
            if (query != null)
            {
                request.Query = query;
            }
            List(request, listAction);
        }

        public override void Save(T data, StoreDataDelegate<T> storeDelegate = null)
        {
            if (data != null)
            {
                bool newItem = data.Guid == null;
                if (newItem) // new
                {
                    data.Guid = Guid.NewGuid();
                }
                data = storeDelegate?.Invoke(data) ?? data;
                if (data != null)
                {
                    if (newItem)
                    {
                        if (!_insertList.ContainsKey(data.Guid.Value))
                        {
                            _insertList.Add(data.Guid.Value, data);
                        }
                        else
                        {
                            _insertList[data.Guid.Value] = data;
                        }
                    }
                    else
                    {
                        if (data is Models.AbstractLogModel)
                        {
                            (data as Models.AbstractLogModel).PrevUpdatedAt = (data as Models.AbstractLogModel).UpdatedAt;
                            (data as Models.AbstractLogModel).UpdatedAt = DateTime.UtcNow;
                        }
                        if (!_updateList.ContainsKey(data.Guid.Value))
                        {
                            _updateList.Add(data.Guid.Value, data);
                        }
                        else
                        {
                            _updateList[data.Guid.Value] = data;
                        }
                    }
                }
            }
        }

        public override void StoreChanges()
        {
            if (Connector != null)
            {
                string indexName = GetIndexName();

                //delete
                while (_deleteList.Count > 0)
                {
                    var kvp = _deleteList.First();
                    Connector.Delete<T>(kvp.Key, (i) => i.Index(indexName));
                    _deleteList.Remove(kvp.Key);
                }
                //update
                while (_updateList.Count > 0)
                {
                    var kvp = _updateList.First();
                    Connector.Index<T>(kvp.Value, i => i.Id(kvp.Key).Index(indexName));
                    _updateList.Remove(kvp.Key);
                }
                //insert
                while (_insertList.Count > 0)
                {
                    var kvp = _insertList.First();
                    Connector.Index(kvp.Value, i => i.Id(kvp.Key).Index(indexName));
                    _insertList.Remove(kvp.Key);
                }
            }
            else
            {
                throw new Exceptions.StoreException("No database connector provided");
            }
        }

        public string GetIndexName()
        {
            var type = typeof(T);
            return string.Format("{0}_{1}", _settings.Name, type.Name).ToLower();
        }

        public override void Destroy()
        {
            DeleteIndex();
        }
    }
}