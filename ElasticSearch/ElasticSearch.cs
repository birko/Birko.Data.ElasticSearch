using Nest;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;

namespace Birko.Data.ElasticSearch
{
    public static class ElasticSearch
    {
        private static Dictionary<string, ElasticClient> _clients;

        public static ElasticClient GetClient(Store.Settings settings)
        {
            if (_clients == null)
            {
                _clients = new Dictionary<string, ElasticClient>();
            }
            if (!_clients.ContainsKey(settings.GetId()))
            {
                var local = new Uri(settings.Location);
                var indexName = string.Format("{0}_{1}", settings.Name, "default").ToLower();
                ConnectionSettings clientSettings = new ConnectionSettings(local)
                        //.DefaultIndex(indexName)
                        .DisableDirectStreaming();
                _clients.Add(settings.GetId(), new ElasticClient(clientSettings));
            }
            return _clients[settings.GetId()];
        }
    }
}
