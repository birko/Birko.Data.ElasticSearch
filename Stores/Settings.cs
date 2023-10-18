using System;
using System.Collections.Generic;
using System.Text;

namespace Birko.Data.Stores.ElasticSearch
{
    public class Settings : Stores.Settings
    {
        public IEnumerable<IndexSettings> IndexSettings { get; set; }
    }

    public class IndexSettings
    {
        public string TypeName { get; set; }
        public string Name { get; set; }
        public int? MaxResultWindow { get; set; }
    }
}
