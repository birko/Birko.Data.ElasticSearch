using System;
using System.Collections.Generic;
using System.Text;

namespace Birko.Data.ElasticSearch.Stores
{
    public class Settings : Data.Stores.Settings
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
