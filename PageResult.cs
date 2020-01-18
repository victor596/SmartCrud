using System;
using System.Collections.Generic;
namespace SmartCrud
{
    [Serializable]
    public class PageResult<T> where T:new()
    {
        public IEnumerable<T> DataList { get; set; }
        public int TotalCount { get; set; }
    }
    [Serializable]
    public class PageResultDataTable
    {
        public System.Data.DataTable DataList { get; set; }
        public int TotalCount { get; set; }
    }
}
