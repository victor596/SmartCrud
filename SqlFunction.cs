using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
namespace SmartCrud
{
    /// <summary>
    /// 管理字段替换(bin)
    /// </summary>
    public class SqlFunction
    {
        public static SqlFunction CreateCurrentTimeFunction(DbContext conn, params string[] fields)
        {
            if (0 == (fields?.Length ?? 0)) return null;
            SqlFunction result = new SqlFunction();
            string repl = conn.GetCurrentTimeFuncName();
            foreach (string field in fields)
                result.AddSqlFunction(field, repl);
            return result;
        }
        private Dictionary<string, string> _repl = new Dictionary<string, string>();
        public bool HasRepField
        {
            get
            {
                return null != _repl && 0 < _repl.Count;
            }
        }
        public Dictionary<string, string> SqlFunctionList
        {
            get { return _repl; }
        }
        public string GetSqlFunction(string fieldName)
        {
            string value = string.Empty;
            _repl.TryGetValue(fieldName, out value);
            return value;
        }
        public string this[string fieldName]
        {
            get
            {
                return GetSqlFunction(fieldName);
            }
        }
        public bool ExistsField(string fieldName)
        {
            return _repl.ContainsKey(fieldName);
        }
        public SqlFunction AddSqlFunction(string fieldName, string repTo)
        {
            string ret = GetSqlFunction(fieldName);
            if (string.IsNullOrEmpty(ret))
                _repl.Add(fieldName, repTo);
            else
                _repl[fieldName] = repTo;
            return this;
        }
        public SqlFunction()
        {

        }
        public SqlFunction(string fieldName, string repTo)
        {
            AddSqlFunction(fieldName, repTo);
        }
    }
}
