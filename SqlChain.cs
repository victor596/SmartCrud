using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Text;
using System.Threading.Tasks;

namespace SmartCrud
{
    public class SqlChain
    {
        private DbContext _connInfo = null;
        /// <summary>
        /// Chain操作中使用参数对象
        /// </summary>
        private RequestBase param = null;
        /// <summary>
        /// Chain操作中使用命令类型
        /// </summary>
        private CommandType _CmdType = CommandType.Text;
        /// <summary>
        /// Chain操作中使用命令
        /// </summary>
        private string _SQL = string.Empty;
        private StringBuilder _strBld = null;
        private bool _useSqlBuilder = false;
        private string GetSql()
        {
            return _useSqlBuilder ? _strBld.ToString() : _SQL;
        }
        /// <summary>
        /// 自动调用ResetParameter
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="cmdType"></param>
        /// <param name="sql"></param>
        public SqlChain(DbContext connInfo, CommandType cmdType, string sql,bool useStrBld=false )
        {
            _connInfo = connInfo;
            _CmdType = cmdType;
            _useSqlBuilder = useStrBld;
            if (_useSqlBuilder)
                _strBld = new StringBuilder(sql, 1024);
            else
                _SQL = sql;
            ResetParameter();
        }
        /// <summary>
        /// 清空参数
        /// </summary>
        /// <returns></returns>
        public SqlChain ResetParameter()
        {
            if (null != param)
                param.Clear();
            else
                param = new RequestBase();
            return this;
        }
        public SqlChain AppendSql(string sql)
        {
            if (!_useSqlBuilder)
                throw new ArgumentException("useSqlBuilder");
            _strBld.Append(sql);
            return this;
        }
        /// <summary>
        /// Chain操作中使用
        /// </summary>
        /// <param name="paraName"></param>
        /// <param name="value"></param>
        /// <returns></returns>
        public SqlChain AddParameter(string paraName, object value,Func<bool> addThisPara=null)
        {
            if (null == addThisPara || (null!=addThisPara && addThisPara()))
                param.SetValue(paraName, value);
            return this;
        }
        /// <summary>
        /// Chain操作中使用
        /// </summary>
        /// <param name="conditionList"></param>
        /// <returns></returns>
        public SqlChain AddParameters(Dictionary<string, object> conditionList)
        {
            foreach (var ele in conditionList)
                param.SetValue(ele.Key, ele.Value);
            return this;
        }
        /// <summary>
        /// Chain操作中使用
        /// </summary>
        /// <param name="paraValues"></param>
        /// <returns></returns>
        public SqlChain AddParametersFromArray(params object[] paraValues)
        {
            param.AddParametersFromArray(paraValues);
            return this;
        }
        public DataTable GetDataTable()
        {
            return _connInfo.GetDataTable(GetSql(), param, cmdType:_CmdType);
        }
        public IDataReader GetReader()
        {
            return _connInfo.ExecuteReader( GetSql(), param, cmdType: _CmdType);
        }
        /// <summary>
        /// 获取页面,固定使用CommandType.Text方式
        /// </summary>
        /// <param name="PageSize"></param>
        /// <param name="PageIndex"></param>
        /// <param name="useEmit"></param>
        /// <returns></returns>
        public PageResultDataTable GetPage(int PageSize, int PageIndex)
        {
            return _connInfo.SelectPageDataTable(GetSql(), param, PageSize, PageIndex);
        }
        /// <summary>
        /// 获取页面,固定使用CommandType.Text方式
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="PageSize"></param>
        /// <param name="PageIndex"></param>
        /// <returns></returns>
        public PageResult<T> GetPageList<T>(int PageSize, int PageIndex) where T : new()
        {
            return _connInfo.SelectPage<T>(GetSql(), param, PageSize, PageIndex);
        }
        /// <summary>
        /// 获取页面,固定使用CommandType.Text方式
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="useEmit"></param>
        /// <returns></returns>
        public IEnumerable<T> GetList<T>() where T : new()
        {
            return _connInfo.Query<T>( GetSql(), param);
        }
        public int Execute()
        {
            return _connInfo.ExecuteNonQuery( GetSql(), param);
        }
        public bool IsExists()
        {
            return _connInfo.ExistsPrimitive(GetSql(), param);
        }
        public object SingleValue()
        {
            return _connInfo.ExecuteScalar( GetSql(), param);
        }
        public T SingleValue<T>()
        {
            return _connInfo.ExecuteScalar<T>(GetSql(), param);
        }
    }
}
