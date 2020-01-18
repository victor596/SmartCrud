using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using static Dapper.SqlMapper;
namespace SmartCrud
{
    public partial class DbContext : IDisposable
    {
        public DbConnection Db { get; private set; }
        public DbConnType DbType { get; private set; }
        public bool IsOracleDb => DbType.IsOracle();
        public bool IsMySQL => DbType.IsMySQL();
        public bool SupportForUpdate => this.IsOracleDb || this.IsMySQL || this.DbType== DbConnType.POSTGRESQL;
        /// <summary>
        /// 事务
        /// </summary>
        public DbTransaction Transaction { get; private set; }
        /// <summary>
        /// 当前是否在事务中
        /// </summary>
        public bool IsInTransaction { get { return null != this.Transaction; } }
        private DbProviderFactory dbProvider = null;
        ~DbContext()
        {
            Dispose(false);//释放非托管资源，托管资源由终极器自己完成了
        }
        public void Dispose()
        {
            Dispose(true);
            GC.SuppressFinalize(this);
        }
        private bool disposed = false;
        protected virtual void Dispose(bool disposing)
        {
            if (disposed) return;
            if (disposing) //Clean up all managed resources.
            {
                Close();
                this.Db?.Dispose();
                this.Transaction?.Dispose();
            }
            //Clean up all native resources.
            //
            disposed = true;
        }
        private void Close()
        {
            this.Db?.Close();
        }
        /// <summary>
        /// 结束事务后是否关闭连接
        /// </summary>
        private bool closeConnectionAftTrans = false;
        public DbContext(DbConnectInfo dbconnect)
            : this(dbconnect.ToDbConnectionString(), SmartCrudHelper.GetEnumByKey<DbConnType>(dbconnect.ConnType.ToUpper()))
        { }
        public DbContext(string connectString, DbConnType dbConnType)
        {
            SqlBuilder.LoadTypeHandler();
            this.DbType = dbConnType;
            if (this.DbType == DbConnType.MSSQL)
            {
                if (connectString.IndexOf("MultipleActiveResultSets", StringComparison.OrdinalIgnoreCase) < 0)
                    connectString += ";MultipleActiveResultSets=true;";
            }
            dbProvider = DatabaseProvider.GetFactory(dbConnType);
            this.Db = dbProvider.CreateConnection();
            this.Db.ConnectionString = connectString;
            this.Db.Open();

        }
        public void BeginTrans(IsolationLevel isoLevel = IsolationLevel.Unspecified)
        {
            if (this.IsInTransaction)
                throw new Exception("Already in Transaction");
            bool wasClosed = Db.State == ConnectionState.Closed;
            closeConnectionAftTrans = wasClosed;
            if (wasClosed) Db.Open();
            if (isoLevel != IsolationLevel.Unspecified)
                this.Transaction = Db.BeginTransaction(isoLevel);
            else
                this.Transaction = Db.BeginTransaction();
        }
        public void Commit()
        {
            if (!this.IsInTransaction)
                throw new Exception("Not in Transaction");
            Transaction.Commit();
            this.Transaction = null;
            if (closeConnectionAftTrans)
            {
                this.Db.Close();
            }
        }
        public void Rollback()
        {
            if (!this.IsInTransaction)
                throw new Exception("Not in Transaction");
            Transaction.Rollback();
            this.Transaction = null;
            if (closeConnectionAftTrans)
            {
                this.Db.Close();
            }
        }
        /// <summary>
        /// useBrackets=true则是使用这样的参数 #{Para1},否则就是使用 #Para1#
        /// </summary>
        public bool UseBrackets { get; set; } = false;
        private string TransSql(string sql, object param)
        {
            return this.DbType.TransSQL(sql, param,this.UseBrackets);
        }
        public DateTime Now()
        {
            string sql = $"select {Dbdialect.CURRENT_TIME}";
            if (this.IsOracleDb) sql += " from dual";
            return ExecuteScalar<DateTime>(sql);
        }
        public int ExecuteNonQuery( string sql, object param=null, int? commandTimeout = null, CommandType cmdType= CommandType.Text)
        {
            return this.Db.Execute(TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        public object ExecuteScalar( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.ExecuteScalar(TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        public T ExecuteScalar<T>( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.ExecuteScalar<T>(TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        public IDataReader ExecuteReader( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.ExecuteReader(TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        public IEnumerable<object> Query(Type t,  string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.Query(t, TransSql(sql, param), param, this.Transaction, true, commandTimeout, cmdType);
        }
        public IEnumerable<dynamic> Query( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.Query<dynamic>(TransSql(sql, param), param, this.Transaction, true, commandTimeout, cmdType);
        }
        public IEnumerable<T> Query<T>( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.Query<T>(TransSql(sql, param), param, this.Transaction, true, commandTimeout, cmdType);
        }
        public GridReader QueryMultiple( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.QueryMultiple(TransSql(sql, param), param, this.Transaction,  commandTimeout, cmdType);
        }
        public DataTable GetDataTable( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            IDataReader reader = null;
            try
            {
                reader = ExecuteReader( sql, param, commandTimeout,cmdType);
                DataTable dt = new DataTable("data");
                dt.Load(reader);
                return dt;
            }
            finally
            {
                if (null != reader) reader.Close();
            }
        }
        public DataTable GetDataTablePage( string sql, int start, int rows, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            if (this.IsMySQL)
                return GetDataTable(SqlBuilder.ConvertToMySqlPagingSql(sql, start, rows), param, commandTimeout, cmdType);
            else if (this.DbType == DbConnType.SQLITE)
                return GetDataTable(SqlBuilder.ConvertToSqlitePagingSql(sql, start, rows), param, commandTimeout, cmdType);
            else if(this.DbType== DbConnType.POSTGRESQL)
                return GetDataTable(SqlBuilder.ConvertToPostgreSqlPagingSql(sql,start,rows), param, commandTimeout, cmdType);
            else if (this.DbType == DbConnType.MSSQL)
            {
                bool converted = false;
                string newSql = SqlBuilder.ConvertToMsSqlPagingSql(sql, out converted);
                if (converted)
                {
                    var para = SqlBuilder.ConvertParameter(param).SetValue("startIndex", start).SetValue("pageSize", rows);
                    return GetDataTable(newSql, para, commandTimeout, cmdType);
                }
            }
            else if (this.IsOracleDb /*&& start < 300000*/)
            {
                bool converted = false;
                string newSql = SqlBuilder.ConvertToOraclePagingSql(sql, out converted);
                if (converted)
                {
                    RequestBase para = SqlBuilder.ConvertParameter(param).SetValue("endIndex", start + rows).SetValue("startIndex", start);
                    return GetDataTable(newSql, para, commandTimeout, cmdType);
                }
            }
            var cmd = this.Db.CreateCommand();
            if (this.Transaction != null)
                cmd.Transaction = this.Transaction;
            
            cmd.CommandText = TransSql(sql, param);
            if (commandTimeout.HasValue)
            {
                cmd.CommandTimeout = commandTimeout.Value;
            }
            else if (SqlMapper.Settings.CommandTimeout.HasValue)
            {
                cmd.CommandTimeout = SqlMapper.Settings.CommandTimeout.Value;
            }
            cmd.CommandType = cmdType;
            SetupDbCommand(param, ref cmd);
            bool wasClosed = Db.State == ConnectionState.Closed;
            if (wasClosed) this.Db.Open();
#if !REF_ALL_DB || NETFRAMEWORK
            var adpter = this.dbProvider.CreateDataAdapter();
#else
            var adpter = DataBaseProvider.GetDbAdapter(this.DbType);
#endif
            adpter.SelectCommand = cmd;
            DataTable dt = new DataTable("data");
            try
            {
                adpter.Fill(start, rows, dt);
                return dt;
            }
            finally
            {
                adpter.Dispose();
                cmd.Dispose();
                if (wasClosed) Close();
            }
        }
        public dynamic QueryFirst( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.QueryFirst<dynamic>(TransSql(sql, param), param, this.Transaction,  commandTimeout, cmdType);
        }
        public T QueryFirst<T>( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.QueryFirst<T>(TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        public object QueryFirst(Type t,  string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.QueryFirst(t,TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        public T QueryFirstOrDefault<T>( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.QueryFirstOrDefault<T>(TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        public dynamic QueryFirstOrDefault( string sql, object param = null, int? commandTimeout = null, CommandType cmdType = CommandType.Text)
        {
            return Db.QueryFirstOrDefault(TransSql(sql, param), param, this.Transaction, commandTimeout, cmdType);
        }
        /// <summary>
        /// 处理连接串,参数支持Diction<string,object>,DbParameter[] , new{}
        /// </summary>
        /// <param name="param"></param>
        /// <param name="cmd"></param>
        internal void SetupDbCommand(object param, ref DbCommand cmd)
        {
            if (null == param) return ;
            IEnumerable<DbParameter> dbpars = param as IEnumerable<DbParameter>;
            if (null != dbpars)
            {
                foreach (var ele in dbpars) cmd.Parameters.Add(ele);
            }
            else
            {
                IDictionary<string, object> dic = SmartCrud.SmartCrudHelper.AsDictionary(param);
                if (null != dic)
                {
                    foreach (var ele in dic)
                    {
#if !REF_ALL_DB || NETFRAMEWORK
                        var par = this.dbProvider.CreateParameter();
#else
                        var par = DataBaseProvider.GetParameter(this.DbType);
#endif
                        par.ParameterName = ele.Key.TrimParaName();
                        par.Value = ele.Value;
                        cmd.Parameters.Add(par);
                    }
                }
            }
        }
    }
}
