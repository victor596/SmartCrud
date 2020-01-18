using Dapper;
using System;
using System.Collections.Generic;
using System.Data;
using System.Linq;
using System.Linq.Expressions;
using System.Reflection;
using System.Text;
using static Dapper.SqlMapper;
using static SmartCrud.SmartCrudHelper;
using static SmartCrud.SqlBuilder;
namespace SmartCrud
{
    public static partial class DbContextExtension
    {
        public static bool IsMySQL(this DbConnType dbConnType) =>  dbConnType == DbConnType.MYSQL;
        public static bool IsOracle(this DbConnType dbConnType) => dbConnType == DbConnType.ORACLE || dbConnType == DbConnType.ODPNET;
        public static string TreatParaName(this DbContext connInfo, string paraName) => connInfo.DbType.TreatParaName(paraName);
        public static void AddParameters(Dictionary<string, object> paramemters, ref RequestBase para)
        {
            if (null == paramemters) return;
            foreach (var ele in paramemters) para.SetValue(ele.Key, ele.Value);
        }
        /// <summary>
        /// Chain操作中使用
        /// </summary>
        /// <param name="cmdType"></param>
        /// <param name="sql"></param>
        /// <returns></returns>
        public static SqlChain StartSqlChain(this DbContext connInfo, string sql, CommandType cmdType = CommandType.Text)
        {
            return new SqlChain(connInfo, cmdType, sql);
        }
        public static int InsertOrUpdate<T>(this DbContext connInfo, T record, SqlFunction repl = null, Expression<Func<T, object>> onlyFields = null, string tableName = "",bool forUpdate=false) where T : new()
        {
            if (connInfo.Exists<T>(record,tableName,forUpdate: forUpdate))
            {
                return connInfo.Update<T>(record, repl, onlyFields, tableName);
            }
            else
                return connInfo.Insert<T>(record, repl, onlyFields, tableName);
        }
        /// <summary>
        /// 锁定一条记录,UpdateField在ORACLE下无效，用于执行 Update T set UpdateField=UpdateField where ...
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="obj"></param>
        public static bool LockRecord<T>(this DbContext connInfo, T record, string updateField, string tableName = "")
        {
            Type t = typeof(T);
            TableInfo tableInfo = GetTableInfo(t, 0, tableName);
            if (null == tableInfo)
            {
                throw new ArgumentNullException("GetTableInfo");
            }
            StringBuilder whereState = new StringBuilder(256);
            whereState.Append(" WHERE"); // where子句
            int pkcount = tableInfo.PKeys.Count;
            RequestBase pars = new RequestBase();
            Dictionary<string, PropertyInfo> fields = EntityReader.GetTypePropertyMapping(t);
            int i = 0;
            foreach (KeyValuePair<string, PropertyInfo> item in
                fields.Where(c => tableInfo.PKeys.Contains(c.Key,StringComparer.OrdinalIgnoreCase)))
            {
                if (i > 0) whereState.Append(" AND ");
                whereState.AppendFormat(" {0}={1} ", connInfo.DbDelimiter(item.Key), connInfo.TreatParaName(item.Key));
                pars.Add(item.Key, GetPropertyValue<T>(record, item, connInfo.DbType));
                ++i;
            }
            StringBuilder sql = new StringBuilder(512);
            if (connInfo.SupportForUpdate)
            {
                sql.Append("SELECT 'Y' FROM ");
                sql.Append(connInfo.DbDelimiter(tableName));
                sql.Append(whereState.ToString());
                sql.Append(" FOR UPDATE ");
                return connInfo.Db.ExecuteScalar<string>(sql.ToString(), pars, connInfo.Transaction, commandType: CommandType.Text).IsTrue();
            }
            else
            {
                if (string.IsNullOrEmpty(updateField))
                {
                    throw new Exception("update field error!");
                }
                sql.Append("UPDATE ");
                sql.Append(tableInfo.TableName.DbDelimiter(connInfo));
                sql.AppendFormat(" SET {0}={1}", updateField.DbDelimiter(connInfo), updateField);
                sql.Append(whereState.ToString());
                return 1 == connInfo.Db.Execute(sql.ToString(), pars, connInfo.Transaction, commandType: CommandType.Text);
            }
        }
        /// <summary>
        /// 锁定一条记录,UpdateField在ORACLE下无效，用于执行 Update T set UpdateField=UpdateField where ...
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="sqlTableName"></param>
        /// <param name="lstFieldVal"></param>
        /// <param name="updateField"></param>
        /// <returns></returns>
        public static bool LockRecord(this DbContext connInfo, string sqlTableName, RequestBase param, string updateField)
        {
            StringBuilder whereStatement = new StringBuilder(256);
            int i = 0;
            foreach (var ele in param)
            {
                if (i > 0)
                    whereStatement.Append(" AND ");
                else
                    whereStatement.Append(" WHERE ");
                whereStatement.AppendFormat("{0}={1}", ele.Key.DbDelimiter(connInfo), connInfo.TreatParaName(ele.Key));
                ++i;
            }
            if (connInfo.SupportForUpdate)
            {
                return connInfo.Db.ExecuteScalar<string>(
                    string.Format("SELECT 'Y' FROM {0} {1} FOR UPDATE", sqlTableName.DbDelimiter(connInfo)
                    , whereStatement.ToString()), param, connInfo.Transaction, commandType: CommandType.Text).IsTrue();
            }
            else
            {
                if (string.IsNullOrEmpty(updateField))
                {
                    throw new ArgumentNullException("updateField");
                }
                return (1 == connInfo.Db.Execute(
                    string.Format("UPDATE {0} SET {1}={1} {2}", sqlTableName.DbDelimiter(connInfo),
                    updateField.DbDelimiter(connInfo), whereStatement.ToString()), param, connInfo.Transaction, commandType: CommandType.Text));
            }
        }
        #region Select
        /// <summary>
        /// 普通SELECT语句取多少记录(用替代法替换)
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static int Count(this DbContext connInfo, string sql, object param)
        {
            bool useRepl = sql.StartsWith("SELECT", StringComparison.OrdinalIgnoreCase)
                && sql.IndexOf("UNION", StringComparison.OrdinalIgnoreCase) < 0;
            if (useRepl)
            {
                if (!sql.StartsWith("SELECT COUNT(*)", StringComparison.OrdinalIgnoreCase))
                {
                    int indexFROM = sql.IndexOf("FROM", 0, StringComparison.OrdinalIgnoreCase);
                    int indexORDERBY = sql.IndexOf("ORDER BY", 0, StringComparison.OrdinalIgnoreCase);
                    StringBuilder sqlbld = new StringBuilder(512);
                    sqlbld.Append("SELECT COUNT(*) ");
                    if (0 <= indexORDERBY)
                    {
                        sqlbld.Append(sql.Substring(indexFROM, indexORDERBY - indexFROM));
                    }
                    else
                    {
                        sqlbld.Append(sql.Substring(indexFROM));
                    }
                    return SmartCrudHelper.GetIntValue(connInfo.ExecuteScalar(sqlbld.ToString(), param));
                }
                return connInfo.ExecuteScalar<int>(sql, param);
            }
            else
            {
                IDataReader reader = null;
                try
                {
                    reader = connInfo.ExecuteReader(sql, param);
                    int rows = 0;
                    while (reader.Read()) ++rows;
                    return rows;
                }
                finally
                {
                    if (null != reader) reader.Close();
                }
            }
        }
        public static object SingleValue<T>(this DbContext connInfo, object[] pkValues, Expression<Func<T, object>> fieldName,string tableName="", bool forUpdate = false)
            => SingleValuePrimitive<T>(connInfo, pkValues, SmartCrudHelper.GetPropertyName<T>(fieldName),tableName,forUpdate);
        /// <summary>
        /// 取单个值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="pkValues"></param>
        /// <param name="FieldName"></param>
        /// <param name="pkCount"></param>
        /// <returns></returns>
        public static object SingleValuePrimitive<T>(this DbContext connInfo, object[] pkValues, string fieldName,string tableName="", bool forUpdate=false)
        {
            if (0 == (pkValues?.Length ?? 0)) throw new Exception("primary keys are missing!");
            Type t = typeof(T);
            TableInfo tableInfo = GetTableInfo(t, tableName);
            StringBuilder sqlbld = new StringBuilder(128);
            sqlbld.Append("SELECT ");
            sqlbld.Append(fieldName.DbDelimiter(connInfo));
            sqlbld.Append(" FROM ");
            sqlbld.Append(tableInfo.TableName.DbDelimiter(connInfo));
            sqlbld.Append(" WHERE ");
            RequestBase pars = new RequestBase();
            int j = 0;
            foreach (string pkName in tableInfo.PKeys)
            {
                if (0 != j) sqlbld.Append(" AND ");
                sqlbld.AppendFormat(" {0}={1} ", pkName.DbDelimiter(connInfo), connInfo.TreatParaName(pkName));
                pars.Add(pkName, pkValues[j]);
                ++j;
            }
            if (forUpdate && connInfo.SupportForUpdate) sqlbld.Append(" for update");
            return connInfo.Db.ExecuteScalar(sqlbld.ToString(), pars, connInfo.Transaction, commandType: CommandType.Text);
        }
        public static IEnumerable<T> SelectAll<T>(this DbContext connInfo, string tableName = "")
            where T : new()
        {
            return connInfo.Db.Query<T>(connInfo.GetSelectSql<T>(true), transaction: connInfo.Transaction, commandType: CommandType.Text);
        }
        /// <summary>
        /// Start下标从零开始
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="Start">0--></param>
        /// <param name="Rows"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static IEnumerable<T> Select<T>(this DbContext connInfo, string sql, object param, int Start, int Rows)
            where T : new()
        {
            if (connInfo.IsMySQL)
                return connInfo.Query<T>(SqlBuilder.ConvertToMySqlPagingSql(sql,Start,Rows), param);
            else if (connInfo.DbType == DbConnType.SQLITE)
                return connInfo.Query<T>(SqlBuilder.ConvertToSqlitePagingSql(sql,Start,Rows), param);
            else if(connInfo.DbType== DbConnType.POSTGRESQL)
                return connInfo.Query<T>(SqlBuilder.ConvertToPostgreSqlPagingSql(sql, Start, Rows), param);
            else if (connInfo.DbType == DbConnType.MSSQL)
            {
                bool converted = false;
                string newSql = SqlBuilder.ConvertToMsSqlPagingSql(sql, out converted);
                if (converted)
                {
                    RequestBase para = SqlBuilder.ConvertParameter(param).SetValue("pageSize", Rows).SetValue("startIndex", Start);
                    return connInfo.Query<T>(newSql, para);
                }
            }
            else if (connInfo.IsOracleDb /*&& Start < 300000*/)
            {
                bool converted = false;
                string newSql = SqlBuilder.ConvertToOraclePagingSql(sql, out converted);
                if (converted)
                {
                    RequestBase para = SqlBuilder.ConvertParameter(param).SetValue("endIndex", Start + Rows).SetValue("startIndex", Start);
                    return connInfo.Query<T>(newSql, para);
                }
            }
            DataTable dt = null;
            try
            {
                dt = connInfo.GetDataTablePage(sql, Start, Rows, param);
                return EntityReader.GetEntities<T>(dt);
            }
            finally
            {
                dt?.Dispose();
            }
        }
        /// <summary>
        /// 分页取数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="Pars"></param>
        /// <param name="PageSize">为零不限制页面大小即返回所有数据</param>
        /// <param name="PageIndex">PageIndex从1开始</param>
        /// <returns></returns>
        public static PageResult<T> SelectPage<T>(this DbContext connInfo, string sql, object param, int PageSize, int PageIndex) where T : new()
        {
            int totalCount = 0;
            IEnumerable<T> result = SelectPage<T>(connInfo, sql, param, PageSize, PageIndex, true, ref totalCount);
            return new PageResult<T> { DataList = result, TotalCount = totalCount };
        }
        /// <summary>
        /// 分页导出数据,PageIndex从1开始
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="Pars"></param>
        /// <param name="PageSize">为零不限制页面大小即返回所有数据</param>
        /// <param name="PageIndex">PageIndex从1开始</param>
        /// <param name="returnTotalRows"></param>
        /// <param name="TotalRows"></param>
        /// <returns></returns>
        public static IEnumerable<T> SelectPage<T>(this DbContext connInfo,
            string sql, object param, int PageSize, int PageIndex, bool returnTotalRows, ref int TotalRows)
            where T : new()
        {
            bool getAll = 0 == PageSize;
            if (PageSize < 0) throw new ArgumentException(nameof(PageSize));
            if (!getAll)
            {
                if (PageIndex < 1) throw new ArgumentException(nameof(PageIndex));
            }
            if (!getAll)
            {
                if (returnTotalRows)
                {
                    TotalRows = 0;
                    TotalRows = Count(connInfo, sql, param);
                }
                return Select<T>(connInfo, sql, param, (PageIndex - 1) * PageSize, PageSize);
            }
            else
            {
                List<T> result = connInfo.Query<T>(sql, param)?.ToList();
                if (returnTotalRows)
                {
                    TotalRows = result?.Count ?? 0;
                }
                return result;
            }
        }
        public static IEnumerable<T> SelectPagePrimitive<T>(this DbContext connInfo, string sql, object param, int PageSize, int PageIndex)
            where T : new()
        {
            int totalRows = 0;
            return SelectPage<T>(connInfo, sql, param, PageSize, PageIndex, false, ref totalRows);
        }
        /// <summary>
        /// 分页取数
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="Pars"></param>
        /// <param name="PageSize">为零不限制页面大小即返回所有数据</param>
        /// <param name="PageIndex">PageIndex从1开始</param>
        /// <returns></returns>
        public static PageResultDataTable SelectPageDataTable(this DbContext connInfo,
            string sql, RequestBase param, int PageSize, int PageIndex,
            string filter = "", string orderBy = "", Dictionary<string, object> condition = null)
        {
            int totalCount = 0;
            DataTable result = SelectPageDataTable(connInfo, sql, param,
                PageSize, PageIndex, true, ref totalCount, filter, orderBy, condition);
            return new PageResultDataTable { DataList = result, TotalCount = totalCount };
        }
        public static DataTable GetDataTable(this DbContext connInfo, string sql, RequestBase param,
            string filter = "", string orderStatement = "", Dictionary<string, object> condition = null)
            => SelectPageDataTable(connInfo, sql, param, 0, 0, filter, orderStatement, condition)?.DataList;
        /// <summary>
        /// 分页导出数据,PageIndex从1开始
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="Pars"></param>
        /// <param name="PageSize">为零不限制页面大小即返回所有数据</param>
        /// <param name="PageIndex">PageIndex从1开始</param>
        /// <param name="returnTotalRows"></param>
        /// <param name="TotalRows"></param>
        /// <returns></returns>
        public static DataTable SelectPageDataTable(this DbContext connInfo,
            string sql, RequestBase param, int PageSize, int PageIndex, bool returnTotalRows, ref int TotalRows,
            string filter = "", string orderStatement = "", Dictionary<string, object> condition = null)
        {
            bool getAll = (0 == PageSize);
            if (PageSize < 0)
            {
                throw new Exception("PageSize must greater or equal than 0!");
            }
            if (!getAll && PageIndex < 1)
            {
                throw new Exception("PageIndex must greater or equal than 1!");
            }
            StringBuilder sqlbld = new StringBuilder(1024);
            sqlbld.Append(sql);
            bool hasCondi = (!string.IsNullOrEmpty(filter) || (null != condition && 0 < condition.Count)); //无过滤
            if (hasCondi)
            {
                bool containWhere = 0 <= sql.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
                sqlbld.Append(containWhere ? " AND " : " WHERE ");
                sqlbld.Append(filter);
                AddParameters(condition, ref param);
            }
            AppendOrderBy(orderStatement, sqlbld);
            string SQL = sqlbld.ToString();
            if (!getAll)
            {
                if (returnTotalRows)
                {
                    TotalRows = 0;
                    TotalRows = Count(connInfo, SQL, param);
                }
                return connInfo.GetDataTablePage(SQL, (PageIndex - 1) * PageSize, PageSize, param);
            }
            else
            {
                DataTable result = connInfo.GetDataTable(SQL, param);
                if (returnTotalRows)
                {
                    TotalRows = (null != result ? result.Rows.Count : 0);
                }
                return result;
            }
        }
        /// <summary>
        /// 取页数据,conditionFilter中的不确定参数应该以 {0},{1}开头,pars可以为空
        /// 此方法会把conditionList中的参数代替conditionFilter中的{0},{1}...
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="pars"></param>
        /// <param name="conditionFilter"></param>
        /// <param name="conditionList"></param>
        /// <param name="PageSize"></param>
        /// <param name="PageIndex">从1开始</param>
        /// <returns></returns>
        public static PageResult<T> SelectPageFilter<T>(this DbContext connInfo, string sql,
            RequestBase param, int PageSize, int PageIndex, string filter = "", string orderStatement = "", Dictionary<string, object> condition = null)
            where T : new()
        {
            StringBuilder sqlbld = new StringBuilder(1024);
            sqlbld.Append(sql);
            bool hasCondi = (!string.IsNullOrEmpty(filter) ||
               (0 < (condition?.Count ?? 0))); //无过滤
            if (!hasCondi)
            {
                AppendOrderBy(orderStatement, sqlbld);
                return SelectPage<T>(connInfo, sqlbld.ToString(), param, PageSize, PageIndex);
            }
            bool containWhere = 0 <= sql.IndexOf("WHERE ", StringComparison.OrdinalIgnoreCase);
            sqlbld.Append(containWhere ? " AND " : " WHERE ");
            sqlbld.Append(filter);
            AppendOrderBy(orderStatement, sqlbld);
            AddParameters(condition, ref param);
            return SelectPage<T>(connInfo, sqlbld.ToString(), param, PageSize, PageIndex);
        }
        public static IEnumerable<T> SelectFilter<T>(this DbContext connInfo, string filter, object param = null, string tableName = "") where T : new()
        {
            StringBuilder sqlbld = new StringBuilder(512);
            string fieldStr = SqlBuilder.GetFieldStr<T>(connInfo.DbType);
            if (string.IsNullOrEmpty(fieldStr))
                throw new Exception("Field is null!");
            sqlbld.Concat("SELECT ", fieldStr, " FROM ", GetTableName<T>(connInfo.DbType, tableName));
            if (!string.IsNullOrEmpty(filter))
            {
                if (!filter.StartsWith("WHERE",
                    StringComparison.OrdinalIgnoreCase)) sqlbld.Append(" WHERE ");
                sqlbld.Append(filter);
            }
            return connInfo.Query<T>(sqlbld.ToString(), param);
        }
        /// <summary>
        /// conditionFilter 类似于 a.FieldName=#Para1# and b.FieldName=#Para2#
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="pars"></param>
        /// <param name="conditionFilter"></param>
        /// <param name="conditionList"></param>
        /// <returns></returns>
        public static IEnumerable<T> SelectCondition<T>(this DbContext connInfo, string sql, RequestBase param, string filter = "", Dictionary<string, object> condition = null)
            where T : new()
        {
            StringBuilder sqlbld = new StringBuilder(2048);
            sqlbld.Append(sql);
            bool hasCondi = (!string.IsNullOrEmpty(filter) || (null != condition && 0 < condition.Count));
            if (!hasCondi)
            {
                return connInfo.Query<T>(sqlbld.ToString(), param);
            }
            bool containWhere = 0 <= sql.IndexOf(" WHERE ", StringComparison.OrdinalIgnoreCase);
            sqlbld.Append(containWhere ? " AND " : " WHERE ");
            sqlbld.Append(filter);
            AddParameters(condition, ref param);
            return connInfo.Query<T>(sqlbld.ToString(), param);
        }
        /// <summary>
        /// 根据主键来取记录(最常用)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="PkValues"></param>
        /// <returns></returns>
        public static T SelectPrimitive<T>(this DbContext connInfo, object[] pkValues, IEnumerable<string> onlyFields = null, bool isExceptField = false, string tableName = "", bool forUpdate = false)
            where T : new()
        {
            string sql = "";
            if (isExceptField && 0 < (onlyFields?.Count() ?? 0))//排除字段
            {
                sql = connInfo.GetSelectSql<T>(false, GetFields(typeof(T), true, onlyFields.ToArray()), tableName);
            }
            else
            {
                sql = connInfo.GetSelectSql<T>(false, onlyFields, tableName);
            }
            if (forUpdate && connInfo.SupportForUpdate) sql += " for update";
            TableInfo ti = GetTableInfo(typeof(T), tableName);
            RequestBase param = new RequestBase();
            int j = 0;
            foreach (string pkName in ti.PKeys)
            {
                param.Add(pkName, pkValues[j]);
                ++j;
            }
            return connInfo.Db.QueryFirstOrDefault<T>(sql, param, connInfo.Transaction, commandType: CommandType.Text);
        }
        public static T Select<T>(this DbContext connInfo, object[] pkValues, Expression<Func<T, object>> onlyFields = null, bool isExceptField = false, string tableName = "", bool forUpdate = false)
            where T : new()
        {
            return SelectPrimitive<T>(connInfo, pkValues,SmartCrudHelper.GetPropertyNames<T>(onlyFields), isExceptField, tableName,forUpdate:forUpdate);
        }
        public static IEnumerable<T> Select<T>(this DbContext connInfo, RequestBase param, Expression<Func<T, object>> onlyFields = null, bool isExceptField = false, string tableName = "")
            where T : new()
        {
            return SelectPrimitive<T>(connInfo, param, onlyFields?.GetPropertyNames<T>(), isExceptField, tableName);
        }
        public static IEnumerable<T> Select<T>(this DbContext db, string fields, RequestBase para, bool isExceptField, string tableName) where T : new()
        {
            return db.SelectPrimitive<T>(para, fields.Split(new char[] { ',', ';' }, StringSplitOptions.RemoveEmptyEntries), isExceptField, tableName);
        }
        public static IEnumerable<T> SelectPrimitive<T>(this DbContext connInfo, RequestBase param, IEnumerable<string> onlyFields, bool isExceptField = false, string tableName = "")
            where T : new()
        {
            string sql = "";
            if (isExceptField && 0 < (onlyFields?.Count() ?? 0))//排除字段
            {
                sql = connInfo.GetSelectSql<T>(param, GetFields(typeof(T), true, onlyFields.ToArray()), tableName);
            }
            else
            {
                sql = connInfo.GetSelectSql<T>(param, onlyFields, tableName);
            }
            return connInfo.Db.Query<T>(sql, param, connInfo.Transaction, commandType: CommandType.Text);
        }
        #endregion
        #region Delete
        public static int Delete<T>(this DbContext connInfo, string filter, object param, string tableName = "")
            where T : new()
        {
            StringBuilder sqlbld = new StringBuilder(256);
            sqlbld.Concat("DELETE FROM ", GetTableName<T>(connInfo.DbType, tableName));
            if (!filter.StartsWith("WHERE", StringComparison.OrdinalIgnoreCase)) sqlbld.Append(" WHERE ");
            sqlbld.Append(filter);
            return connInfo.ExecuteNonQuery(sqlbld.ToString(), param);
        }
        /// <summary>
        /// 传入主键值
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="PkValues"></param>
        /// <returns></returns>
        public static int Delete<T>(this DbContext connInfo, object[] pkValues, string tableName = "")
            where T : new()
        {
            int pkCount = pkValues?.Length ?? 0;
            if (0 == pkCount)
                throw new Exception("Primary keys are not specified!");
            Type t = typeof(T);
            RequestBase param = new RequestBase();
            TableInfo tableInfo = GetTableInfo(t, 0, tableName);
            if (0 == (tableInfo?.PKeys?.Count() ?? 0))
                throw new ArgumentNullException("PKeys");
            int j = 0;
            foreach (string pkField in tableInfo.PKeys)
            {
                param.Add(pkField, pkValues[j]);
                ++j;
            }
            return connInfo.Db.Execute(connInfo.GetDeleteSql(t, tableName), param, connInfo.Transaction, commandType: CommandType.Text);
        }
        public static int Delete<T>(this DbContext connInfo, RequestBase param, string tableName = "")
           where T : new()
        {
            if (0 == (param?.Count ?? 0))
                throw new ArgumentNullException("param");
            string sql = connInfo.GetDeleteSql(typeof(T), tableName, (from p in param select p.Key));
            return connInfo.Db.Execute(sql, param, connInfo.Transaction, commandType: CommandType.Text);
        }
        public static int Delete<T>(this DbContext connInfo, IEnumerable<T> records, string tableName = "") where T : new()
        {
            return connInfo.Db.Execute(connInfo.GetDeleteSql(typeof(T), tableName), records, connInfo.Transaction, commandType: CommandType.Text);
        }
        public static int Delete(this DbContext connInfo, string tableName, RequestBase param)
        {
            StringBuilder sqlbld = new StringBuilder(128);
            sqlbld.Append($"Delete from {tableName.DbDelimiter(connInfo)} ");
            int i = 0;
            foreach (var ele in param)
            {
                if (i > 0)
                    sqlbld.Append(" and ");
                else
                    sqlbld.Append(" where ");
                sqlbld.Append(connInfo.GetParaPair(ele.Key));
                ++i;
            }
            return connInfo.Db.Execute(sqlbld.ToString(), param, connInfo.Transaction, commandType: CommandType.Text);
        }
        public static int Delete<T>(this DbContext connInfo, T record, Expression<Func<T, object>> matchFields = null, string tableName = "") where T : new()
        {
            return DeletePrimitive<T>(connInfo, record, matchFields?.GetPropertyNames<T>(), tableName);
        }
        public static int DeletePrimitive<T>(this DbContext connInfo, T record, IEnumerable<string> matchFields = null, string tableName = "") where T : new()
        {
            return connInfo.Db.Execute(connInfo.GetDeleteSql(typeof(T), tableName, matchFields), record, connInfo.Transaction, commandType: CommandType.Text);
        }
        #endregion
        #region EXISTS
        public static bool Exists<T>(this DbContext connInfo, object[] pkValues, string tableName = "", bool forUpdate = false)
            where T : new()
        {
            if (0 == (pkValues?.Length ?? 0))
                throw new Exception("Primary keys are missing!");
            Type t = typeof(T);
            TableInfo tableInfo = GetTableInfo(t, 0, tableName);
            if (0 == (tableInfo?.PKeys?.Count() ?? 0))
                throw new ArgumentNullException("PKeys");
            RequestBase param = new RequestBase();
            int j = 0;
            foreach (string pkName in tableInfo.PKeys)
            {
                param.Add(pkName, pkValues[j]);
                ++j;
            }
            string sql = connInfo.GetExistsSql(t, tableName);
            if (forUpdate && connInfo.SupportForUpdate) sql += " for update";
            return connInfo.Db.ExecuteScalar<string>(sql, param, connInfo.Transaction, commandType: CommandType.Text).IsTrue();
        }
        public static bool Exists<T>(this DbContext connInfo, T record, string tableName = "",bool forUpdate=false ) where T : new()
        {
            string sql = connInfo.GetExistsSql(typeof(T), tableName);
            if (forUpdate && connInfo.SupportForUpdate) sql += " for update";
            return connInfo.Db.ExecuteScalar<string>(sql, record, connInfo.Transaction, commandType: CommandType.Text).IsTrue();
        }
        public static bool ExistsInTable(this DbContext connInfo, string tableName, RequestBase param)
        {
            StringBuilder sqlbld = new StringBuilder(128);
            sqlbld.Concat(" select 'Y' from ", connInfo.DbDelimiter(tableName, true));
            int i = 0;
            foreach (var ele in param)
            {
                if (i == 0)
                    sqlbld.Append(" where ");
                else
                    sqlbld.Append(" and ");
                sqlbld.Append(connInfo.GetParaPair(ele.Key));
                ++i;
            }
            return connInfo.Db.ExecuteScalar<string>(sqlbld.ToString(), param, connInfo.Transaction, commandType: CommandType.Text).IsTrue();
        }
        public static bool Exists<T>(this DbContext connInfo, RequestBase param, string tableName = "")
            where T : new()
        {
            return ExistsInTable(connInfo, GetTableName<T>(connInfo.DbType, tableName), param);
        }
        /// <summary>
        /// 使用普通的SELECT语句,这个成本有点高
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static bool ExistsCount(this DbContext connInfo, string sql, object param = null)
        {
            return 0 < Count(connInfo, sql, param);
        }
        public static bool ExistsFilter<T>(this DbContext connInfo, string filter, object param = null, string tableName = "") where T : new()
        {
            if (string.IsNullOrWhiteSpace( filter) && null != param)
                throw new ArgumentNullException(filter);
            string sql = $"select 'Y' FROM {GetTableName<T>(connInfo.DbType, tableName, true)}";
            if (!string.IsNullOrWhiteSpace(filter))
            {
                if (!filter.TrimStart(new char[] { ' ' }).StartsWith("where", StringComparison.OrdinalIgnoreCase))
                    sql += " WHERE ";
                sql += filter;
            }
            return ExistsPrimitive(connInfo, sql, param);
        }
        /// <summary>
        /// 返回的字段不能为NULL,最好就是 'Y'
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="sql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static bool ExistsPrimitive(this DbContext connInfo, string sql, object param = null)
        {
            object result = connInfo.ExecuteScalar(sql, param);
            return !SmartCrudHelper.IsNullOrDBNull(result);
        }
        #endregion
        #region Insert
        public static int Insert<T>(this DbContext connInfo, T record, SqlFunction repl = null, Expression<Func<T, object>> onlyFields = null, string tableName = "")
            where T : new()
        {
            return InsertPrimitive<T>(connInfo, new[] { record }, repl, SmartCrudHelper.GetPropertyNames<T>(onlyFields), tableName);
        }
        public static int InsertPrimitive<T>(this DbContext connInfo, T record, SqlFunction repl = null, IEnumerable<string> onlyFields = null, string tableName = "")
            where T : new()
        {
            return connInfo.InsertPrimitive<T>(new[] { record }, repl, onlyFields, tableName);
        }
        /// <summary>
        /// 会排除自动增长列
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="Record"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static int InsertPrimitive<T>(this DbContext connInfo, IEnumerable<T> records, SqlFunction repl = null, IEnumerable<string> onlyFields = null, string tableName = "")
            where T : new()
        {
            string sql = connInfo.GetInsertSql<T>(repl, onlyFields, tableName);
            if (connInfo.DbType != DbConnType.ODPNET)
            {
                return connInfo.Db.Execute(sql, records, connInfo.Transaction, commandType: CommandType.Text);
            }
            else
            {
                Type t = typeof(T);
                TableInfo tblInfo = EntityReader.GetEntityMapping(t, true);
                Dictionary<string, PropertyInfo> fields = tblInfo.PropertyMappings;
                int count = 0;
                foreach (T record in records)
                {
                    RequestBase param = new RequestBase();
                    foreach (KeyValuePair<string, PropertyInfo> item in fields)
                    {
                        if (tblInfo.IsCurrentTimeField(item.Key)
                            || (null != repl && repl.ExistsField(item.Key))) continue;
                        param.Add(item.Key, SqlBuilder.GetPropertyValue<T>(record, item, connInfo.DbType));
                    }
                    count += connInfo.Db.Execute(sql, param, connInfo.Transaction, commandType: CommandType.Text);
                }
                return count;
            }
        }
        public static int Insert<T>(this DbContext connInfo, IEnumerable<T> records, SqlFunction repl = null, Expression<Func<T, object>> onlyFields = null, string tableName = "")
            where T : new()
        {
            return InsertPrimitive<T>(connInfo, records, repl, onlyFields?.GetPropertyNames<T>(), tableName);
        }
        public static int Insert<T>(this DbContext connInfo, RequestBase param, string tableName = "")
            where T : new()
        {
            if (0 == (param?.Count ?? 0)) throw new ArgumentNullException("param");
            string sql = SqlBuilder.GetInsertSql<T>(connInfo, null, (from p in param select p.Key), tableName);
            /*以下也可以
            StringBuilder sqlbld = new StringBuilder(512);
            sqlbld.StrBldAppend("INSERT INTO ", GetTableName<T>(connInfo.DbType, tableName));
            sqlbld.Append("(");
            sqlbld.Append(string.Join(",", param.Select(c => c.Key.DbDelimiter(connInfo))));
            sqlbld.Append(") VALUES (");
            sqlbld.Append(string.Join(",", param.Select(c => connInfo.TreatParaName(c.Key))));
            sqlbld.Append(")");
            */
            return connInfo.Db.Execute(sql, param, connInfo.Transaction, commandType: CommandType.Text);
        }
        #endregion
        #region Update
        public static int Update<T>(this DbContext connInfo, T record, SqlFunction repl = null, Expression<Func<T, object>> onlyFields = null, string tableName = "")
           where T : new()
        {
            return UpdatePrimitive<T>(connInfo, new[] { record }, repl, onlyFields?.GetPropertyNames<T>(), tableName);
        }
        public static int UpdatePrimitive<T>(this DbContext connInfo, T record, SqlFunction repl = null, IEnumerable<string> onlyFields = null, string tableName = "")
         where T : new()
        {
            return UpdatePrimitive<T>(connInfo, new[] { record }, repl, onlyFields, tableName);
        }
        public static int Update<T>(this DbContext connInfo, IEnumerable<T> records, SqlFunction repl = null, Expression<Func<T, object>> onlyFields = null, string tableName = "")
            where T : new()
        {
            return UpdatePrimitive<T>(connInfo, records, repl, onlyFields?.GetPropertyNames<T>(), tableName);
        }
        public static int UpdatePrimitive<T>(this DbContext connInfo, IEnumerable<T> records, SqlFunction repl = null, IEnumerable<string> onlyFields = null, string tableName = "")
            where T : new()
        {
            string sql = connInfo.GetUpdateSql<T>(repl, onlyFields, tableName);
            return connInfo.Db.Execute(sql, records, connInfo.Transaction, commandType: CommandType.Text);
        }
        #endregion
    }
}
