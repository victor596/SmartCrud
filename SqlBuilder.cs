using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Collections.Concurrent;
using System.Reflection;
using System.Data;
using System.Linq.Expressions;
using System.Data.Common;
using static SmartCrud.SmartCrudHelper;
using Dapper;
namespace SmartCrud
{
    public class StringGuidHandler : SqlMapper.TypeHandler<string>
    {
        public override void SetValue(IDbDataParameter parameter, string value)
        {
            parameter.Value = value;
        }
        public override string Parse(object value)
        {
            if (value is string ret)
                return ret;
            else if (value is Guid ret2)
                return ret2.ToString().ToUpper();
            return value?.ToString();
        }
    }
    public static class SqlBuilder
    {
        private readonly static ConcurrentDictionary<string, string> tblFields = new ConcurrentDictionary<string, string>();
        private readonly static ConcurrentDictionary<string, string> insertStr = new ConcurrentDictionary<string, string>();
        private readonly static ConcurrentDictionary<string, string> updateStr = new ConcurrentDictionary<string, string>();
        private readonly static ConcurrentDictionary<string, string> existsStr = new ConcurrentDictionary<string, string>();
        private readonly static ConcurrentDictionary<string, TableInfo> dicTableInfo = new ConcurrentDictionary<string, TableInfo>();
        private readonly static ConcurrentDictionary<string, string> dicDeleteStr = new ConcurrentDictionary<string, string>();
        readonly static ConcurrentDictionary<string, string> dicSelectStr = new ConcurrentDictionary<string, string>();
        static SqlBuilder()
        {
            Dapper.SqlMapper.AddTypeHandler(typeof(string), new StringGuidHandler());
        }
        internal static void LoadTypeHandler()
        { 
            
        }
        /// <summary>
        /// 从TypeDefine取类型的SQL表信息，包括表名和主键定义
        /// </summary>
        /// <param name="typeName"></param>
        /// <returns></returns>
        public static TableInfo GetTableInfo(Type t, int pkCount = 0, string tableName = "")
        {
            if (null == t) return null;
            string key = $"tblInfo:{t.FullName}:{pkCount}:{tableName}";
            TableInfo result = null;
            if (dicTableInfo.TryGetValue(key, out result)) return result;
            result = EntityReader.GetEntityMapping(t);
            if (!string.IsNullOrEmpty(tableName)) result.TableName = tableName;
            if (0 == (result?.PKeys?.Count ?? 0))//取前N个属性
            {
                if (0 < pkCount)
                {
                    foreach (KeyValuePair<string, PropertyInfo> ele in result.PropertyMappings.Take(pkCount))
                    {
                        result.PKeys.Add(ele.Key);
                    }
                }
            }
            dicTableInfo.TryAdd(key, result);
            return result;
        }
        public static TableInfo GetTableInfo(Type t, string tableName = "") => GetTableInfo(t, 0, tableName);
        public static SqlFunction GetCurrentTimeFunction(this DbContext conn, params string[] fields)
        {
            if (0 == (fields?.Length ?? 0)) return null;
            SqlFunction result = new SqlFunction();
            string repl = conn.GetCurrentTimeFuncName();
            foreach (string field in fields)
                result.AddSqlFunction(field, repl);
            return result;
        }
        public static string DbDelimiter(this DbConnType dbConnType, string TableOrFieldName, bool checkSymExists = false)
        {
            if (dbConnType == DbConnType.MSSQL)
            {
                if (checkSymExists)
                    return TableOrFieldName.StartsWith("[") ? TableOrFieldName : $"[{TableOrFieldName}]";
                else
                    return $"[{TableOrFieldName}]";
            }
            else if (dbConnType.IsMySQL())
            {
                if (checkSymExists)
                    return TableOrFieldName.StartsWith("`") ? TableOrFieldName : $"`{TableOrFieldName}`";
                else
                    return $"`{TableOrFieldName}`";
            }
            return TableOrFieldName;
        }
        public static string DbDelimiter(this DbContext conn, string TableOrFieldName, bool checkSymExists = false)
            => DbDelimiter(conn.DbType, TableOrFieldName, checkSymExists);
        public static string DbDelimiter(this string TableOrFieldName, DbConnType dbConnType, bool checkSymExists = false)
            => dbConnType.DbDelimiter(TableOrFieldName, checkSymExists);
        public static string DbDelimiter(this string TableOrFieldName, DbContext conn, bool checkSymExists = false)
            => conn.DbDelimiter(TableOrFieldName, checkSymExists);
        public static string ConvertToMySqlPagingSql(string sql,int start,int rows) => $"{sql} limit {start},{rows}";
        public static string ConvertToSqlitePagingSql(string sql, int start, int rows) => $"{sql} limit {rows},offset {start}";
        public static string ConvertToPostgreSqlPagingSql(string sql, int start, int rows) => $"{sql} limit {rows},offset {start}";
        public static string ConvertToMsSqlPagingSql(string sql, out bool converted)
        {
            converted = false;
            if (!sql.StartsWith("select", StringComparison.OrdinalIgnoreCase)) return string.Empty;
            int lastIndexOfOrderby = sql.LastIndexOf("order by", StringComparison.OrdinalIgnoreCase);
            if (lastIndexOfOrderby < 15) return string.Empty;
            string selectStatementWithoutSelect = sql.Substring(0, lastIndexOfOrderby).Substring(6); //sql不带order by和select
            string orderByStatement = sql.Substring(lastIndexOfOrderby); //order by 
            if (!sql.EndsWith(orderByStatement, StringComparison.OrdinalIgnoreCase)) return string.Empty; //判断order by是不是在最后
            converted = true;
            return $@"select top (select @pageSize) * 
	from (select row_number() over({orderByStatement}) as rownumber,{selectStatementWithoutSelect} ) temp_rows where rownumber> @startIndex"; //(@pageIndex-1)*@pageSize
        }
        public static string ConvertToOraclePagingSql(string sql, out bool converted)
        {
            converted = false; //单表查询
            sql = sql.Replace("\r\n", " ").Replace('\n',' ').Replace('\t', ' ');
            if (!sql.StartsWith("select", StringComparison.OrdinalIgnoreCase)) return string.Empty;
            bool useSingleTableQuery = false;//单表查询
            int indexOfOrderby = sql.IndexOf(" order by ", StringComparison.OrdinalIgnoreCase);
            int indexOfWhere = sql.IndexOf(" where ", StringComparison.OrdinalIgnoreCase);
            int indexOfGroupby = sql.IndexOf(" group by ", StringComparison.OrdinalIgnoreCase);
            int indexOfFrom = sql.IndexOf(" from ", StringComparison.OrdinalIgnoreCase);
            if (indexOfFrom == -1)
            {
                indexOfFrom = sql.IndexOf("*from ", StringComparison.OrdinalIgnoreCase);
                indexOfFrom++;
            }
            int indexOfJoin = sql.IndexOf(" join ", StringComparison.OrdinalIgnoreCase);
            //get table name
            string strFrom = sql.Substring(indexOfFrom); //From之后的
            string tableName = "";
            if (0 < indexOfWhere)
                tableName = strFrom.Substring(5, strFrom.IndexOf("where ", StringComparison.OrdinalIgnoreCase) - 5);
            else if (0 < indexOfOrderby)
                tableName = strFrom.Substring(5, strFrom.IndexOf("order by ", StringComparison.OrdinalIgnoreCase) - 5);
            else
                tableName = strFrom;
            bool singleTable = -1== tableName.IndexOf(',');
            //单表查询
            useSingleTableQuery = -1 == indexOfGroupby && -1 == indexOfJoin && singleTable;
            if (useSingleTableQuery)
            {
                if (-1 == indexOfOrderby) //没有orderby这种方式最快
                {
                    string whereOrAnd = -1 == indexOfWhere ? " where " : " and ";
                    return $"SELECT * FROM (SELECT ROWNUM AS rowno,{sql.Substring(6)} {whereOrAnd} ROWNUM <= :endIndex) t_tmp_000  WHERE rowno > :startIndex";
                }
                else
                {
                    string strField = sql.Substring(6, indexOfFrom - 6);//字段 取select 和 from中间的字符串
                    string[] arrFields = strField.Split(new char[] { ',', ' ' }, StringSplitOptions.RemoveEmptyEntries);
                    StringBuilder strbld = new StringBuilder(256);
                    strbld.Concat("select ", string.Join(",", from p in arrFields select $"t2.{p}"));
                    strbld.Append(" FROM (SELECT RID FROM (SELECT R.RID, ROWNUM LINENUM FROM (SELECT ROWID RID ");
                    strbld.Append(strFrom);
                    strbld.Append($") R WHERE ROWNUM <= :endIndex) WHERE LINENUM > :startIndex) T1, {tableName} T2 WHERE T1.RID = T2.ROWID");
                    return strbld.ToString();
                }
            }
            else
            {
                if (-1 == indexOfOrderby && -1== indexOfGroupby) //无排序语句,粗略的判断忽略子查询
                {
                    string whereOrAnd = "";
                    int lstIndexOfWhere = sql.LastIndexOf(" where ", StringComparison.OrdinalIgnoreCase);
                    if (-1 == lstIndexOfWhere) //没有where子句
                        whereOrAnd = " where ";
                    else //有WHERE子句要判断是子查询里还是子查询外,计算 where之后的 ( )数量 如果 )的数量多于(就是在子查询里
                    {
                        string tmp = sql.Substring(lstIndexOfWhere + 5);
                        int lc = tmp.Count(c => c == '(');
                        int rc = tmp.Count(c => c == ')');
                        if (lc == rc) //where语句在子查询外
                            whereOrAnd = " and ";
                        else
                            whereOrAnd = " where ";
                    }
                    return $"SELECT * FROM (SELECT ROWNUM AS rowno,{sql.Substring(6)} {whereOrAnd} ROWNUM <= :endIndex) t_tmp_000  WHERE rowno > :startIndex";
                }
                return $"SELECT * FROM (SELECT t_tmp_000.*, ROWNUM AS rowno FROM ( {sql}) t_tmp_000 WHERE ROWNUM <= :endIndex) table_alias WHERE table_alias.rowno > :startIndex";
            }
        }
        private static string GetFieldsKey(IEnumerable<string> fields)
        {
            if (null == fields || 0 == (fields?.Count() ?? 0)) return string.Empty;
            return string.Join(",", fields.OrderBy(c => c));
        }
        public static string GetSelectSql<T>(this DbContext db, RequestBase para, IEnumerable<string> onlyFields = null, string tableName = "") where T : new()
        {
            string sql = GetSelectSql<T>(db, true, onlyFields, tableName);
            int i = 0;
            foreach (var par in para)
            {
                if (0 == i)
                    sql += " where ";
                else
                    sql += " and ";
                sql += GetParaPair(db, par.Key);
                ++i;
            }
            return sql;
        }
        public static string GetSelectSql<T>(this DbContext db, bool isAll, IEnumerable<string> onlyFields = null, string tableName = "") where T : new()
        {
            string result = "";
            Type t = typeof(T);
            string key = $"{t.FullName}:select:{db.DbType.ToString()}:{isAll}:{tableName}:{GetFieldsKey(onlyFields)}";
            if (dicSelectStr.TryGetValue(key, out result)) return result;
            StringBuilder sqlbld = new StringBuilder(512);
            string fieldStr = "";
            if (0 == (onlyFields?.Count() ?? 0))
                fieldStr = SqlBuilder.GetFieldStr<T>(db.DbType);
            else
                fieldStr = string.Join(",", onlyFields);
            TableInfo ti = GetTableInfo(t, tableName: tableName);
            sqlbld.Concat("SELECT ", fieldStr, " FROM ", db.DbDelimiter(ti.TableName));
            if (!isAll)
            {
                sqlbld.Concat(" WHERE ");
                int i = 0;
                foreach (string pk in ti.PKeys)
                {
                    if (i > 0) sqlbld.Concat(" and ");
                    sqlbld.Concat(GetParaPair(db, pk));
                    ++i;
                }
            }
            result = sqlbld.ToString();
            dicSelectStr.TryAdd(key, result);
            return result;
        }
        public static string GetDeleteSql(this DbContext db, Type t, string tableName = "",IEnumerable<string> matchFields=null)
        {
            bool noType = false;
            if (null == t)
            {
                if(string.IsNullOrEmpty(tableName))
                    throw new ArgumentNullException("t/tableName");
                if (0 == (matchFields?.Count() ?? 0))
                    throw new ArgumentNullException("matchFields");
                noType = true;
            }
            TableInfo ti = null!=t ? GetTableInfo(t, tableName):null;
            string key = $"{t?.FullName?? ti?.TableName??tableName }:delete:{db.DbType.ToString()}:{tableName}:{GetFieldsKey(matchFields)}";
            string result = "";
            if (dicDeleteStr.TryGetValue(key, out result)) return result;
            StringBuilder sqlbld = new StringBuilder(128);
            if (!noType)
            {
                if (0 == (ti?.PKeys?.Count() ?? 0))
                    throw new ArgumentNullException("PKeys");
                sqlbld.Concat("DELETE FROM ", ti.TableName.DbDelimiter(db, true), " WHERE ");
                int j = 0;
                if (0 < (matchFields?.Count() ?? 0))
                {
                    foreach (string field in matchFields)
                    {
                        if (j > 0) sqlbld.Append(" AND ");
                        sqlbld.Concat( db.GetParaPair(field));
                        ++j;
                    }
                }
                else
                {
                    foreach (string pkField in ti.PKeys)
                    {
                        if (j > 0) sqlbld.Append(" AND ");
                        sqlbld.Append(db.GetParaPair(pkField));
                        ++j;
                    }
                }
            }
            else
            {
                sqlbld.Concat("delete from ", db.DbDelimiter(ti?.TableName??tableName, true), " where ");
                int j = 0;
                foreach (string field in matchFields)
                {
                    if (j > 0) sqlbld.Append(" AND ");
                    sqlbld.Append(db.GetParaPair(field));
                    ++j;
                }
            }
            result = sqlbld.ToString();
            dicDeleteStr.TryAdd(key, result);
            return result;
        }
        public static string GetExistsSql(this DbContext connInfo, Type t, string tableName = "")
        {
            string key = $"{t.FullName}:exists:{connInfo.DbType.ToString()}:{tableName}";
            string result = "";
            if (existsStr.TryGetValue(key, out result)) return result;
            StringBuilder sqlbld = new StringBuilder(128);
            sqlbld.Append("SELECT 'Y' FROM ");
            TableInfo tableInfo = GetTableInfo(t, 0, tableName);
            if (0 == (tableInfo?.PKeys?.Count() ?? 0))
                throw new ArgumentNullException("PKeys");
            sqlbld.Concat(connInfo.DbDelimiter(tableInfo.TableName, true));
            int j = 0;
            foreach (string pkName in tableInfo.PKeys)
            {
                if (0 != j)
                    sqlbld.Append(" AND ");
                else
                    sqlbld.Append(" where ");
                sqlbld.Append(connInfo.GetParaPair(pkName));
                ++j;
            }
            result = sqlbld.ToString();
            existsStr.TryAdd(key, result);
            return result;
        }
        /// <summary>
        /// 取INSERT语句的定义串
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static string GetInsertSql<T>(this DbContext connInfo, SqlFunction sqlFunc = null, IEnumerable<string> onlyFields = null, string tableName = "")
            where T : new()
        {
            Type t = typeof(T);
            string key = $"{t.FullName}:insert:{connInfo.DbType.ToString()}:{tableName}:{GetFieldsKey(onlyFields)}:{sqlFunc?.ToString()}";
            string result = "";
            if (insertStr.TryGetValue(key, out result)) return result;
            TableInfo tableInfo = EntityReader.GetEntityMapping(t, true);
            bool specFields = (0 < (onlyFields?.Count() ?? 0)); //指定栏位
            Dictionary<string, PropertyInfo> fieldDefine = tableInfo.PropertyMappings;
            StringBuilder strbld = new StringBuilder(256);
            strbld.Concat("INSERT INTO ", GetTableName<T>(connInfo.DbType, tableName, true), " (");
            if (!specFields)
                strbld.Append(string.Join(",", fieldDefine.Select(c => connInfo.DbDelimiter(c.Key)).ToArray()));
            else
            {
                strbld.Append(string.Join(",", onlyFields));
            }
            strbld.Append(") VALUES (");
            if (!specFields)
            {
                strbld.Append(string.Join(",", fieldDefine.Select(c =>
                   tableInfo.IsCurrentTimeField(c.Key) ? connInfo.DbType.GetCurrentTimeFuncName() :
                      (sqlFunc?.ExistsField(c.Key) ?? false) ? sqlFunc?.GetSqlFunction(c.Key) :
                       connInfo.DbType.TreatParaName(c.Key)
                    ).ToArray()));
            }
            else
            {
                strbld.Append(string.Join(",", onlyFields.Select(c =>
                   tableInfo.IsCurrentTimeField(c) ? connInfo.DbType.GetCurrentTimeFuncName() :
                      (sqlFunc?.ExistsField(c) ?? false) ? sqlFunc?.GetSqlFunction(c) :
                       connInfo.DbType.TreatParaName(c)
                    ).ToArray()));
            }
            strbld.Append(" )");
            result = strbld.ToString();
            insertStr.TryAdd(key, result);
            return result;
        }
        /// <summary>
        /// 生成UPDATE的语句
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connType"></param>
        /// <param name="pkCount"></param>
        /// <returns></returns>
        public static string GetUpdateSql<T>(this DbContext connInfo, SqlFunction sqlFunc = null, IEnumerable<string> onlyFields = null, string tableName = "")
            where T : new()
        {
            Type t = typeof(T);
            string key = $"{t.FullName}:update:{connInfo.DbType.ToString()}:{tableName}:{GetFieldsKey(onlyFields)}:{sqlFunc?.ToString()}";
            string result = "";
            if (updateStr.TryGetValue(key, out result)) return result;
            TableInfo ti = GetTableInfo(t, tableName);
            if (0 == (ti?.PKeys?.Count ?? 0))
                throw new Exception("Primary keys are missing!");
            bool specFields = (0 < (onlyFields?.Count() ?? 0)); //指定栏位
            Dictionary<string, PropertyInfo> fieldDefine = EntityReader.GetTypePropertyMapping(t);
            StringBuilder strbld = new StringBuilder(256);
            strbld.Concat("UPDATE ", connInfo.DbDelimiter(ti.TableName), " SET ");
            //非主键
            if (!specFields)
            {
                strbld.Append(string.Join(",", fieldDefine.Where(c =>
                   !ti.PKeys.Contains(c.Key, StringComparer.OrdinalIgnoreCase)).Select(c =>
                       string.Format("{0}={1}", connInfo.DbDelimiter(c.Key),
                       ti.IsCurrentTimeField(c.Key) ? connInfo.GetCurrentTimeFuncName() :
                         (sqlFunc?.ExistsField(c.Key) ?? false) ? sqlFunc?.GetSqlFunction(c.Key) :
                              connInfo.TreatParaName(c.Key)
                       )).ToArray()));
            }
            else
            {
                strbld.Append(string.Join(",", onlyFields.Where(c =>
                  !ti.PKeys.Contains(c, StringComparer.OrdinalIgnoreCase)).Select(c =>
                      string.Format("{0}={1}", connInfo.DbDelimiter(c),
                      ti.IsCurrentTimeField(c) ? connInfo.GetCurrentTimeFuncName() :
                        (sqlFunc?.ExistsField(c) ?? false) ? sqlFunc?.GetSqlFunction(c) :
                             connInfo.TreatParaName(c)
                      )).ToArray()));
            }
            strbld.Append(" WHERE ");
            //主键
            strbld.Append(string.Join(" AND ", fieldDefine.Where(c =>
               ti.PKeys.Contains(c.Key, StringComparer.OrdinalIgnoreCase)).Select(c =>
                   string.Format("{0}={1}", connInfo.DbDelimiter(c.Key),
                   connInfo.TreatParaName(c.Key))).ToArray()));
            result = strbld.ToString();
            updateStr.TryAdd(key, result);
            return result;
        }
        /// <summary>
        /// 获取更新的语句
        /// </summary>
        /// <param name="tableName"></param>
        /// <param name="Columns"></param>
        /// <param name="Keys"></param>
        /// <param name="connType"></param>
        /// <returns></returns>
        public static string GetUpdateSqlStr(this DbConnType connType, string tableName, string[] Columns, string[] Keys)
        {
            StringBuilder strbld = new StringBuilder(512);
            strbld.AppendFormat("Update {0} set ", connType.DbDelimiter(tableName));
            for (int i = 0, count = Columns.Length; i < count; i++)
            {
                string fieldName = Columns[i];
                if (Keys.Contains(fieldName)) continue;
                string parName = connType.TreatParaName(fieldName);
                strbld.AppendFormat("{0}={1},", connType.DbDelimiter(fieldName), parName);
            }
            strbld.Remove(strbld.Length - 1, 1);
            strbld.Append(" where ");
            for (int i = 0, count = Keys.Length; i < count; i++)
            {
                if (0 != i) strbld.Append(" and ");
                string fieldName = Keys[i];
                string parName = connType.TreatParaName(fieldName);
                strbld.AppendFormat("{0}={1}", connType.DbDelimiter(fieldName), parName);
            }
            return strbld.ToString();
        }
        /// <summary>
        /// 返回 Id=@Id
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="paraName"></param>
        /// <returns></returns>
        public static string GetParaPair(this DbContext connInfo, string paraName)
        {
            return string.Format(" {0}={1} ", connInfo.DbDelimiter(paraName), connInfo.TreatParaName(paraName));
        }
        /// <summary>
        /// 取表的名称
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static string GetTableName<T>(DbConnType? dbType, string TableName = "", bool checkSymExists = false) where T : new()
        {
            string result = "";
            if (string.IsNullOrEmpty(TableName))
            {
                Type t = typeof(T);
                TableInfo ret = GetTableInfo(t, TableName);
                if (null != ret)
                    result = ret.TableName;
                else
                    result = t.Name;
            }
            else
            {
                result = TableName;
            }
            return dbType.HasValue ? result.DbDelimiter(dbType.Value, checkSymExists) : result;
        }
        /// <summary>
        /// 获取实体类的属性
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static List<string> GetFields(Type t, bool isExceptField = false, params string[] onlyFields)
        {
            bool specFields = 0 < (onlyFields?.Length ?? 0);
            if (isExceptField && !specFields) throw new ArgumentException("isExceptField/onlyFields");
            Dictionary<string, PropertyInfo> pi = EntityReader.GetTypePropertyMapping(t);
            if (null == pi || 0 == pi.Count) return null;
            List<string> result = new List<string>();
            foreach (var ele in pi)
            {
                if (!specFields)
                    result.Add(ele.Key);
                else
                {
                    if (isExceptField) //排除字段
                    {
                        if (onlyFields.Contains(ele.Key, StringComparer.OrdinalIgnoreCase)) continue;
                    }
                    else
                    {
                        if (!onlyFields.Contains(ele.Key, StringComparer.OrdinalIgnoreCase)) continue;
                    }
                    result.Add(ele.Key);
                }
            }
            return result;
        }
        /// <summary>
        /// 获取实体类的属性
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static IEnumerable<string> GetFields<T>(bool isExceptField = false, params string[] onlyFields) where T : class
        {
            return GetFields(typeof(T), isExceptField, onlyFields);
        }
        public static IEnumerable<string> GetFields<T>(bool isExceptField = false, Expression<Func<T, object>> exprFields = null) where T : class
          => GetFields<T>(isExceptField, exprFields?.GetPropertyNames<T>()?.ToArray());
        /// <summary>
        /// 取类型的字段集合用,连接
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        private static string GetFieldStrPri(this Type t, DbConnType? connType = null, string contact = ",", string prefix = "", bool isExceptField = false, params string[] onlyFields)
        {
             bool hasPre = !string.IsNullOrWhiteSpace(prefix);
            var fields = GetFields(t,isExceptField,onlyFields);
            if (0 == (fields?.Count() ?? 0)) return string.Empty;
            return string.Join(contact,connType==null?
               (!hasPre ? fields:(from p in fields select $"{prefix}.{p}")) : 
                from p in fields select hasPre?$"{prefix}.{connType.Value.DbDelimiter(p)}": connType.Value.DbDelimiter(p) );
        }
        /// <summary>
        /// 取类型的字段集合用,连接
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <returns></returns>
        public static string GetFieldStr<T>(DbConnType? connType = null, string contact = ",", string prefix = "", bool isExceptField = false, params string[] onlyFields) where T : new()
        {
            Type t = typeof(T);
            string key = $"{t.FullName}:{connType?.ToString()}:{contact}:{prefix}:{isExceptField}:{GetFieldsKey(onlyFields)}";
            string ret = "";
            if (tblFields.TryGetValue(key, out ret)) return ret;
            ret = GetFieldStrPri(t, connType, contact, prefix, isExceptField, onlyFields);
            if (!string.IsNullOrEmpty(ret)) tblFields.TryAdd(key, ret);
            return ret;
        }
        public static string GetFieldStr<T>(DbConnType? connType = null, string contact = ",", string prefix = "", bool isExceptField = false, Expression<Func<T, object>> exprFields = null) where T : new()
            => GetFieldStr<T>(connType, contact, prefix, isExceptField, exprFields?.GetPropertyNames<T>()?.ToArray());
        /// <summary>
        /// 给SQL语句拼结ORDER BY子句
        /// </summary>
        /// <param name="orderBy"></param>
        /// <param name="sql"></param>
        public static void AppendOrderBy(string orderBy, ref string sql)
        {
            if (string.IsNullOrEmpty(orderBy)) return;
            if (!orderBy.TrimStart(new char[] { ' ' }
                    ).StartsWith("ORDER BY", StringComparison.OrdinalIgnoreCase))
            {
                sql += " ORDER BY ";
            }
            sql += orderBy;
        }
        /// <summary>
        /// 给SQL语句拼结ORDER BY子句
        /// </summary>
        /// <param name="orderBy"></param>
        /// <param name="sql"></param>
        public static void AppendOrderBy(string orderBy, StringBuilder sqlbld)
        {
            if (string.IsNullOrEmpty(orderBy)) return;
            if (!orderBy.TrimStart(new char[] { ' ' }
                    ).StartsWith("ORDER BY", StringComparison.OrdinalIgnoreCase))
            {
                sqlbld.Append(" ORDER BY ");
            }
            sqlbld.Append(orderBy);
        }

        /// <summary>
        /// 记录中的NULL要转换成DBNULL.VALUE(只限于 DbParameter)
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="Record"></param>
        /// <param name="item"></param>
        /// <param name="connType"></param>
        /// <returns></returns>
        public static object GetPropertyValue<T>(T Record, KeyValuePair<string, PropertyInfo> item, DbConnType? connType)
        {
            object ret = item.Value.GetValue(Record, null);
            //没办法，ODP.NET不支持 bool,GUID
            if (connType.HasValue && connType == DbConnType.ODPNET)
            {
                Type t = SmartCrudHelper.GetInnerType(item.Value.PropertyType);
                if (t == GuidType)
                    return ret?.ToString().ToUpper();
                else if (t == Bool)
                    return (byte)(((bool)ret) ? 1 : 0);
                else if (t == SmartCrudHelper.SByte)
                    return Convert.ToByte(ret);
            }
            return ret;
        }
        /// <summary>
        /// [1,2,3] ===> (1,2,3) or ('1','2','3')
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="elements"></param>
        /// <param name="addQuota"></param>
        /// <returns></returns>
        public static string GetSQLInString<T>(this ICollection<T> elements, bool addQuota)
        {
            if (0 == (elements?.Count ?? 0)) return string.Empty;
            StringBuilder strbld = new StringBuilder(256);
            strbld.Append("(");
            strbld.Append(string.Join(",",
                from p in elements select !addQuota ? p.ToString() : $"'{p.ToString()}'"));
            strbld.Append(")");
            return strbld.ToString();
        }
        /// <summary>
        /// 批量增加SQL参数,参数名1，值1，参数名2，值2....
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="conditionList"></param>
        public static void AddParametersFromArray(this RequestBase pars, params object[] paraValues)
        {
            if (null == paraValues || 0 == paraValues.Length || 0 != (paraValues.Length % 2))
            {
                throw new ArgumentException("paraValues");
            }
            if (null == pars) pars = new RequestBase();
            for (int i = 0, count = paraValues.Length / 2; i < count; i++)
            {
                pars.SetValue(paraValues[i * 2].ToString(), paraValues[i * 2 + 1]);
            }
        }
        /// <summary>
        /// 从DATATABLE中取字段
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static IEnumerable<string> GetFields(this DataTable dt, params string[] exceptFields)
        {
            foreach (DataColumn dc in dt.Columns)
            {
                string fieldName = dc.ColumnName;
                if (exceptFields?.Contains(fieldName, StringComparer.OrdinalIgnoreCase) ?? false) yield break;
                yield return fieldName;
            }
        }
        /// <summary>
        /// 从DATATABLE中取主键字段s
        /// </summary>
        /// <param name="dt"></param>
        /// <returns></returns>
        public static string[] GetPrimaryFields(this DataTable dt)
        {
            DataColumn[] primaryKey = dt.PrimaryKey;
            if (null == primaryKey || 0 == primaryKey.Length) return null;
            int colCount = primaryKey.Length;
            string[] Fields = new string[colCount];
            for (int i = 0; i < colCount; i++) Fields[i] = primaryKey[i].ColumnName;
            return Fields;
        }
        /// <summary>
        /// 用','连接字段
        /// </summary>
        /// <param name="Columns"></param>
        /// <returns></returns>
        public static string GetFieldStringViaColumns(string[] Columns)
        {
            return string.Join(",", Columns).TrimEnd(',');
        }
        /// <summary>
        /// 把字段串转换成各数据库连接类型对应的形式,such as  @Name,@Id,@Code
        /// </summary>
        /// <param name="Columns"></param>
        /// <param name="connType"></param>
        /// <returns></returns>
        public static string GetParaStringViaColumns(this DbConnType connType, string[] Columns)
        {
            StringBuilder strbld = new StringBuilder(512);
            for (int i = 0, count = Columns.Length; i < count; i++)
            {
                strbld.AppendFormat("{0},", connType.TreatParaName(Columns[i]));
            }
            return strbld.ToString().TrimEnd(',');
        }
        public static SqlFunction CreateDbNowFunction(this DbContext conn, params string[] fields)
        {
            if (0 == (fields?.Count() ?? 0)) return null;
            SqlFunction result = new SqlFunction();
            string repl = conn.GetCurrentTimeFuncName();
            foreach (string field in fields)
                result.AddSqlFunction(field, repl);
            return result;
        }
        public static SqlFunction CreateDbNowFunction<T>(this DbContext conn, Expression<Func<T, object>> expr) where T : class, new()
        {
            return conn.CreateDbNowFunction(SmartCrudHelper.GetPropertyNames<T>(expr)?.ToArray());
        }
        public static RequestBase ConvertParameter(object param)
        {
            RequestBase result = new RequestBase();
            if (null == param) return result;
            IEnumerable<DbParameter> dbpars = param as IEnumerable<DbParameter>;
            if (null != dbpars)
            {
                foreach (var ele in dbpars)
                    result.SetValue(ele.ParameterName.TrimParaName(), ele.Value);
            }
            else
            {
                IDictionary<string, object> dic = AsDictionary(param);
                if (null != dic)
                {
                    foreach (var ele in dic)
                    {
                        result.SetValue(ele.Key, ele.Value);
                    }
                }
            }
            return result;
        }
    }
}
