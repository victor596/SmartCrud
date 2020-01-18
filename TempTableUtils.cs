using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using Dapper;
namespace SmartCrud
{
    public static class TempTableUtils
    {
        /// <summary>
        /// 获取临时表的名称
        /// </summary>
        /// <param name="connInfo"></param>
        /// <param name="tableName"></param>
        /// <returns></returns>
        public static string GetTempTableName(this DbContext connInfo, string tableName)
        {
            if (connInfo.DbType == DbConnType.MSSQL)
            {
                if (!tableName.StartsWith("#"))
                {
                    tableName = string.Format("#{0}", tableName);
                }
            }
            else
            {
                if (tableName.StartsWith("#"))
                {
                    tableName = tableName.TrimStart(new char[] { '#' });
                }
            }
            return tableName;
        }
        /// <summary>
        /// 插入临时表,ORACLE操作完之后一定要删除掉
        /// </summary>
        /// <param name="conn"></param>
        /// <param name="dt"></param>
        /// <param name="tableName"></param>
        /// <param name="funcCreateTable"></param>
        /// <param name="Fields"></param>
        public static void InsertTempTable(this DbContext conn, DataTable dt, string tableName,Action<DbContext> funcCreateTable, params string[] Fields)
        {
            if (string.IsNullOrEmpty(tableName)) tableName = dt.TableName;
            tableName = conn.GetTempTableName(tableName);
            if (0 == (Fields?.Length ?? 0))
                Fields = dt.GetDataTableColumns()?.ToArray();
            if (conn.DbType == DbConnType.MYSQL)
                conn.InsertMYSQLTmpTable(dt, tableName, funcCreateTable, Fields);
            else if (conn.IsOracleDb)
                conn.InsertOraTmpTable(dt, tableName, funcCreateTable, Fields);
            else if (conn.DbType == DbConnType.MSSQL)
                conn.InsertSQLTmpTable(dt, tableName, funcCreateTable, Fields);
        }
        public static void DeleteTempTable(this DbContext conn, string tableName)
        {
            tableName = conn.GetTempTableName(tableName);
            if (conn.DbType == DbConnType.MSSQL)
                conn.ExecuteNonQuery(GetDeleteSQLTmpTableString(tableName));
            else if (conn.DbType == DbConnType.MYSQL)
                conn.ExecuteNonQuery(GetDeleteMYSQLTmpTableString(tableName));
            else if (conn.IsOracleDb)
                conn.ExecuteNonQuery($"begin execute immediate 'truncate table {tableName}' ; end; ");
        }
        /// <summary>
        /// 把数据INSERT到MYSQL的临时表
        /// </summary>
        /// <param name="ConnInfo"></param>
        /// <param name="dt"></param>
        /// <param name="TableName"></param>
        /// <param name="LotCount"></param>
        /// <param name="Fields"></param>
        private static void InsertMYSQLTmpTable(this DbContext ConnInfo,
            DataTable dt, string TableName, Action<DbContext> funcCreateTable, params string[] Fields)
        {
            if (ConnInfo.DbType != DbConnType.MYSQL)
                throw new Exception("Only MYSQL support");
            if (null != funcCreateTable)
                funcCreateTable.Invoke(ConnInfo);
            else
            {
                ConnInfo.ExecuteNonQuery(GetDeleteMYSQLTmpTableString(TableName));//删除表
                ConnInfo.ExecuteNonQuery(GetCreateMYSQLTmpTableString(dt, TableName, Fields));//创建表
            }
            string sql = $"insert into {TableName}(";
            sql += string.Join(",", Fields);
            sql += ") values(";
            sql += string.Join(",", from p in Fields select $"#{p}#");
            sql += ")";
            RequestBase pars = new RequestBase();
            foreach (DataRow dr in dt.Rows)
            {
                foreach (string fieldName in Fields)
                    pars.SetValue(fieldName, dr[fieldName]);
                ConnInfo.ExecuteNonQuery(sql, pars);
            }
        }
        /// <summary>
        /// 获取清除MYSQL临时表的语句
        /// </summary>
        /// <param name="TmpTableName"></param>
        /// <returns></returns>
        private static string GetDeleteMYSQLTmpTableString(string TmpTableName)
        {
            return string.Format("DROP TEMPORARY TABLE IF EXISTS {0}", TmpTableName);
        }
        /// <summary>
        /// 删除MYSQL的临时表
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="TmpTableName"></param>
        private static void DeleteMySQLTmpTable(DbContext Conn, string TmpTableName)
        {
            Conn.ExecuteNonQuery(GetDeleteMYSQLTmpTableString(TmpTableName));
        }
        /// <summary>
        /// 获取建立MYSQL临时表的语句
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="TmpTableName"></param>
        /// <param name="Fields"></param>
        /// <returns></returns>
        private static string GetCreateMYSQLTmpTableString(
            DataTable dt, string TmpTableName, params string[] Fields)
        {
            StringBuilder strbld = new StringBuilder();
            strbld.AppendFormat(@"CREATE TEMPORARY TABLE IF NOT EXISTS {0} ( ", TmpTableName);
            foreach (string field in Fields)
            {
                DataColumn dc = dt.Columns[field];
                Type detdataType = dc.DataType;
                DATATYPE dataType = SmartCrudHelper.GetDataType(detdataType);
                strbld.AppendFormat(" {0} ", field);
                if (dataType == DATATYPE.NUMBER)
                {
                    if (SmartCrudHelper.IsInt(detdataType))
                        strbld.Append(" INT,");
                    else
                        strbld.Append(" DECIMAL(14,4),");
                }
                else if (dataType == DATATYPE.DATE)
                    strbld.Append(" DATETIME,");
                else
                {
                    strbld.AppendFormat(" VARCHAR({0}),",
                        dc.MaxLength > 0 ? dc.MaxLength : 56);
                }
            }
            string Ret = strbld.ToString().TrimEnd(new char[] { ',' });
            strbld = null;
            Ret += " )";
            return Ret;
        }
        /// <summary>
        /// SQL SERVER做临时表
        /// e.g.
        /// FwTempTable.InsertSQLTmpTable(connInfo, dtBra, "Insert into #tmpBra(BranchCode)values('{0}');", 100, "#tmpBra", new string[] { "BranchCode" });
        /// </summary>
        /// <param name="ConnInfo"></param>
        /// <param name="dt"></param>
        /// <param name="sql"></param>
        /// <param name="LotCount"></param>
        /// <param name="Fields"></param>
        private static void InsertSQLTmpTable(this DbContext ConnInfo,
           DataTable dt, string TmpTableName,
           Action<DbContext> funcCreateTable, params string[] Fields)
        {
            if (ConnInfo.DbType != DbConnType.MSSQL)
                throw new Exception("Only SQLSERVER support!");
            if (null != funcCreateTable)
                funcCreateTable.Invoke(ConnInfo);
            else
                ConnInfo.ExecuteNonQuery(GetCreateSQLTmpTableString(dt, TmpTableName, Fields));
            string sql = $"insert into {TmpTableName}(";
            sql += string.Join(",", Fields);
            sql += ") values(";
            sql += string.Join(",", from p in Fields select $"#{p}#");
            sql += ")";
            RequestBase pars = new RequestBase();
            foreach (DataRow dr in dt.Rows)
            {
                foreach (string fieldName in Fields)
                    pars.SetValue(fieldName, dr[fieldName]);
                ConnInfo.ExecuteNonQuery(sql, pars);
            }
        }
        /// <summary>
        /// 删除SQL SERVER的临时表
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="TmpTableName"></param>
        private static void DeleteSQLTmpTable(DbContext Conn, string TmpTableName)
        {
            Conn.ExecuteNonQuery(GetDeleteSQLTmpTableString(TmpTableName));
        }
        /// <summary>
        /// 获取清除SQL临时表的语句,#可有可无
        /// </summary>
        /// <param name="TmpTableName"></param>
        /// <returns></returns>
        private static string GetDeleteSQLTmpTableString(string TmpTableName)
        {
            return string.Format(@"if object_id('tempdb.dbo.{0}') is not null 
  begin
    truncate table {0};
    drop table {0};
  end;", TmpTableName);
        }
        /// <summary>
        /// 获取建立SQL临时表的语句
        /// </summary>
        /// <param name="dt"></param>
        /// <param name="TmpTableName"></param>
        /// <param name="Fields"></param>
        /// <returns></returns>
        private static string GetCreateSQLTmpTableString(
            DataTable dt, string TmpTableName, params string[] Fields)
        {
            StringBuilder strbld = new StringBuilder(1024);
            strbld.Append(GetDeleteSQLTmpTableString(TmpTableName));
            strbld.AppendFormat(@" create table {0} ( ", TmpTableName);
            foreach (string field in Fields)
            {
                DataColumn dc = dt.Columns[field];
                Type detdataType = dc.DataType;
                DATATYPE dataType = SmartCrudHelper.GetDataType(detdataType);
                strbld.AppendFormat(" {0} ", field);
                if (dataType == DATATYPE.NUMBER)
                {
                    if (SmartCrudHelper.IsInt(detdataType))
                        strbld.Append(" INT,");
                    else
                        strbld.Append(" DECIMAL(12,2),");
                }
                else if (dataType == DATATYPE.DATE)
                    strbld.Append(" DATETIME,");
                else
                {
                    strbld.AppendFormat(" VARCHAR({0}),",
                        dc.MaxLength > 0 ? dc.MaxLength : 56);
                }
            }
            string Ret = strbld.ToString().TrimEnd(new char[] { ',' });
            strbld = null;
            Ret += " )";
            return Ret;
        }
        /// <summary>
        /// ORACLE INSERT临时表,不会事先清除临时表
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="dt"></param>
        /// <param name="sql"></param>
        /// <param name="LotCount"></param>
        /// <param name="Fields"></param>
        private static void InsertOraTmpTable(this DbContext ConnInfo,
            DataTable dt, string TableName, Action<DbContext> funcCreateTable, params string[] Fields)
        {
            if (!ConnInfo.IsOracleDb)
                throw new Exception("Only ORACLE support!");
            funcCreateTable?.Invoke(ConnInfo);
            string sql = $"insert into {TableName}(";
            sql += string.Join(",", Fields);
            sql += ") values(";
            sql += string.Join(",", from p in Fields select $"#{p}#");
            sql += ")";
            RequestBase pars = new RequestBase();
            foreach (DataRow dr in dt.Rows)
            {
                foreach (string fieldName in Fields)
                    pars.SetValue(fieldName, dr[fieldName]);
                ConnInfo.ExecuteNonQuery(sql, pars);
            }
        }
    }
}
