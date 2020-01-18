using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using System.Data.Common;
namespace SmartCrud
{
    /// <summary>
    /// 数据库方言
    /// </summary>
    public static class Dbdialect
    {
        readonly static char[] dbParPrefix ;
        public readonly static string CURRENT_TIME ;
        public readonly static string IS_NULL ;
        static Dbdialect()
        {
            dbParPrefix = new char[] { ':', '@' };
            CURRENT_TIME = "#NOW#";
            IS_NULL = "#ISNULL#";
        }
        /// <summary>
        /// 取各种数据库ISNULL的函数
        /// </summary>
        /// <param name="connType"></param>
        /// <returns></returns>
        public static string GetIsNullFuncName(this DbConnType connType)
        {
            if (connType.IsOracle())
                return "NVL";
            else if (connType == DbConnType.MSSQL)
                return "ISNULL";
            else if (connType.IsMySQL())
                return "IFNULL";
            else if (connType == DbConnType.POSTGRESQL)
                return "NULLIF";
            else if (connType == DbConnType.SQLITE)
                return "IFNULL";
            else
                return "ISNULL";
        }
        public static string GetIsNullFuncName(this DbContext connInfo)
        {
            return connInfo.DbType.GetIsNullFuncName();
        }
        /// <summary>
        /// 返回时间加减的表达式1:DAY 2:HOUR 3:MINUTE 4:SECOND
        /// </summary>
        /// <param name="connType"></param>
        /// <param name="dateType"></param>
        /// <param name="Value"></param>
        /// <returns></returns>
        public static string GetDateAddString(this DbConnType connType, DATEPART dateType, int Value)
        {
            if (dateType == DATEPART.MS) throw new NotSupportedException("DATEPART.MS");
            string sym = "+";
            if (0>Value)//减
            {
                Value *= -1;
                sym = "-";
            }
            string result = "";
            switch (dateType)
            {
                case  DATEPART.DAY: //天数
                    if (connType.IsMySQL())
                        result = $"{sym} INTERVAL {Value} DAY";
                    else if (connType == DbConnType.POSTGRESQL)
                        result = $"{sym} interval '{Value} days'";
                    else
                        result = $"{sym}{Value}";
                    break;
                case DATEPART.HOUR: //小时
                    if (connType.IsMySQL())
                        result = $"{sym} INTERVAL {Value} HOUR";
                    else if (connType == DbConnType.POSTGRESQL)
                        result = $"{sym} interval '{Value} hours'";
                    else
                        result = $"{sym}{Value}/24.0";
                    break;
                case DATEPART.MINUTE: //分时
                    if (connType.IsMySQL())
                        result = $"{sym} INTERVAL {Value} MINUTE";
                    else if (connType == DbConnType.POSTGRESQL)
                        result = $"{sym} interval '{Value} minutes'";
                    else
                        result = $"{sym}{Value}/1440.0";
                    break;
                case DATEPART.SECOND:
                    if (connType.IsMySQL())
                        result = $"{sym} INTERVAL {Value} seconds";
                    else if (connType == DbConnType.POSTGRESQL)
                        result = $"{sym} interval '{Value} seconds'";
                    else
                        result = $"{sym}{Value}/86400.0";
                    break;
            }
            return result;
        }
        public static string GetDateAddString(this DbContext connInfo, DATEPART dateType, int Value)
        {
            return connInfo.DbType.GetDateAddString(dateType, Value);
        }
        /// <summary>
        /// 取各种数据库获取当前时间的函数
        /// </summary>
        /// <param name="connType"></param>
        /// <returns></returns>
        public static string GetCurrentTimeFuncName(this DbConnType connType)
        {
            if (connType.IsOracle())
                return "current_date";
            else if (connType == DbConnType.MSSQL)
                return "getdate()";
            else if (connType.IsMySQL())
                return "now()";
            else if (connType == DbConnType.POSTGRESQL)
                return "localtimestamp";
            else if (connType == DbConnType.SQLITE)
                return "datetime('now')";
            else
                return "date()";
        }
        public static string GetCurrentTimeFuncName(this DbContext conn)
        {
            return conn.DbType.GetCurrentTimeFuncName();
        }
        /// <summary>
        /// 获取连字符号
        /// </summary>
        /// <param name="connType"></param>
        /// <returns></returns>
        public static string GetStringConcat(this DbConnType connType, string fieldOrVar1, string fieldOrVar2)
        {
            if (connType.IsOracle()||connType== DbConnType.POSTGRESQL || connType== DbConnType.SQLITE)
                return string.Format("{0} || {1}", fieldOrVar1, fieldOrVar2);
            else if (connType.IsMySQL())
                return string.Format("CONCAT({0},{1})", fieldOrVar1, fieldOrVar2);
            else
                return string.Format("{0} + {1}", fieldOrVar1, fieldOrVar2);
        }
        /// <summary>
        /// @parName  ====>  parName
        /// </summary>
        /// <param name="paraName"></param>
        /// <returns></returns>
        internal static string TrimParaName(this string paraName)
        {
            return paraName.TrimStart(dbParPrefix);
        }
        /// <summary>
        /// 获取数据库参数的前缀
        /// </summary>
        /// <param name="connType"></param>
        /// <returns></returns>
        private static string GetConnTypePrefixChar(this DbConnType connType)
        {
            if (connType.IsOracle() || connType== DbConnType.POSTGRESQL)
                return ":";
            else if (connType.IsMySQL())
                return "@";
            else
                return "@";
        }
        /// <summary>
        /// parName  ====>  @parName
        /// </summary>
        /// <param name="dbConnType"></param>
        /// <param name="paraName"></param>
        /// <returns></returns>
        public static string TreatParaName(this DbConnType dbConnType, string paraName)
        {
            if (dbParPrefix.Contains(paraName[0])) //存在前缀
                return $"{dbConnType.GetConnTypePrefixChar()}{paraName.TrimParaName()}";
            else
                return $"{dbConnType.GetConnTypePrefixChar()}{paraName}";
        }
        /// <summary>
        /// parName  ====>  @parName
        /// </summary>
        /// <param name="dbConnType"></param>
        /// <param name="paraNames"></param>
        /// <returns></returns>
        public static IEnumerable<string> TreatParaNames(this DbConnType dbConnType, params string[] paraNames)
        {
            foreach (string par in paraNames)
            {
                yield return TreatParaName(dbConnType, par);
            }
        }
        /// <summary>
        /// 处理连接串,参数支持Diction<string,object>,DbParameter[] , new{},useBrackets=true则是使用这样的参数 #{Para1},否则就是使用 #Para1#
        /// </summary>
        /// <param name="dbConnType"></param>
        /// <param name="oriSql"></param>
        /// <param name="param"></param>
        /// <returns></returns>
        public static string TransSQL(this DbConnType dbConnType,string oriSql,object param,bool useBrackets = false)
        {
            string sql = oriSql;
            int countofChar = oriSql.Count(c => c == '#');
            if (2 <= countofChar)
            {
                sql = sql.Replace(CURRENT_TIME, GetCurrentTimeFuncName(dbConnType)).Replace(IS_NULL, GetIsNullFuncName(dbConnType));
                if (null!=param )
                {
                    IEnumerable<DbParameter> dbpars = param as IEnumerable<DbParameter>;
                    if (null != dbpars)
                    {
                        foreach (var par in dbpars)
                        {
                            string naturePara = par.ParameterName.TrimParaName();
                            string parNameInstead = "";
                            if (!useBrackets)
                                parNameInstead = string.Format("#{0}#", naturePara);
                            else
                                parNameInstead = "#{" + naturePara + "}";
                            sql = sql.Replace(parNameInstead, dbConnType.TreatParaName(naturePara));
                        }
                    }
                    else
                    {
                        IDictionary<string, object> dic = SmartCrudHelper.AsDictionary(param);
                        foreach (var par in dic)
                        {
                            string parNameInstead = "";
                            if (!useBrackets)
                                parNameInstead = string.Format("#{0}#", par.Key);
                            else
                                parNameInstead = "#{" + par.Key + "}";
                            sql = sql.Replace(parNameInstead, dbConnType.TreatParaName(par.Key));
                        }
                    }
                }
            }
            return sql;
        }
    }
}
