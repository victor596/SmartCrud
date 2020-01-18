using System;
using System.Collections.Generic;
using System.IO;
using System.Linq.Expressions;
using System.Reflection;
using System.Runtime.CompilerServices;
using System.Text;
using System.Linq;
namespace SmartCrud
{
    public static class SmartCrudHelper
    {
        public readonly static string AppDir ;
        public readonly static char[] Space , Comma ;
        public const BindingFlags BindingFlag = BindingFlags.Public | BindingFlags.NonPublic | BindingFlags.Instance;
        public readonly static Type GuidType;
        public readonly static Type Bool;
        public readonly static Type String;
        public readonly static Type SByte;
        public readonly static Type ConvertibleType;
        public readonly static Type TaskType;
        public readonly static DateTime StartTime ;
        static SmartCrudHelper()
        {
            GuidType = typeof(Guid);
            Bool = typeof(Boolean);
            String = typeof(string);
            SByte = typeof(SByte);
            ConvertibleType = typeof(IConvertible);
            TaskType = typeof(System.Threading.Tasks.Task);
            AppDir = AppDomain.CurrentDomain.BaseDirectory;
            Space = new char[] { ' ' };
            Comma = new char[] { ',' };
            StartTime = new DateTime(1970, 1, 1, 0, 0, 0, DateTimeKind.Utc);
        }
        public static bool IsStringType(DbDataType dataType)
        {
            return dataType == DbDataType.NChar
                || dataType == DbDataType.Char
                || dataType == DbDataType.Varchar
                || dataType == DbDataType.NVarchar;
        }
        public static bool IsNumber(this Type DataType)
        {
            return IsFloat(DataType) || IsInt(DataType);
        }
        public static bool IsFloat(this Type DataType)
        {
            return DataType == typeof(double) ||
                       DataType == typeof(Single) ||
                       DataType == typeof(float) ||
                       DataType == typeof(decimal);
        }
        public static bool IsInt(this Type DataType)
        {
            return DataType == typeof(int) ||
                       DataType == typeof(Int16) ||
                       DataType == typeof(Int32) ||
                       DataType == typeof(Int64) ||
                       DataType == typeof(byte) ||
                       DataType == typeof(long);
        }
        public static bool IsDate(this Type DataType)
        {
            return DataType == typeof(DateTime);
        }
        public static bool IsString(this Type DataType)
        {
            return DataType == String || DataType == typeof(char) || DataType == typeof(Char);
        }
        public static DATATYPE GetDataType(this Type DataType)
        {
            DATATYPE ret = DATATYPE.STRING;
            if (IsNumber(DataType))
                ret = DATATYPE.NUMBER;
            else if (IsDate(DataType))
                ret = DATATYPE.DATE;
            else
                ret = DATATYPE.STRING;
            return ret;
        }
        public static IEnumerable<string> GetDataTableColumns(this System.Data.DataTable dt)
        {
            if (0==(dt?.Columns.Count??0)) yield break;
            foreach (System.Data.DataColumn dc in dt.Columns)
                yield return dc.ColumnName;
        }
        /// <summary>
        /// 取底层类型 datetime? ==> datetime
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static Type GetInnerType(this Type t)
        {
            Type[] ts = t.GetGenericArguments();
            if (null == ts || 0 == ts.Length) return t;
            return ts[0];
        }
        /// <summary>
        /// 连接字符串
        /// </summary>
        /// <param name="strbld"></param>
        /// <param name="para"></param>
        /// <returns></returns>
        public static StringBuilder Concat(this StringBuilder strbld, params string[] para)
        {
            if (null != para && 0 < para.Length)
            {
                foreach (string str in para) strbld.Append(str);
            }
            return strbld;
        }
        public static int GetIntValue(this object obj)
        {
            if(IsNullOrDBNull(obj)) return 0;
            int Ret = 0;
            if (!(int.TryParse(obj.ToString(), out Ret))) return 0;
            return Ret;
        }
        public static bool IsTrue(this string str)
        {
            return !string.IsNullOrEmpty(str) &&
                ("Y".Equals(str, StringComparison.OrdinalIgnoreCase)
                || "YES".Equals(str, StringComparison.OrdinalIgnoreCase)
                || "1".Equals(str, StringComparison.OrdinalIgnoreCase)
                || "true".Equals(str, StringComparison.OrdinalIgnoreCase));
        }
        public static string SplitTypeAndAssemble(string strTypeAndAssemble, out string assembleName)
        {
            string className = string.Empty;
            assembleName = string.Empty;
            string[] arr = System.Text.RegularExpressions.Regex.Split(strTypeAndAssemble, "]],");//带泛型
            int len = (arr?.Length ?? 0);
            if (len > 2) throw new ArgumentException(strTypeAndAssemble);
            if (len == 2) //有泛型应用集 ConfigWithGenericsDemo.IRepository`1[[System.String, mscorlib]], ConfigWithGenericsDemo
            {
                className = arr[0].TrimStart(Space).TrimEnd(Space) + "]]";
                assembleName = arr[1].TrimStart(Space).TrimEnd(Space);
            }
            else if (len == 1) //FireWolf.DotNetty.IMessageCoding, FireWolf.DotNetty
            {
                string[] arrSub = arr[0].Split(Comma, 2, StringSplitOptions.RemoveEmptyEntries);
                if ((arrSub?.Length ?? 0) == 1)
                    className = arrSub[0];
                else
                {
                    className = arrSub[0].TrimStart(Space).TrimEnd(Space);
                    assembleName = arrSub[1].TrimStart(Space).TrimEnd(Space);
                }
            }
            return className;
        }
        public static Type GetType(string typeDesc)
        {
            string assembleName = string.Empty;
            string className = SplitTypeAndAssemble(typeDesc, out assembleName);
            bool useFile = false;
#if NETCOREAPP
            useFile = false;
#endif 
            Assembly ass = null;
            if (assembleName.IndexOf(",") > 0 && !useFile)
                ass = Assembly.Load(assembleName); //.net core不知为啥出不来
            else
            {
                string fileName = Path.Combine(AppDir, assembleName + ".dll");
                if (!File.Exists(fileName))
                    fileName = Path.Combine(AppDir, assembleName + ".exe");
                ass = Assembly.LoadFrom(fileName);
            }
            return ass.GetType(className, true, true);
        }
        public static T GetEnumItemByValue<T>(int Value)
        {
            if (!(typeof(T).IsEnum)) throw new Exception("Not enum !");
            return (T)Enum.ToObject(typeof(T), Value);
        }
        public static T GetEnumByKey<T>(string Key)
        {
            if (!typeof(T).IsEnum) throw new Exception("Not enum!");
            return (T)Enum.Parse(typeof(T), Key);
        }
        public static IEnumerable<string> GetPropertyNames<T>(this Expression<Func<T, object>> expr)
        {
            if (null == expr) yield break;
            if (expr.Body is NewArrayExpression arr)
            {
                foreach (Expression exp in arr.Expressions)
                {
                    if (exp is UnaryExpression o)
                    {
                        yield return ((MemberExpression)o.Operand).Member.Name;
                    }
                    else if (exp is MemberExpression o1)
                    {
                        yield return o1.Member.Name;
                    }
                    else if (exp is ParameterExpression o2)
                    {
                        yield return o2.Type.Name;
                    }
                }
            }
            else
            {
                string result = GetPropertyName<T>(expr);
                yield return result;
            }
        }
        public static string GetPropertyName<T>(this Expression<Func<T, object>> expr)
        {
            var rtn = "";
            if (expr.Body is UnaryExpression)
            {
                rtn = ((MemberExpression)((UnaryExpression)expr.Body).Operand).Member.Name;
            }
            else if (expr.Body is MemberExpression)
            {
                rtn = ((MemberExpression)expr.Body).Member.Name;
            }
            else if (expr.Body is ParameterExpression)
            {
                rtn = ((ParameterExpression)expr.Body).Type.Name;
            }
            return rtn;
        }
        /// <summary>
        /// 仅支持Dictionary<string,object>和匿名类
        /// </summary>
        /// <param name="param"></param>
        /// <returns></returns>
        public static IDictionary<string, object> AsDictionary(object param)
        {
            IDictionary<string, object> result = param as IDictionary<string, object>;
            if (null == result)
            {
                result = GetDictionaryValues(param);
            }
            return result;
        }
        /// <summary>
        /// 对匿名类获取值
        /// </summary>
        /// <param name="obj"></param>
        /// <returns></returns>
        public static IDictionary<string, object> GetDictionaryValues(object obj)
        {
            if (null == obj) return null;
            Type t = obj.GetType();
            PropertyInfo[] properties = null;
            bool isAnonymousType = IsAnonymousType(t);
            if (isAnonymousType)
                properties = t.GetProperties(BindingFlag);
            else
                properties = t.GetProperties();
            if (null == properties || 0 == properties.Length) return null;
            IDictionary<string, object> result = new Dictionary<string, object>();
            foreach (PropertyInfo pi in properties)
            {
                result.Add(pi.Name, pi.GetValue(obj, null));
            }
            return result;
        }
        public static bool IsAnonymousType(this Type type)
        {
            if (type == null) throw new ArgumentNullException("type");
            return Attribute.IsDefined(type, typeof(CompilerGeneratedAttribute), false)
                && type.IsGenericType && type.Name.Contains("AnonymousType")
                && (type.Name.StartsWith("<>") || type.Name.StartsWith("VB$"))
                && (type.Attributes & TypeAttributes.NotPublic) == TypeAttributes.NotPublic;
        }
        public static bool IsNullOrDBNull(object obj)
        {
            return (null == obj || DBNull.Value == obj);
        }
        /// <summary>
        /// 转换异常方法 result传入时不可为空
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="exp"></param>
        /// <param name="result"></param>
        /// <param name="errCode"></param>
        /// <returns></returns>
        public static T ExpToResult<T>(this Exception exp, T result, int errCode = -1)
            where T : IResultBase, new()
        {
            if (null == result) throw new ArgumentNullException(nameof(result));
            ClassedException ce = exp as ClassedException;
            if (null != ce)
            {
                result.errorCode = ce.ErrorCode;
                result.errorMessage = ce.Message;
                return result;
            }
            result.errorCode = errCode;
            result.errorMessage = exp.Message;
            return result;
        }
        /// <summary>
        /// 是否是简单的数据类string,byte[],ValueType
        /// </summary>
        /// <param name="t"></param>
        /// <returns></returns>
        public static bool IsSimpleType(this Type t)
        {
            return t == String || t == typeof(byte[]) || t.IsValueType || t.IsEnum;
        }
    }
}
