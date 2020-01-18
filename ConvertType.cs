using System;
namespace SmartCrud
{
    public static class ConvertType
    {
        public static string GetString(object value)
        {
            return Convert.ToString(value);
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <param name="targetType"></param>
        /// <returns></returns>
        public static object GetEnum(object value, Type targetType)
        {
            return Enum.Parse(targetType, value.ToString());
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static bool GetBoolean(object value)
        {
            if (value is Boolean o)
            {
                return o;
            }
            else
            {
                int intValue = (int)GetInt32(value);
                if (intValue == 0)
                {
                    return false;
                }
                else
                {
                    return true;
                }
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static byte GetByte(object value)
        {
            if (value is Byte o)
            {
                return o;
            }
            else
            {
                return byte.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static SByte GetSByte(object value)
        {
            if (value is SByte o)
            {
                return o;
            }
            else
            {
                return SByte.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static Char GetChar(object value)
        {
            if (value is Char o)
            {
                return o;
            }
            else
            {
                return Char.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static Guid GetGuid(object value)
        {
            if (value is Guid o)
            {
                return o;
            }
            else
            {
                return new Guid(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static Int16 GetInt16(object value)
        {
            if (value is Int16 o)
            {
                return o;
            }
            else
            {
                return Int16.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static UInt16 GetUInt16(object value)
        {
            if (value is UInt16 o)
            {
                return o;
            }
            else
            {
                return UInt16.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static Int32 GetInt32(object value)
        {
            if (value is Int32 o)
            {
                return o;
            }
            else
            {
                return Int32.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetUInt32(object value)
        {
            if (value is UInt32 o)
            {
                return o;
            }
            else
            {
                return UInt32.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetInt64(object value)
        {
            if (value is Int64 o)
            {
                return o;
            }
            else
            {
                return Int64.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetUInt64(object value)
        {
            if (value is UInt64 o)
            {
                return o;
            }
            else
            {
                return UInt64.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetSingle(object value)
        {
            if (value is Single o)
            {
                return o;
            }
            else
            {
                return Single.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetDouble(object value)
        {
            if (value is Double o)
            {
                return o;
            }
            else
            {
                return Double.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetDecimal(object value)
        {
            if (value is Decimal o)
            {
                return o;
            }
            else
            {
                return Decimal.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetDateTime(object value)
        {
            if (value is DateTime o)
            {
                return o;
            }
            else
            {
                return DateTime.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static object GetTimeSpan(object value)
        {
            if (value is TimeSpan o)
            {
                return o;
            }
            else
            {
                return TimeSpan.Parse(value.ToString());
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定枚举类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <returns></returns>
        public static byte[] GetBinary(object value)
        {
            //如果该字段为NULL则返回null
            if (value == DBNull.Value)
            {
                return null;
            }
            else if (value is Byte[] o)
            {
                return o;
            }
            else
            {
                return null;
            }
        }
        /// <summary>
        /// 将Object转换成字符串类型
        /// </summary>
        /// <param name="value">object类型的实例</param>
        /// <returns></returns>
        /// <summary>
        /// 将Object类型数据转换成对应的可空数值类型表示
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <param name="targetType">可空数值类型</param>
        /// <returns></returns>
        public static object GetGenericValueFromObject(object value, Type targetType)
        {
            if (value == DBNull.Value)
            {
                return null;
            }
            else
            {
                //获取可空数值类型对应的基本数值类型，如int?->int,long?->long
                //Type nonGenericType = genericTypeMappings[targetType];
                Type nonGenericType = targetType.GetGenericArguments()[0];
                return GetNonGenericValueFromObject(value, nonGenericType);
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <param name="targetType">目标对象的类型</param>
        /// <returns></returns>
        public static object GetNonGenericValueFromObject(object value, Type targetType)
        {
            if (targetType.IsEnum)//因为
            {
                return ConvertType.GetEnum(value, targetType);
            }
            else
            {
                switch (targetType.Name)
                {
                    case "Byte": return ConvertType.GetByte(value);
                    case "SByte": return ConvertType.GetSByte(value);
                    case "Char": return ConvertType.GetChar(value);
                    case "Boolean": return ConvertType.GetBoolean(value);
                    case "Guid": return ConvertType.GetGuid(value);
                    case "Int16": return ConvertType.GetInt16(value);
                    case "UInt16": return ConvertType.GetUInt16(value);
                    case "Int32": return ConvertType.GetInt32(value);
                    case "UInt32": return ConvertType.GetUInt32(value);
                    case "Int64": return ConvertType.GetInt64(value);
                    case "UInt64": return ConvertType.GetUInt64(value);
                    case "Single": return ConvertType.GetSingle(value);
                    case "Double": return ConvertType.GetDouble(value);
                    case "Decimal": return ConvertType.GetDecimal(value);
                    case "DateTime": return ConvertType.GetDateTime(value);
                    case "TimeSpan": return ConvertType.GetTimeSpan(value);
                    default: return null;
                }
            }
        }
        /// <summary>
        /// 将指定的 Object 的值转换为指定类型的值。
        /// </summary>
        /// <param name="value">实现 IConvertible 接口的 Object，或者为 null</param>
        /// <param name="targetType">要转换的目标数据类型</param>
        /// <returns></returns>
        public static object GetValueFromObject(object value, Type targetType)
        {
            if (targetType == typeof(string))//如果要将value转换成string类型
            {
                return ConvertType.GetString(value);
            }
            else if (targetType == typeof(byte[]))//如果要将value转换成byte[]类型
            {
                return ConvertType.GetBinary(value);
            }
            else if (targetType.IsGenericType)//如果目标类型是泛型
            {
                return GetGenericValueFromObject(value, targetType);
            }
            else//如果是基本数据类型（包括数值类型、枚举和Guid）
            {
                return GetNonGenericValueFromObject(value, targetType);
            }
        }
    }
}
