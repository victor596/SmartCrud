using System;
using System.Collections.Concurrent;
using System.Collections.Generic;
using System.Data;
using System.Data.Common;
using System.Linq;
using System.Reflection;
using static SmartCrud.SmartCrudHelper;
namespace SmartCrud
{
    /// <summary>
    /// came from: http://weibo.com/zhoufoxcn
    /// </summary>
    public sealed class EntityReader
    {
        //将类型与该类型所有的可写且未被忽略属性之间建立映射
        private readonly static ConcurrentDictionary<string, TableInfo> entityMappings
            = new ConcurrentDictionary<string, TableInfo>();
        private readonly static ConcurrentDictionary<Type, CustomedDbFieldInfo[]> customFieldsMappings
           = new ConcurrentDictionary<Type, CustomedDbFieldInfo[]>();
        /// <summary>
        /// 必须要初始化时增加，因为没有提供锁
        /// </summary>
        /// <param name="type"></param>
        /// <param name="custFieldAttr"></param>
        public static void AppendCustomFieldInfo(Type type, CustomedDbFieldInfo[] custFields)
        {
            customFieldsMappings.TryAdd(type, custFields);
        }
        /// <summary>
        /// 将DataTable中的所有数据转换成List>T<集合
        /// </summary>
        /// <typeparam name="T">DataTable中每条数据可以转换的数据类型</typeparam>
        /// <param name="dataTable">包含有可以转换成数据类型T的数据集合</param>
        /// <returns></returns>
        public static IEnumerable<T> GetEntities<T>(DataTable dataTable) 
        {
            if (dataTable == null) throw new ArgumentNullException(nameof(dataTable));
            //如果T的类型满足以下条件：字符串、ValueType或者是Nullable<ValueType>
            if (typeof(T).IsSimpleType())
            {
                return GetSimpleEntities<T>(dataTable);
            }
            else
            {
                //return dataTable.CreateDataReader().ToCollection<T>();
                return DapperEntityConverter.ToCollection<T>(dataTable.CreateDataReader()); //不能使用using (var reader = dataTable.CreateDataReader())
            }
        }
        /// <summary>
        /// 将DbDataReader中的所有数据转换成List>T<集合
        /// </summary>
        /// <typeparam name="T">DbDataReader中每条数据可以转换的数据类型</typeparam>
        /// <param name="dataTable">包含有可以转换成数据类型T的DbDataReader实例</param>
        /// <returns></returns>
        public static IEnumerable<T> GetEntities<T>(DbDataReader reader)
        {
            if (reader == null) throw new ArgumentNullException(nameof(reader));
            Type t = typeof(T);
            if (t==SmartCrudHelper.String || t.IsValueType)
            {
                return GetSimpleEntities<T>(reader);
            }
            else
            {
                //return reader.ToCollection<T>()?.ToList();
                return DapperEntityConverter.ToCollection<T>(reader);
            }

        }
        /// <summary>
        /// 从DataTable中将每一行的第一列转换成T类型的数据
        /// </summary>
        /// <typeparam name="T">要转换的目标数据类型</typeparam>
        /// <param name="dataTable">包含有可以转换成数据类型T的数据集合</param>
        /// <returns></returns>
        private static List<T> GetSimpleEntities<T>(DataTable dataTable)
        {
            List<T> list = new List<T>();
            foreach (DataRow row in dataTable.Rows)
            {
                list.Add((T)ConvertType.GetValueFromObject(row[0], typeof(T)));
            }
            return list;
        }
        /// <summary>
        /// 从DataTable中读取复杂数据类型集合,如果实体类为BOOL型，会自动把非零值归为true,零值归为false
        /// </summary>
        /// <typeparam name="T">要转换的目标数据类型</typeparam>
        /// <param name="dataTable">包含有可以转换成数据类型T的数据集合</param>
        /// <returns></returns>
        private static List<T> GetComplexEntities<T>(DataTable dataTable) where T:new()
        {
            if (null == dataTable) return null;
            List<T> list = new List<T>();
            Dictionary<string, PropertyInfo> properties = 
                GetEntityMapping(typeof(T)).PropertyMappings;
            T t;
            foreach (DataRow row in dataTable.Rows)
            {
                t = default(T);
                foreach (KeyValuePair<string, PropertyInfo> item in properties)
                {
                    string fieldName = item.Key;
                    if (!dataTable.Columns.Contains(fieldName)) continue;
                    object val = row[fieldName];
                    //如果对应的属性名出现在数据源的列中则获取值并设置给对应的属性
                    if (! IsNullOrDBNull(val  ))
                    {
                        if (item.Value.PropertyType != Bool)
                        {
                            item.Value.SetValue(t, ConvertType.GetValueFromObject(val, item.Value.PropertyType), null);
                        }
                        else
                        {
                            item.Value.SetValue(t, ConvertType.GetValueFromObject(val, Bool), null);
                        }
                    }
                }
                list.Add(t);
            }
            return list;
        }
        /// <summary>
        /// 从DbDataReader的实例中读取复杂的数据类型,如果实体类为BOOL型，会自动把非零值归为true,零值归为false
        /// </summary>
        /// <typeparam name="T">要转换的目标类</typeparam>
        /// <param name="reader">DbDataReader的实例</param>
        /// <returns></returns>
        private static List<T> GetComplexEntities<T>(DbDataReader reader) where T : new()
        {
            List<T> list = new List<T>();
            Dictionary<string, PropertyInfo> properties = GetEntityMapping(typeof(T)).PropertyMappings;
            T t;
            while (reader.Read())
            {
                t = default(T);
                foreach (KeyValuePair<string, PropertyInfo> item in properties)
                {
                    string fieldName = item.Key;
                    int index = -1;
                    try
                    {
                        index = reader.GetOrdinal(fieldName);
                    }
                    catch
                    {
                        continue;
                    }
                    if (index < 0) continue;
                    //如果对应的属性名出现在数据源的列中则获取值并设置给对应的属性
                    if (! reader.IsDBNull(index))
                    {
                        if (item.Value.PropertyType != Bool)
                        {
                            item.Value.SetValue(t,
                                ConvertType.GetValueFromObject(reader[index],
                                item.Value.PropertyType), null);
                        }
                        else
                        {
                            item.Value.SetValue(t,
                                  ConvertType.GetValueFromObject(reader[index], Bool), null);
                        }
                    }
                }
                list.Add(t);
            }
            return list;
        }
        /// <summary>
        /// 从DbDataReader的实例中读取简单数据类型（String,ValueType)
        /// </summary>
        /// <typeparam name="T">目标数据类型</typeparam>
        /// <param name="reader">DbDataReader的实例</param>
        /// <returns></returns>
        private static List<T> GetSimpleEntities<T>(DbDataReader reader)
        {
            List<T> list = new List<T>();
            while (reader.Read())
            {
                list.Add((T)ConvertType.GetValueFromObject(reader[0], typeof(T)));
            }
            return list;
        }
        /// <summary>
        /// 获取该类型中属性与数据库字段的对应关系映射
        /// </summary>
        /// <param name="type"></param>
        /// <param name="ignorAutoIncreField">是否排除设置了SQLFieldAutoIncrement=True的字段</param>
        /// <returns></returns>
        private static TableInfo GenerateTypeMapping(Type type, bool ignoreAutoIncreField = false)
        {
            TableInfo result = new TableInfo();
            //获取表名
            TableNameAttribute sqlTableNameAttr =
               Attribute.GetCustomAttribute(type, typeof(TableNameAttribute)) as TableNameAttribute;
            if (null != sqlTableNameAttr)
                result.TableName = sqlTableNameAttr.TableName;
            else
                result.TableName = type.Name;
            CustomedDbFieldInfo[] customFields = null;
            bool hasCustomFields = false;
            if (customFieldsMappings.TryGetValue(type, out customFields)) hasCustomFields = true;
            List<KeyValue<uint,string>> mapList = new List<KeyValue<uint, string>>();//主键定义
            PropertyInfo[] properties = type.GetProperties(BindingFlag);
            foreach (PropertyInfo p in properties)
            {
                var custField = !hasCustomFields?null: customFields.FirstOrDefault(c =>
                    c.FieldName.Equals(p.Name, StringComparison.OrdinalIgnoreCase));
                //判断是否是主键
                KeyAttribute pkDefine = Attribute.GetCustomAttribute(p, typeof(KeyAttribute)) as KeyAttribute;
                if (null != pkDefine)
                {
                    mapList.Add(new KeyValue<uint, string>(pkDefine.Order, p.Name));
                }
                else if (null != custField && custField.PrimaryKeyIndex >= 0)
                {
                    mapList.Add(new KeyValue<uint, string>((uint)custField.PrimaryKeyIndex, p.Name));
                }
                //判断是否排除
                ExclusivedAttribute exclusivedAttr=  Attribute.GetCustomAttribute(p, typeof(ExclusivedAttribute) ) as ExclusivedAttribute;
                if (null == pkDefine && null != exclusivedAttr)
                {
                    continue;
                }
                else if (null != custField && custField.Exclusived)
                {
                    continue;
                }
                //判断是否是自动增长列
                AutoIncrementAttribute autoFieldDefine = Attribute.GetCustomAttribute(p, typeof(AutoIncrementAttribute)) as AutoIncrementAttribute;
                if (null != autoFieldDefine)
                {
                    if (null == pkDefine && ignoreAutoIncreField) continue;
                    result.AutoIncrementFields.Add(p.Name);
                }
                else if (null != custField && custField.AutoIncrement)
                {
                    result.AutoIncrementFields.Add(p.Name);
                }
                //currenttime
                DbNowAttribute currentTimeDefine = Attribute.GetCustomAttribute(p, typeof(DbNowAttribute)) as DbNowAttribute;
                if (null == pkDefine && null != currentTimeDefine)
                {
                    result.CurrentTimeFields.Add(p.Name);
                }
                else if (null != custField && custField.CurrentTime)
                {
                    result.CurrentTimeFields.Add(p.Name);
                }
                //如果该属性是可读并且未被忽略的，则有可能在实例化该属性对应的类时用得上
                if (p.CanWrite)
                {
                    result.PropertyMappings.Add(p.Name, p);
                }
            }
            if (0 < mapList.Count)
            {
                mapList.Sort(delegate(KeyValue<uint, string> a, KeyValue<uint, string> b)
                {
                    return a.Key.CompareTo(b.Key);
                });
                string[] PKeys = new string[mapList.Count];
                int i = 0;
                foreach (KeyValue<uint, string> ele in mapList)
                {
                    PKeys[i] = ele.Value;
                    ++i;
                }
                result.PKeys.Clear();
                result.PKeys.AddRange(PKeys);
            }
            return result;
        }
        public static TableInfo GetEntityMapping(Type type,bool ignoreAutoIncreField=false)
        {
            if (null == type || !type.IsClass)
            {
                throw new ArgumentException(nameof(type));
            }
            string key = type.FullName + "_" + ignoreAutoIncreField.ToString();
            TableInfo entityMapping = null;
            if (entityMappings.TryGetValue(key, out entityMapping)) return entityMapping;
            entityMapping = GenerateTypeMapping(type, ignoreAutoIncreField);
            if (null != entityMapping)
            {
                entityMappings.TryAdd(key, entityMapping);
            }
            return entityMapping;
        }
        public static Dictionary<string, PropertyInfo> GetTypePropertyMapping(Type type, bool ignoreAutoIncreField = false)
        {
            TableInfo result = GetEntityMapping(type, false);
            return result?.PropertyMappings ;
        }
        public static TableInfo GetEntityMapping(Type type)
        {
            return GetEntityMapping(type, false);
        }
        public static TableInfo GetEntityMapping<T>() where T : class
        {
            return GetEntityMapping(typeof(T));
        }
    }
}