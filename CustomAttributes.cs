using System;
using System.Collections.Generic;
using System.Linq;
using System.Reflection;
using System.Text;
namespace SmartCrud
{
    public class CustomedDbFieldInfo
    {
        public string FieldName { get; set; }
        /// <summary>
        ///-1非主键
        /// </summary>
        public int PrimaryKeyIndex { get; set; } = -1;
        /// <summary>
        /// 排除的属性
        /// </summary>
        public bool Exclusived { get; set; } = false;
        /// <summary>
        /// 自增长
        /// </summary>
        public bool AutoIncrement { get; set; } = false;
        /// <summary>
        /// 更新时自动设置为当前时间
        /// </summary>
        public bool CurrentTime { get; set; } = false;
        /// <summary>
        /// 实体属性定义
        /// </summary>
        /// <param name="name">属性名称</param>
        /// <param name="pkIndex">主键序列,如果定义主键则需要大于或等于0 </param>
        /// <param name="exclude"></param>
        /// <param name="autoIncr"></param>
        /// <param name="currTime"></param>
        public CustomedDbFieldInfo(string name, int pkIndex = -1, bool exclude = false, bool autoIncr = false, bool currTime = false)
        {
            this.FieldName = name;
            this.PrimaryKeyIndex = pkIndex;
            this.Exclusived = exclude;
            this.AutoIncrement = autoIncr;
            this.CurrentTime = currTime;
        }
    }
    /// <summary>
    /// 实体资料
    /// </summary>
    [Serializable]
    public class TableInfo
    {
        /// <summary>
        /// 对应的SQL表名称
        /// </summary>
        public string TableName { get; set; }
        /// <summary>
        /// 主键集合
        /// </summary>
        public List<string> PKeys { get; private set; }
        /// <summary>
        /// 属于主键成员
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public bool IsKey(string fieldName)
        {
            return (null != PKeys && PKeys.Any(c =>
                  c.Equals(fieldName, StringComparison.OrdinalIgnoreCase)));
        }
        /// <summary>
        /// 自动增长列集合
        /// </summary>
        public List<string> AutoIncrementFields { get; private set; }
        /// <summary>
        /// 是否自增长列成员
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public bool IsAutoIncField(string fieldName)
        {
            return (null != AutoIncrementFields && AutoIncrementFields.Any(c =>
                c.Equals(fieldName, StringComparison.OrdinalIgnoreCase)));
        }
        /// <summary>
        /// 新增或修改时此字段自动设置为数据库当前时间
        /// </summary>
        public List<string> CurrentTimeFields { get; private set; }
        /// <summary>
        /// 是否current_time列成员
        /// </summary>
        /// <param name="fieldName"></param>
        /// <returns></returns>
        public bool IsCurrentTimeField(string fieldName)
        {
            return (null != CurrentTimeFields && CurrentTimeFields.Any(c =>
                c.Equals(fieldName, StringComparison.OrdinalIgnoreCase)));
        }
        public Dictionary<string, PropertyInfo> PropertyMappings { get; private set; }
        public override string ToString()
        {
            return this.TableName;
        }
        public string GetText()
        {
            StringBuilder strbld = new StringBuilder();
            strbld.AppendLine(string.Format("TableName:{0}", TableName));
            strbld.AppendLine(string.Format("Keys:{0}",
                null == PKeys ? string.Empty : string.Join(",", PKeys)));
            strbld.AppendLine(string.Format("AutoIncrementFields:{0}",
                null == AutoIncrementFields ? string.Empty : string.Join(",", AutoIncrementFields)));
            strbld.AppendLine(string.Format("CurrentTimeFields:{0}",
                null == CurrentTimeFields ? string.Empty : string.Join(",", CurrentTimeFields)));
            strbld.AppendLine(string.Format("Fields:{0}", string.Join(",",
                (from KeyValuePair<string, PropertyInfo> ele in PropertyMappings select ele.Key))));
            return strbld.ToString();
        }
        public TableInfo()
        {
            PropertyMappings = new Dictionary<string, PropertyInfo>();
            PKeys = new List<string>();
            AutoIncrementFields = new List<string>();
            CurrentTimeFields = new List<string>();
        }
    }
    /// <summary>
    /// 定义属性对应的数据库字段类型
    /// </summary>
    public class DbFieldTypeAttribute : Attribute
    {
        public DbDataType DbType { get; set; }
        /// <summary>
        /// 只有字符串类型才能使用此属性默认为0
        /// </summary>
        public int Size { get; set; }
        public DbFieldTypeAttribute(DbDataType dbType)
        {
            if (dbType == DbDataType.NONE)
            {
                throw new ArgumentException("dbType");
            }
            this.DbType = dbType;
            this.Size = 0;
        }
        public DbFieldTypeAttribute(DbDataType dbType, int size)
        {
            if (dbType == DbDataType.NONE)
            {
                throw new ArgumentException("dbType");
            }
            this.DbType = dbType;
            if (0 > size)
            {
                throw new ArgumentException("size");
            }
            this.Size = size;
            bool isStrType = SmartCrudHelper.IsStringType(dbType);
            if (0 == this.Size)
            {
                throw new ArgumentException("size");
            }
        }
    }
    /// <summary>
    /// 主键定义,索引从0开始
    /// </summary>
    public class KeyAttribute : Attribute
    {
        public uint Order { get; set; }
        public KeyAttribute() : this(0) { }
        public KeyAttribute(uint order)
        {
            if (0 > order) throw new ArgumentException("order");
            this.Order = order;
        }
    }
    /// <summary>
    /// 类型映射的SQL表格,索引从0开始
    /// </summary>
    public class TableNameAttribute : Attribute
    {
        public string TableName { get; set; }
        public TableNameAttribute(string tableName)
        {
            this.TableName = tableName;
        }
    }
    /// <summary>
    /// 实体类忽略的属性,根据实体类的Type获取字段时忽略这些类
    /// </summary>
    public class ExclusivedAttribute : Attribute
    {
        public ExclusivedAttribute() : base()
        {
        }
    }
    /// <summary>
    /// 新增/修改时自动替换为数据库当前时间的函数
    /// </summary>
    public class DbNowAttribute : Attribute
    {
        public DbNowAttribute() : base()
        {
        }
    }
    /// <summary>
    /// 自动增长列属性
    /// </summary>
    public class AutoIncrementAttribute : Attribute
    {
        public AutoIncrementAttribute() : base()
        {
        }
    }
}
