using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SmartCrud
{
    /// <summary>
    /// 封装之后的类型(通用使用)
    /// </summary>
    [Flags]
    public enum DbDataType
    {
        Char = 0,
        NChar,
        Varchar,
        NVarchar,
        Text,
        NText,
        Blob,
        Byte,
        Int16,
        Int,
        Long,
        Float,
        Decimal,
        Double,
        Date,
        DateTime,
        Cursor,
        GUID,
        NONE
    }
}
