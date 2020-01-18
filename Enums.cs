using System;
namespace SmartCrud
{
    [Flags]
    public enum DbConnType
    {
        MSSQL = 0,
        ORACLE,
        ODPNET,
        SQLITE,
        MYSQL,
        POSTGRESQL
    }
    public enum DATATYPE
    {
        STRING = 0,
        NUMBER = 1,
        DATE = 2
    }
    public enum DATEPART
    {
        DAY,
        HOUR,
        MINUTE,
        SECOND,
        MS
    }
    public enum DML
    {
        SELECT = 0,
        INSERT,
        UPDATE,
        DELETE
    }
    public enum BILLLOCKRESULT
    {
        ALREADY_LOCKED = 0,
        SUCCESS_LOCK = 1,
        SUCCESS_UNLOCK = 2,
        FAIL_LOCK = 3
    }
    public enum BILLFORMAT2
    {
        NONE = 0,
        YYYYMM = 1,
        YYMM = 2,
        YYYYMMDD = 3,
        YYMMDD = 4,
        YY = 5,
        BIT36 = 6, //36进制
        BIT16 = 7, //16进制
    }
}
