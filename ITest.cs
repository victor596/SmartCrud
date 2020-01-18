using System;
namespace SmartCrud
{
    public interface ITest
    {
        DateTime GetTime(DbConnType connType, string ConnectionString);
    }
}
