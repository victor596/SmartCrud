using System;
namespace SmartCrud
{
    public class Test: ITest
    {
        public DateTime GetTime(DbConnType connType, string ConnectionString)
        {
            using (DbContext conn = new DbContext(ConnectionString,connType))
            {
                return conn.Now();
            }
        }
    }
}
