using SmartCrud;
using System;
using System.Collections.Concurrent;
using System.Data.Common;
namespace SmartCrud
{
    internal static class DatabaseProvider
    {
        readonly static ConcurrentDictionary<string, DbProviderFactory> dic = new ConcurrentDictionary<string, DbProviderFactory>();
        /// <summary>
        ///     Returns the .net standard conforming DbProviderFactory.
        /// </summary>
        /// <param name="assemblyQualifiedNames">The assembly qualified name of the provider factory.</param>
        /// <returns>The db provider factory.</returns>
        /// <exception cref="ArgumentException">Thrown when <paramref name="assemblyQualifiedNames" /> does not match a type.</exception>
        private static DbProviderFactory GetFactory(string assemblyQualifiedNames)
        {
            Type ft = SmartCrudHelper.GetType(assemblyQualifiedNames);
            if (ft == null)
                throw new ArgumentException($"Could not load the {assemblyQualifiedNames} DbProviderFactory.");
            return (DbProviderFactory)ft.GetField("Instance").GetValue(null);
            
        }
        public static DbProviderFactory GetFactory(DbConnType dbConnType)
        {
            DbProviderFactory result = null;
            string str = dbConnType.ToString();
            if (dic.TryGetValue(str, out result)) return result;
            result = GetFactoryDirect(dbConnType);
            dic.TryAdd(str, result);
            return result;
        }
        private static DbProviderFactory GetFactoryDirect(DbConnType dbConnType)
        {
            if (dbConnType == DbConnType.MSSQL) //ok
                return System.Data.SqlClient.SqlClientFactory.Instance; //ok
            else if (dbConnType == DbConnType.MYSQL) //ok
                return GetFactory("MySql.Data.MySqlClient.MySqlClientFactory,MySql.Data"); //ok
               //return GetFactory("MySql.Data.MySqlClient.MySqlClientFactory,MySqlConnector");
            else if (dbConnType == DbConnType.ODPNET) //ok
                return GetFactory("Oracle.ManagedDataAccess.Client.OracleClientFactory, Oracle.ManagedDataAccess");
            else if (dbConnType == DbConnType.ORACLE) //ok
                return GetFactory("System.Data.OracleClient.OracleClientFactory,System.Data.OracleClient");
            else if (dbConnType == DbConnType.SQLITE) // ok
                return GetFactory("System.Data.SQLite.SQLiteFactory, System.Data.SQLite");
            else if (dbConnType == DbConnType.POSTGRESQL) //ok
                return GetFactory("Npgsql.NpgsqlFactory,Npgsql");
            else
                throw new NotSupportedException(dbConnType.ToString());
        }
    }
}
