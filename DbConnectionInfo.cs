using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SmartCrud
{
    public class DbConnectInfo
    {
        public string Datasource { get; set; }
        public string Database { get; set; }
        public string UserId { get; set; }
        public string Password { get; set; }
        /// <summary>
        /// MSSQL,MYSQL,ODPNET,ORACLE,SQLITE,POSTGRESQL
        /// </summary>
        public string ConnType { get; set; }
        public string ToDbConnectionString()
        {
            if (this.ConnType.Equals("ORACLE", StringComparison.OrdinalIgnoreCase))
            {
                string ds = this.Datasource;
                int index = ds.IndexOf("UNICODE", StringComparison.OrdinalIgnoreCase);
                if (-1 == index)
                {
                    if (!ds.EndsWith(";", StringComparison.OrdinalIgnoreCase)) ds += ";";
                    ds += "UNICODE=true";
                }
                return $"Data Source={ds};User Id={this.UserId};Password={this.Password}";
            }
            else if (this.ConnType.Equals("ODPNET", StringComparison.OrdinalIgnoreCase))
                return $"Data Source={this.Datasource};User Id={this.UserId};Password={this.Password}";
            else if (this.ConnType.Equals("MYSQL", StringComparison.OrdinalIgnoreCase))
                return $"Server={this.Datasource};Database={this.Database};User Id={this.UserId};Password={this.Password}";
            else if (this.ConnType.Equals("POSTGRESQL", StringComparison.OrdinalIgnoreCase))
                return $"Server={Datasource};Database={this.Database};uid={this.UserId};pwd={this.Password}";
            else if (this.ConnType.Equals("SQLITE", StringComparison.OrdinalIgnoreCase))
                return $"Data Source={this.Datasource}";
            else if (this.ConnType.Equals("MSSQL", StringComparison.OrdinalIgnoreCase))
                return $"Data Source={this.Datasource};Initial Catalog={this.Database};User Id={this.UserId};Password={this.Password}";
            else
                throw new NotSupportedException(this.ConnType);
        }
    }
}
