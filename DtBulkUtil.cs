using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Data;
using static SmartCrud.SmartCrudHelper;
namespace SmartCrud
{
    /*
     ODP.NET 不要传GUID,BIT,BOOL,SBYTE类型的数据
     */
    public static class DtBulkUtil
    {
        /// <summary>
        /// 是否存在相应的主键
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="TableName"></param>
        /// <param name="dr"></param>
        /// <param name="keyFields"></param>
        /// <param name="pars"></param>
        /// <param name="ConnType"></param>
        /// <returns></returns>
        private static bool ExistsDataRow(DbContext connInfo,string TableName, DataRow dr, string[] keyFields, RequestBase pars)
        {
            StringBuilder strbld = new StringBuilder(256);
            strbld.AppendFormat("select 'Y' From {0} where ", TableName);
            int count = keyFields.Length;
            //pars.Clear();
            for (int i = 0; i < count; i++)
            {
                if (0 != i) strbld.Append(" and ");
                string fieldName = keyFields[i];
                object obj = null;
                strbld.AppendFormat(" {0}={1} ", fieldName, connInfo.TreatParaName(fieldName));
                obj = GetDataFieldValue(dr[fieldName], connInfo.DbType);
                pars.SetValue(fieldName, obj);
            }
            return connInfo.ExistsPrimitive(strbld.ToString(), pars);
        }
        private static object GetDataFieldValue(object obj, DbConnType connType)
        {
            if (SmartCrudHelper.IsNullOrDBNull(obj))
            {
                return null; // DBNull.Value;  //dapper下不可使用 DBNull.Value
            }
            else
            {
                if (connType == DbConnType.ODPNET)
                {
                    Type t = obj.GetType();
                    if (t == GuidType)
                    {
                        return obj?.ToString().ToUpper();
                    }
                    else if (t == Bool)
                    {
                        return (byte)(((bool)obj) ? 1 : 0);
                    }
                    else if (t == SmartCrudHelper.SByte)
                    {
                        return Convert.ToByte(obj);
                    }
                }
                return obj;
            }
        }
        /// <summary>
        /// 更新行
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="TableName"></param>
        /// <param name="dr"></param>
        /// <param name="pars"></param>
        /// <param name="ConnType"></param>
        /// <param name="Fields"></param>
        /// <param name="sqlUpdate"></param>
        /// <returns></returns>
        private static bool UpdateDataRow(DbContext connInfo, string TableName,DataRow dr, RequestBase pars, string[] Fields, string[] PkFields, string sqlUpdate)
        {
            for (int i = 0, count = Fields.Length; i < count; i++)
            {
                object obj = null;
                string fieldName = Fields[i];
                if (PkFields.Contains(fieldName,StringComparer.OrdinalIgnoreCase)) continue;
                obj = GetDataFieldValue(dr[fieldName], connInfo.DbType);
                pars.SetValue(fieldName, obj);
            }
            for (int i = 0, count = PkFields.Length; i < count; i++)
            {
                object obj = null;
                string fieldName = PkFields[i];
                obj = GetDataFieldValue(dr[fieldName], connInfo.DbType);
                pars.SetValue(fieldName, obj);
            }
            return 1 <= connInfo.ExecuteNonQuery(sqlUpdate, pars);
        }
        /// <summary>
        /// 删除行
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="dr"></param>
        /// <param name="Keys"></param>
        /// <param name="Pars"></param>
        /// <param name="ConnType"></param>
        /// <param name="Sqldelete"></param>
        /// <returns></returns>
        private static bool DeleteDataRow(DbContext connInfo, DataRow dr,string[] Keys, RequestBase Pars, string Sqldelete)
        {
            //Pars.Clear();
            for (int i = 0, count = Keys.Length; i < count; i++)
            {
                string fieldName = Keys[i];
                object obj = null;
                obj = GetDataFieldValue(dr[fieldName, DataRowVersion.Original], connInfo.DbType);
                Pars.SetValue(fieldName, obj);
            }
            return (1 <= connInfo.ExecuteNonQuery(Sqldelete, Pars));
        }
        private static bool DeleteDataRow2(DbContext connInfo, DataRow dr, string[] Keys, RequestBase Pars, string Sqldelete)
        {
            //Pars.Clear();
            for (int i = 0, count = Keys.Length; i < count; i++)
            {
                string fieldName = Keys[i];
                object obj = null;
                obj = GetDataFieldValue(dr[fieldName], connInfo.DbType);
                Pars.SetValue(fieldName, obj);
            }
            return (1 <= connInfo.ExecuteNonQuery(Sqldelete, Pars));
        }
        /// <summary>
        /// 增加行
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="TableName"></param>
        /// <param name="dr"></param>
        /// <param name="pars"></param>
        /// <param name="ConnType"></param>
        /// <param name="Fields"></param>
        /// <param name="FieldStr"></param>
        /// <param name="ParStr"></param>
        /// <returns></returns>
        private static bool AddDataRow(DbContext connInfo, string TableName, DataRow dr, RequestBase pars, string[] Fields, string FieldStr, string ParStr)
        {
            StringBuilder strbld = new StringBuilder(512);
            strbld.AppendFormat("insert into {0}({1}) values({2})", TableName, FieldStr, ParStr);
            for (int i = 0, count = Fields.Length; i < count; i++)
            {
                string fieldName = Fields[i];
                object obj = GetDataFieldValue(dr[fieldName], connInfo.DbType);
                pars.SetValue(fieldName, obj);
            }
            return (1 <= connInfo.ExecuteNonQuery(strbld.ToString(), pars));
        }
        /// <summary>
        /// 把Arr1中包含Arr2的元素移动到最后 ,ODP.NET ORACLE必须严格按参数的位置匹配(TMD变态)
        /// </summary>
        /// <param name="Arr1"></param>
        /// <param name="Arr2"></param>
        /// <returns></returns>
        private static string[] PutArr2ToLast(string[] Arr1, string[] Arr2)
        {
            int count = Arr1.Length;
            string[] newArr = new string[count];
            int moved = 0;
            foreach (string str in Arr1)
            {
                if (!Arr2.Contains(str,StringComparer.OrdinalIgnoreCase))
                {
                    newArr[moved] = str;
                    ++moved;
                }
            }
            foreach (string str in Arr2)
            {
                newArr[moved] = str;
                ++moved;
            }
            return newArr;
        }
        #region UPDATE
        /// <summary>
        /// 更新记录集到数据库
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="dtData"></param>
        /// <param name="NoOpWhenExists"></param>
        /// <param name="ThrowExceptionIfNotAffected"></param>
        /// <returns></returns>
        public static int UpdateDataTableToDB( this DbContext connInfo, DataTable dtData, bool NoOpWhenExists, bool ThrowExceptionIfNotAffected)
        {
            if (null == dtData || 0 == dtData.Rows.Count) return 0;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
                throw new Exception("No tablename settled on datatable!");
            var Fields = SqlBuilder.GetFields(dtData)?.ToArray();
            string[] PrimaryFields = dtData.GetPrimaryFields();
            string[] updateFields = PutArr2ToLast(Fields, PrimaryFields);
            if (null == Fields || 0 == Fields.Length ||
                null == PrimaryFields || 0 == PrimaryFields.Length)
                throw new Exception("No fields or no primary key defined!");
            RequestBase pars = new RequestBase();
            RequestBase parsInsert = new RequestBase();
            RequestBase pkPars = new RequestBase();
            string sqlUpdate = connInfo.DbType.GetUpdateSqlStr(TableName, Fields, PrimaryFields);
            string fieldStr = SqlBuilder.GetFieldStringViaColumns(Fields);
            string parStr = connInfo.DbType.GetParaStringViaColumns(Fields);
            string deleteStr = connInfo.GetDeleteSql(null, tableName: TableName,matchFields: PrimaryFields);
            int ret = 0;
            bool singleKey = (1 == PrimaryFields.Length); //单主键
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = false, Modified = false;
                if (ExistsDataRow(connInfo, TableName, dr, PrimaryFields, pkPars))
                {
                    if (!NoOpWhenExists)
                    {
                        opOK = UpdateDataRow(connInfo, TableName, dr, pars, updateFields, PrimaryFields, sqlUpdate);
                        if (opOK) Modified = true;
                    }
                    else
                    {
                        opOK = true;
                        Modified = false;
                    }
                }
                else
                {
                    opOK = AddDataRow(connInfo, TableName, dr, parsInsert, Fields, fieldStr, parStr);
                    if (opOK) Modified = true;
                }
                if (!opOK && ThrowExceptionIfNotAffected) throw new Exception("No rows affected!");
                if (opOK && Modified) ++ret;
            }
            return ret;
        }
        public static int UpdateDataTableToDB(this DbContext connInfo,DataTable dtData, bool NoOpWhenExists,bool ThrowExceptionIfNotAffected, Func<DbContext, DML, DataRow, bool> checkFunc)
        {
            if (null == checkFunc)
            {
                return UpdateDataTableToDB(connInfo, dtData, NoOpWhenExists, ThrowExceptionIfNotAffected);
            }
            if (null == dtData || 0 == dtData.Rows.Count) return 0;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
            {
                throw new Exception("No tablename settled on datatable!");
            }
            string[] Fields = dtData.GetFields()?.ToArray();
            string[] PrimaryFields = dtData.GetPrimaryFields();
            string[] updateFields = PutArr2ToLast(Fields, PrimaryFields);
            if (null == Fields || 0 == Fields.Length ||
                null == PrimaryFields || 0 == PrimaryFields.Length)
                throw new Exception("No fields or no primary key defined!");
            RequestBase pars = new RequestBase();
            RequestBase pkPars = new RequestBase();
            RequestBase parsInsert = new RequestBase();
            string sqlUpdate = connInfo.DbType.GetUpdateSqlStr(TableName, Fields, PrimaryFields);
            string fieldStr = SqlBuilder.GetFieldStringViaColumns(Fields);
            string parStr = connInfo.DbType.GetParaStringViaColumns(Fields);
            string deleteStr = connInfo.GetDeleteSql(null, tableName: TableName, matchFields: PrimaryFields);
            int ret = 0;
            bool singleKey = (1 == PrimaryFields.Length); //单主键
            //删除
            /*
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.Deleted))
            {
                bool opOK=false ;
                if (checkFunc(connInfo, DML.DELETE, dr))
                {
                    opOK = DeleteDataRow(connInfo, dr, PrimaryFields, pkPars,deleteStr);
                    if (!opOK && ThrowExceptionIfNotAffected)
                        throw new Exception("No rows affected!");
                }
                if (opOK) ++ret;
            }
            */
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = false, Modified = false;
                if (ExistsDataRow(connInfo, TableName, dr, PrimaryFields, pkPars))
                {
                    if (!NoOpWhenExists)
                    {
                        if (checkFunc(connInfo, DML.UPDATE, dr))
                        {
                            opOK = UpdateDataRow(connInfo, TableName, dr,
                                pars, updateFields, PrimaryFields, sqlUpdate);
                        }
                        if (opOK) Modified = true;
                    }
                    else
                    {
                        opOK = true;
                        Modified = false;
                    }
                }
                else
                {
                    if (checkFunc(connInfo, DML.INSERT, dr))
                    {
                        opOK = AddDataRow(connInfo, TableName, dr, parsInsert, Fields, fieldStr, parStr);
                    }
                    if (opOK) Modified = true;
                }
                if (!opOK && ThrowExceptionIfNotAffected) throw new Exception("No rows affected!");
                if (opOK && Modified) ++ret;
            }
            return ret;
        }
        /// <summary>
        /// 对数据操作时排除指定的字段，如果操作成功并且返回它
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="dtData"></param>
        /// <param name="NoOpWhenExists"></param>
        /// <param name="ThrowExceptionIfNotAffected"></param>
        /// <param name="lstDataType"></param>
        /// <param name="OpExptRetrunsField"></param>
        /// <returns></returns>
        public static List<object> UpdateDataTableToDB( this DbContext connInfo, DataTable dtData, bool NoOpWhenExists,bool ThrowExceptionIfNotAffected,string OpExptRetrunsField)
        {
            if (null == dtData || 0 == dtData.Rows.Count) return null;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
                throw new Exception("No tablename settled on datatable!");
            string[] Fields = dtData.GetFields(OpExptRetrunsField)?.ToArray();
            string[] PrimaryFields = dtData.GetPrimaryFields();
            string[] updateFields = PutArr2ToLast(Fields, PrimaryFields);
            if (null == Fields || 0 == Fields.Length ||
                null == PrimaryFields || 0 == PrimaryFields.Length)
                throw new Exception("No fields or no primary keys defined!");
            RequestBase pars = new RequestBase();
            RequestBase pkPars = new RequestBase();
            RequestBase parsInsert = new RequestBase();
            string sqlUpdate = connInfo.DbType.GetUpdateSqlStr(TableName, Fields, PrimaryFields);
            string fieldStr = SqlBuilder.GetFieldStringViaColumns(Fields);
            string parStr = connInfo.DbType.GetParaStringViaColumns(Fields);
            string deleteStr = connInfo.GetDeleteSql(null, tableName: TableName, matchFields: PrimaryFields);
            List<object> ret = new List<object>();
            bool singleKey = (1 == PrimaryFields.Length); //单主键
            /*
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.Deleted))
            {
                bool opOK = DeleteDataRow(connInfo, dr, PrimaryFields, pkPars, deleteStr, lstDataType);
                if (!opOK && ThrowExceptionIfNotAffected)
                {
                    throw new Exception("No rows affected!");
                }
                if (opOK) ret.Add(dr[OpExptRetrunsField, DataRowVersion.Original]);
            }
            */
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = false, Modified = false;
                if (ExistsDataRow(connInfo, TableName, dr, PrimaryFields, pkPars))
                {
                    if (!NoOpWhenExists)
                    {
                        opOK = UpdateDataRow(connInfo, TableName, dr, pars,
                            updateFields, PrimaryFields, sqlUpdate);
                        if (opOK) Modified = true;
                    }
                    else
                    {
                        opOK = true;
                        Modified = false;
                    }
                }
                else
                {
                    opOK = AddDataRow(connInfo, TableName, dr, parsInsert, Fields, fieldStr, parStr);
                    if (opOK) Modified = true;
                }
                if (!opOK && ThrowExceptionIfNotAffected)
                {
                    throw new Exception("No rows affected!");
                }
                if (opOK && Modified) ret.Add(dr[OpExptRetrunsField, DataRowVersion.Original]);
            }
            return ret;
        }
        public static List<object> UpdateDataTableToDB( this DbContext connInfo, DataTable dtData, bool NoOpWhenExists,bool ThrowExceptionIfNotAffected,string OpExptRetrunsField, Func<DbContext, DML, DataRow, bool> checkFunc)
        {
            if (null == checkFunc)
            {
                return UpdateDataTableToDB(connInfo, dtData,
                    NoOpWhenExists, ThrowExceptionIfNotAffected, OpExptRetrunsField);
            }
            if (null == dtData || 0 == dtData.Rows.Count) return null;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
            {
                throw new Exception("No tablename settled on datatable!");
            }
            string[] Fields = dtData.GetFields(OpExptRetrunsField)?.ToArray();
            string[] PrimaryFields = dtData.GetPrimaryFields();
            string[] updateFields = PutArr2ToLast(Fields, PrimaryFields);
            if (null == Fields || 0 == Fields.Length ||
                null == PrimaryFields || 0 == PrimaryFields.Length)
                throw new Exception("No fields or no primary keys defined!");
            RequestBase pars = new RequestBase();
            RequestBase pkPars = new RequestBase();
            RequestBase parsInsert = new RequestBase();
            string sqlUpdate = connInfo.DbType.GetUpdateSqlStr(TableName, Fields, PrimaryFields);
            string fieldStr = SqlBuilder.GetFieldStringViaColumns(Fields);
            string parStr = connInfo.DbType.GetParaStringViaColumns(Fields);
            string deleteStr = connInfo.GetDeleteSql(null, tableName: TableName, matchFields: PrimaryFields);
            List<object> ret = new List<object>();
            bool singleKey = (1 == PrimaryFields.Length); //单主键
            /*
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.Deleted))
            {
                bool opOK = false;
                if (checkFunc(connInfo, DML.DELETE, dr))
                {
                    opOK = DeleteDataRow(connInfo, dr, PrimaryFields, pkPars, deleteStr, lstDataType);
                }
                if (!opOK && ThrowExceptionIfNotAffected)
                {
                    throw new Exception("No rows affected!");
                }
                if (opOK) ret.Add(dr[OpExptRetrunsField, DataRowVersion.Original]);
            }
            */
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = false, Modified = false;
                if (ExistsDataRow(connInfo, TableName, dr, PrimaryFields, pkPars))
                {
                    if (!NoOpWhenExists)
                    {
                        if (checkFunc(connInfo, DML.UPDATE, dr))
                        {
                            opOK = UpdateDataRow(connInfo, TableName, dr, pars,
                                updateFields, PrimaryFields, sqlUpdate);
                        }
                        if (opOK) Modified = true;
                    }
                    else
                    {
                        opOK = true;
                        Modified = false;
                    }
                }
                else
                {
                    if (checkFunc(connInfo, DML.INSERT, dr))
                    {
                        opOK = AddDataRow(connInfo, TableName, dr, parsInsert, Fields, fieldStr, parStr);
                    }
                    if (opOK) Modified = true;
                }
                if (!opOK && ThrowExceptionIfNotAffected)
                {
                    throw new Exception("No rows affected!");
                }
                if (opOK && Modified) ret.Add(dr[OpExptRetrunsField, DataRowVersion.Original]);
            }
            return ret;
        }
        #endregion
        #region "DELETE"
        public static int DeleteWithDataTable(this DbContext connInfo, DataTable dtData, bool ThrowExceptionIfNotAffected)
        {
            if (null == dtData || 0 == dtData.Rows.Count) return 0;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
            {
                throw new Exception("No tablename settled on datatable!");
            }
            string[] PrimaryFields = dtData.GetPrimaryFields();
            if (null == PrimaryFields || 0 == PrimaryFields.Length)
                throw new Exception("No fields or no primary key defined!");
            RequestBase pkPars = new RequestBase();
            string deleteStr = connInfo.GetDeleteSql(null, tableName: TableName, matchFields: PrimaryFields);
            int ret = 0;
            bool singleKey = (1 == PrimaryFields.Length); //单主键
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = DeleteDataRow2(connInfo, dr, PrimaryFields, pkPars, deleteStr);
                if (!opOK && ThrowExceptionIfNotAffected)
                    throw new Exception("No rows affected!");
                if (opOK) ++ret;
            }
            return ret;
        }
        public static int DeleteWithDataTable(this DbContext connInfo, DataTable dtData, bool ThrowExceptionIfNotAffected, Func<DbContext, DML, DataRow, bool> checkFunc)
        {
            if (null == checkFunc)
            {
                return DeleteWithDataTable(connInfo, dtData, ThrowExceptionIfNotAffected);
            }
            if (null == dtData || 0 == dtData.Rows.Count) return 0;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
                throw new Exception("No tablename settled on datatable!");
            string[] PrimaryFields = dtData.GetPrimaryFields();
            if (null == PrimaryFields || 0 == PrimaryFields.Length)
                throw new Exception("No fields or no primary key defined!");
            RequestBase pkPars = new RequestBase();
            string deleteStr = connInfo.GetDeleteSql(null, tableName: TableName, matchFields: PrimaryFields);
            int ret = 0;
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = false;
                if (checkFunc(connInfo, DML.DELETE, dr))
                {
                    opOK = DeleteDataRow2(connInfo, dr, PrimaryFields, pkPars, deleteStr);
                }
                if (!opOK && ThrowExceptionIfNotAffected)
                {
                    throw new Exception("No rows affected!");
                }
                if (opOK) ++ret;
            }
            return ret;
        }
        #endregion
        #region "INSERT"
        /// <summary>
        /// 直接插入数据,不检测数据是否存在
        /// </summary>
        /// <param name="Conn"></param>
        /// <param name="dtData"></param>
        /// <param name="ThrowExceptionIfNotAffected"></param>
        /// <param name="lstDataType"></param>
        /// <returns></returns>
        public static int DirectInsert(this DbContext connInfo, DataTable dtData, bool ThrowExceptionIfNotAffected)
        {
            if (null == dtData || 0 == dtData.Rows.Count) return 0;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
            {
                throw new Exception("No tablename settled on datatable!");
            }
            string[] Fields = dtData.GetFields()?.ToArray();
            string[] PrimaryFields = dtData.GetPrimaryFields();
            if (null == Fields || 0 == Fields.Length ||
                null == PrimaryFields || 0 == PrimaryFields.Length)
            {
                throw new Exception("No fields or no primary keys defined!");
            }
            RequestBase pars = new RequestBase();
            string fieldStr = SqlBuilder.GetFieldStringViaColumns(Fields);
            string parStr = connInfo.DbType.GetParaStringViaColumns(Fields);
            int ret = 0;
            bool singleKey = (1 == PrimaryFields.Length); //单主键
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = false, Modified = false;
                opOK = AddDataRow(connInfo, TableName, dr, pars, Fields, fieldStr, parStr);
                if (opOK) Modified = true;
                if (!opOK && ThrowExceptionIfNotAffected)
                    throw new Exception("No rows affected!");
                if (opOK && Modified) ++ret;
            }
            return ret;
        }
        public static int DirectInsert(this DbContext connInfo, DataTable dtData,bool ThrowExceptionIfNotAffected,Func<DbContext, DML, DataRow, bool> checkFunc)
        {
            if (null == checkFunc)
            {
                return DirectInsert(connInfo, dtData, ThrowExceptionIfNotAffected);
            }
            if (null == dtData || 0 == dtData.Rows.Count) return 0;
            string TableName = dtData.TableName;
            if (string.IsNullOrEmpty(TableName))
            {
                throw new Exception("No tablename settled on datatable!");
            }
            string[] Fields = dtData.GetFields()?.ToArray();
            string[] PrimaryFields = dtData.GetPrimaryFields();
            if (null == Fields || 0 == Fields.Length ||
                null == PrimaryFields || 0 == PrimaryFields.Length)
                throw new Exception("No fields or no primary keys defined!");
            RequestBase pars = new RequestBase();
            string fieldStr = SqlBuilder.GetFieldStringViaColumns(Fields);
            string parStr = connInfo.DbType.GetParaStringViaColumns(Fields);
            int ret = 0;
            foreach (DataRow dr in dtData.Select("", "", DataViewRowState.CurrentRows))
            {
                bool opOK = false, Modified = false;
                if (checkFunc(connInfo, DML.INSERT, dr))
                {
                    opOK = AddDataRow(connInfo, TableName, dr, pars, Fields, fieldStr, parStr);
                }
                if (opOK) Modified = true;
                if (!opOK && ThrowExceptionIfNotAffected)
                {
                    throw new Exception("No rows affected!");
                }
                if (opOK && Modified) ++ret;
            }
            return ret;
        }
        #endregion
    }
}
