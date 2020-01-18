using System;
using System.Data;

namespace SmartCrud
{
    public static class TransactionUtil
    {
        /// <summary>
        /// 运行语句，永远不抛出
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="dbconnHandler"></param>
        /// <param name="handler"></param>
        /// <param name="errorHandler"></param>
        /// <returns></returns>
        public static ResultExchange<T> RunSql<T>(this DbContext connInfo,Func<DbContext, T> handler, Action<Exception> errorHandler = null)
        {
            ResultExchange<T> result = new ResultExchange<T> { errorCode = 0, errorMessage = "success" };
            try
            {
                result.data = handler(connInfo);
            }
            catch (Exception ex)
            {
                errorHandler?.Invoke(ex);
                result.errorCode = -1;
                result.errorMessage = ex.Message;
            }
            return result;
        }
        /// <summary>
        /// 运行事务
        /// aftTransactionSuccessHandler如果出错永远不会抛出
        /// befTransactionHandler失败就直接退出
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="handler"></param>
        /// <param name="successValue"></param>
        /// <param name="throwIfError"></param>
        /// <returns></returns>
        public static ResultExchange<T> RunTransactionWithResult<T>(
            this DbContext connInfo,
            Func<DbContext, T> handler, bool throwIfError = false,
            Action aftTransactionSuccessHandler = null,
            Action<Exception> errorHandler = null,
            Action befTransactionHandler = null,
            IsolationLevel? isolation = null)
        {
            ResultExchange<T> result = new ResultExchange<T>();
            try
            {
                befTransactionHandler?.Invoke();
            }
            catch (Exception ex)
            {
                ex.ExpToResult(result);
                errorHandler?.Invoke(ex);
                return result;
            }
            try
            {
                if (isolation.HasValue)
                    connInfo.BeginTrans(isolation.Value);
                else
                    connInfo.BeginTrans();
                result.data = handler(connInfo);
                if (null != result && result.Success)
                {
                    connInfo.Commit();
                    try //不应该抛出错误
                    {
                        aftTransactionSuccessHandler?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        errorHandler?.Invoke(ex);
                    }
                }
                else
                {
                    connInfo.Rollback();
                }
            }
            catch (Exception ex)
            {
                ex.ExpToResult(result);
                if (connInfo.IsInTransaction) connInfo.Rollback();
                errorHandler?.Invoke(ex);
                if (throwIfError) throw ex;
            }
            return result;
        }
        /// <summary>
        /// 运行事务(不抛出错误),与以上的不太一样
        /// aftTransactionSuccessHandler如果出错永远不会抛出
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="handler"></param>
        /// <param name="successValue"></param>
        /// <param name="result"></param>
        /// <param name="errorHandler"></param>
        /// <returns></returns>
        public static T RunTransaction<T>(
            this DbContext connInfo,
          Func<DbContext, bool> handler, bool throwIfError = false,
          Action aftTransactionSuccessHandler = null,
          Action<Exception> errorHandler = null,
          Action befTransactionHandler = null,
          IsolationLevel? isolation = null)
            where T : ResultBase, new()
        {
            T result = new T { errorCode = -1, errorMessage = "fail" };
            try
            {
                befTransactionHandler?.Invoke();
            }
            catch (Exception ex)
            {
                ex.ExpToResult(result);
                errorHandler?.Invoke(ex);
                return result;
            }
            try
            {
                if (isolation.HasValue)
                    connInfo.BeginTrans(isolation.Value);
                else
                    connInfo.BeginTrans();
                if (handler(connInfo))
                {
                    connInfo.Commit();
                    result.errorCode = 0;
                    result.errorMessage = "success";
                    try //不应该抛出错误
                    {
                        aftTransactionSuccessHandler?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        errorHandler?.Invoke(ex);
                    }
                }
                else
                {
                    connInfo.Rollback();
                }
            }
            catch (Exception ex)
            {
                if (connInfo.IsInTransaction) connInfo.Rollback();
                errorHandler?.Invoke(ex);
                if (throwIfError) throw ex;
                ex.ExpToResult(result);
            }
            return result;
        }
        /// <summary>
        /// handlerSuccessFillValue: 把handler的返回值object 写到 T返回值中
        /// </summary>
        /// <typeparam name="T"></typeparam>
        /// <param name="connInfo"></param>
        /// <param name="handler"></param>
        /// <param name="aftTransactionSuccessHandler"></param>
        /// <param name="errorHandler"></param>
        /// <param name="befTransactionHandler"></param>
        /// <param name="handlerSuccessFillValue"></param>
        /// <returns></returns>
        public static T RunTransactionFillValue<T>( this DbContext connInfo,
            Func<DbContext, Tuple<bool,object>> handler, bool throwIfError = false,
            Action aftTransactionSuccessHandler = null,
            Action<Exception> errorHandler = null,
            Action befTransactionHandler = null,
            Action<T,object> handlerSuccessFillValue = null , 
            IsolationLevel? isolation = null)
          where T : ResultBase, new()
        {
            T result = new T { errorCode = -1, errorMessage = "fail" };
            try
            {
                befTransactionHandler?.Invoke();
            }
            catch (Exception ex)
            {
                ex.ExpToResult(result);
                errorHandler?.Invoke(ex);
                return result;
            }
            try
            {
                if (isolation.HasValue)
                    connInfo.BeginTrans(isolation.Value);
                else
                    connInfo.BeginTrans();
                Tuple<bool, object> tempRet = handler(connInfo);
                if (tempRet?.Item1??false )
                {
                    connInfo.Commit();
                    result.errorCode = 0;
                    result.errorMessage = "success";
                    try //不应该抛出错误
                    {
                        handlerSuccessFillValue?.Invoke(result, tempRet.Item2);
                        aftTransactionSuccessHandler?.Invoke();
                    }
                    catch (Exception ex)
                    {
                        errorHandler?.Invoke(ex);
                    }
                }
                else
                {
                    connInfo.Rollback();
                }
            }
            catch (Exception ex)
            {
                if (connInfo.IsInTransaction) connInfo.Rollback();
                errorHandler?.Invoke(ex);
                if (throwIfError) throw ex;
                ex.ExpToResult(result);
            }
            return result;
        }
    }
}
