using System;
using System.Collections.Generic;
using System.Linq.Expressions;

namespace SmartCrud
{
    public class RequestBase : Dictionary<string, object>
    {
        public static RequestBase FromKV(string key, object value)
        {
            return new RequestBase().SetValue(key, value);
        }
        public RequestBase SetValue<T>(Expression<Func<T, object>> expr, object value) where T : class, new()
        {
            return SetValue(SmartCrudHelper.GetPropertyName<T>(expr), value);
        }
        public static RequestBase FromKV<T>(Expression<Func<T, object>> expr, object value) where T:class, new()
        {
            return FromKV(SmartCrudHelper.GetPropertyName<T>(expr), value);
        }
        public RequestBase RemoveKeys(params string[] keys)
        {
            if ((keys?.Length ?? 0) > 0)
            {
                foreach (string key in keys) this.Remove(key);
            }
            return this;
        }
        public RequestBase SetValue(string key, object value)
        {
            this[key] = value;
            return this;
        }
        public RequestBase AddFromDictionary(Dictionary<string, object> dic)
        {
            if (null != dic)
            {
                foreach (var ele in dic) SetValue(ele.Key, ele.Value);
            }
            return this;
        }
    }
    public interface IResultBase
    {
        int errorCode { get; set; }
        string errorMessage { get; set; }
    }
    public class ResultBase : IResultBase
    {
        public bool Success => 0 == this.errorCode;
        public int errorCode { get; set; }
        public string errorMessage { get; set; }
        public override string ToString()
        {
            return $"{errorMessage}({errorCode})";
        }
        public ResultBase(int errCode, string errMsg)
        {
            this.errorCode = errCode;
            this.errorMessage = errMsg;
        }
        public ResultBase() : this(0, "success") { }
        public static ResultBase SUCCESS => new ResultBase { errorCode = 0, errorMessage = "success" };
        public static ResultBase FAIL => new ResultBase { errorCode = -1, errorMessage = "fail" };
    }
    public class ResultExchange<T> : ResultBase
    {
        public string details { get; set; }
        public T data { get; set; }
        public ResultExchange(int code, string msg)
        {
            this.errorCode = code;
            this.errorMessage = msg;
        }
        public ResultExchange(int code, string msg, string detail)
            : this(code, msg)
        {
            this.details = detail;
        }
        public ResultExchange(int code, string msg, string detail, T data)
            : this(code, msg, detail)
        {
            this.data = data;
        }
        public ResultExchange(int code, string msg, T data)
          : this(code, msg, "",data)
        {
        }
        public ResultExchange() : this(0, "success", string.Empty, default(T)) { }
    }

}
