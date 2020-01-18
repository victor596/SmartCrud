using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

namespace SmartCrud
{
    /// <summary>
    /// 带错误编号的异常类
    /// </summary>
    public class ClassedException : Exception
    {
        public int ErrorCode { get; set; }
        public string MessageEn { get; set; }
        public ClassedException()
        {
            this.ErrorCode = -1;
            this.MessageEn = "unknow error";
        }
        public ClassedException(int errorCode, string ExpMsg)
            : base(ExpMsg)
        {
            this.ErrorCode = errorCode;
        }
        public ClassedException(int errorCode, string ExpMsg, string ExpMsgEn)
            : base(ExpMsg)
        {
            this.ErrorCode = errorCode;
            this.MessageEn = ExpMsgEn;
        }
        public static ClassedException FromResultBase(IResultBase result) =>
            new ClassedException(result.errorCode, result.errorMessage);
        public override string ToString()
        {
            return $"{this.Message}({this.ErrorCode})";
        }
    }
}
