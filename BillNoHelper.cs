using System;
namespace SmartCrud
{
    /*
    SET ANSI_NULLS ON
    GO

SET QUOTED_IDENTIFIER ON
GO

SET ANSI_PADDING ON
GO

CREATE TABLE[dbo].[t_BillNo]
    (

   [FBranchCode][varchar](36) NOT NULL,

  [FBillType] [int]
    NOT NULL,

  [FName] [varchar] (56) NOT NULL,

   [FPrefix] [varchar] (20) NOT NULL,

    [FSerialbit] [int] NULL,
	[FFillChar] [char] (1) NULL,
	[FSerialFormat]
    [int]
    NOT NULL,

    [FLstSerialPreFix] [varchar] (20) NOT NULL,
 
     [FLstNumber] [int] NULL,
	[FMaxId] [int] NULL,
	[FMultiCheck] [int] NULL,
 CONSTRAINT[YSBILLNO_BRA_TYPE] PRIMARY KEY CLUSTERED
(
   [FBranchCode] ASC,
   [FBillType] ASC
)WITH(PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON[PRIMARY]
) ON[PRIMARY]

GO

SET ANSI_PADDING OFF
GO

ALTER TABLE[dbo].[t_BillNo] ADD DEFAULT((0)) FOR[FMaxId]
GO

ALTER TABLE[dbo].[t_BillNo] ADD DEFAULT((0)) FOR[FMultiCheck]
GO


CREATE TABLE [dbo].[BillLock](
	[FBranchCode] [varchar](36) NOT NULL,
	[FBillType] [int] NOT NULL,
	[FBillId] [bigint] NOT NULL,
	[FDate] [smalldatetime] NULL,
 CONSTRAINT [ysBillLockPri] PRIMARY KEY CLUSTERED 
(
	[FBranchCode] ASC,
	[FBillType] ASC
)WITH (PAD_INDEX  = OFF, STATISTICS_NORECOMPUTE  = OFF, IGNORE_DUP_KEY = OFF, ALLOW_ROW_LOCKS  = ON, ALLOW_PAGE_LOCKS  = ON) ON [PRIMARY]
) ON [PRIMARY]

GO

SET ANSI_PADDING OFF
GO

ALTER TABLE [dbo].[BillLock] ADD  DEFAULT (getdate()) FOR [FDate]

    */
    public interface IBillOp  //单据类的操作接口
    {
        BILLLOCKRESULT LockBill(int BillId);
        bool IsBillLocked(int Id);
        void UnLockBill(int Id);
        string GetBillNo();
        bool Increase();
        int GetInterId();
        Tuple<int, string> GetAutoNum();
    }
    [Serializable]
    internal class BillSetting
    {
        [Key(0)]
        public string FBranchCode { get; set; } = "FireWolf";
        [Key(1)]
        public int FBillType { get; set; } = 1;
        public string FName { get; set; } = "FireWolf";
        public string FPrefix { get; set; } = string.Empty;
        public int FSerialbit { get; set; } = 11;
        public string FFillChar { get; set; } = string.Empty;
        public int FSerialFormat { get; set; } = 3;
        public string FLstSerialPreFix { get; set; } = string.Empty;
        public int FLstNumber { get; set; } = 0;
        public int FMaxId { get; set; } = 0;
        public bool FMultiCheck { get; set; }
    }
    internal class BillNoRepository
    {
        protected DbContext _Conn = null;
        protected string BillTableName = "t_BillNo";
        protected string BillLockTableName = "BillLock";
        public BillNoRepository(DbContext Conn)
        {
            _Conn = Conn;
        }
        public BillNoRepository(DbContext Conn, string sqlBillNoTableName, string sqlLockTableName)
            : this(Conn)
        {
            if (!string.IsNullOrEmpty(sqlBillNoTableName))
            {
                BillTableName = sqlBillNoTableName;
            }
            if (!string.IsNullOrEmpty(sqlLockTableName))
            {
                BillLockTableName = sqlLockTableName;
            }
        }
        private bool ExistsBillSetting(string BranchCode, int BillType) =>
            _Conn.Exists<BillSetting>(new object[] { BranchCode, BillType }, BillTableName);
        public void AddBillSetting(string BranchCode, int BillType, string Name,
            string Prefix, int SerialBits, char FillChar, int SerialFormat, string LstPreFix, int LstNum)
        {
            if (ExistsBillSetting(BranchCode, BillType)) return;
            if (null == Prefix) Prefix = string.Empty;
            if (null == LstPreFix) LstPreFix = string.Empty;
            if (SerialFormat > 0 && string.IsNullOrWhiteSpace(LstPreFix))
                LstPreFix = GetInitPrefixSerial(SerialFormat);
            _Conn.Insert<BillSetting>(record: new BillSetting
            {
                FBranchCode = BranchCode.ToUpper(),
                FBillType = BillType,
                FFillChar = FillChar.ToString(),
                FLstNumber = LstNum,
                FLstSerialPreFix = LstPreFix,
                FMaxId = LstNum,
                FMultiCheck = false,
                FName = Name,
                FPrefix = Prefix,
                FSerialbit = SerialBits,
                FSerialFormat = SerialFormat
            }, tableName: BillTableName);
        }
        public void AddBillSetting(string BranchCode, int BillType, string Name,
            string Prefix, int SerialBits, char FillChar, BILLFORMAT2 SerialFormat, string LstPreFix, int LstNum)
        {
            AddBillSetting(BranchCode, BillType, Name, Prefix, SerialBits, FillChar, (int)SerialFormat, LstPreFix, LstNum);
        }
        private string GetInitPrefixSerial(int SerialFormat)
        {
            if (SerialFormat > 0)
            {
                DateTime Now = _Conn.Now();
                string PrefixSerial = "";
                switch (SerialFormat)
                {
                    case 1:
                        PrefixSerial = Now.ToString("yyyyMM");
                        break;
                    case 2:
                        PrefixSerial = Now.ToString("yyMM");
                        break;
                    case 3:
                        PrefixSerial = Now.ToString("yyyyMMdd");
                        break;
                    case 4:
                        PrefixSerial = Now.ToString("yyMMdd");
                        break;
                    case 5:
                        PrefixSerial = Now.ToString("yy");
                        break;
                }
                return PrefixSerial;
            }
            return string.Empty;
        }
        public BillSetting GetBillSetting(string branchCode, int billType)
        {
            BillSetting billSetting = _Conn.Select<BillSetting>(pkValues: new object[] { branchCode, billType },tableName: BillTableName);
            if (null == billSetting) return null;
            if (billSetting.FSerialFormat > 0)
            {
                string PrefixSerial = GetInitPrefixSerial(billSetting.FSerialFormat);
                if (string.Compare(billSetting.FLstSerialPreFix, PrefixSerial, true) != 0)
                {
                    if (string.IsNullOrWhiteSpace(billSetting.FLstSerialPreFix))//第一次使用,FLstSerialPreFix为空
                    {
                        _Conn.Update<BillSetting>(new BillSetting
                        {
                            FBillType = billType,
                            FBranchCode = branchCode,
                            FLstSerialPreFix = PrefixSerial
                        }, onlyFields: c=>  c.FLstSerialPreFix  , tableName: BillTableName);
                    }
                    else
                    {
                        _Conn.ExecuteNonQuery($"Update {BillTableName} set FLstSerialPreFix=#LstPrefix#,FLstNumber=1 where FBranchCode=#BranchCode# and FBillType=#BillType#",
                            new { LstPrefix = PrefixSerial, BranchCode = branchCode, BillType = billType });
                        billSetting.FLstSerialPreFix = PrefixSerial;
                        billSetting.FLstNumber = 1;
                    }
                }
            }
            return billSetting;
        }
        public string GetBillNo(string branchCode, int billType)
        {
            BillSetting billSetting = GetBillSetting(branchCode, billType);
            if (null == billSetting)
                throw new ArgumentNullException(nameof(billSetting));
            string Num = "";
            BILLFORMAT2 SerialFormat = SmartCrudHelper.GetEnumItemByValue<BILLFORMAT2>(billSetting.FSerialFormat);
            if (BILLFORMAT2.BIT36 == SerialFormat)
                Num = AnyRadixConvert.ConvertTo(billSetting.FLstNumber, 36);
            else if (BILLFORMAT2.BIT16 == SerialFormat)
                Num = AnyRadixConvert.ConvertTo(billSetting.FLstNumber, 16);
            else
                Num = billSetting.FLstNumber.ToString();
            if (!string.IsNullOrEmpty(billSetting.FFillChar))
            {
                int Fillbits = billSetting.FSerialbit - (billSetting.FLstSerialPreFix.Length + Num.Length);
                if (Fillbits > 0) Num = Num.PadLeft(Fillbits + Num.Length, billSetting.FFillChar[0]);
            }
            return (string.Format("{0}{1}{2}", billSetting.FPrefix, billSetting.FLstSerialPreFix, Num));
        }
        bool Increase(DbContext conn, string branchCode, int billType)
        {
            return 1 == conn.ExecuteNonQuery(
                $"Update {BillTableName} set FMaxId=FMaxId+1,FLstNumber=FLstNumber+1 where FBranchCode=#BranchCode# and FBillType=#BillType#",
                new { BranchCode = branchCode, BillType = billType });
        }
        public bool Increase(string branchCode, int billType) => Increase(_Conn, branchCode, billType);
        public Tuple<int, string> GetAutoNum(string branchCode, int billType)
        {
            try
            {
                _Conn.BeginTrans();
                Increase(branchCode, billType);
                int newId = GetInterId(branchCode, billType);
                string newBillNo = GetBillNo(branchCode, billType);
                _Conn.Commit();
                return new Tuple<int, string>(newId, newBillNo);
            }
            catch (Exception ex)
            {
                if (_Conn.IsInTransaction) _Conn.Rollback();
                throw ex;
            }
        }
        public long GetInterLongId(string branchCode, int billType)
        {
            int billId = GetInterId(branchCode, billType);
            int V = Math.Abs(branchCode.Trim().ToUpper().GetHashCode());
            long Ret = 0;
            string str = string.Format("{0}{1}", V, billId);
            if (!long.TryParse(str, out Ret))
                throw new Exception($"GetInterLongId:{str}");
            return Ret;
        }
        public int GetInterId(string branchCode, int billType)
        {
            string sql = $"SELECT #ISNULL#(MAX(FMAXID),0) FROM {BillTableName} where  FBranchCode=#BranchCode# and FBillType=#BillType#";
            RequestBase pars = new RequestBase();
            pars.Add("BranchCode", branchCode);
            pars.Add("BillType", billType);
            object ret = _Conn.ExecuteScalar(sql, pars);
            if (SmartCrudHelper.IsNullOrDBNull(ret))
                return 0;
            else
                return Convert.ToInt32(ret);
        }
        public bool IsBillLocked(string branchCode, int billType, long billId) =>
            IsBillLocked(_Conn, branchCode, billType, billId);
        private bool IsBillLocked(DbContext conn, string branchCode, int billType, long billId)
        {
            return conn.ExistsPrimitive($"SELECT 'Y' FROM {BillLockTableName} where FBranchCode=#BranchCode# and FBillType=#BillType# and FBillId=#BillId#",
                RequestBase.FromKV("BranchCode", branchCode)
                .SetValue("BillType", billType)
                .SetValue("BillId", billId));
        }
        public BILLLOCKRESULT LockBill(string branchCode, int billType, long billId)
        {
            return _Conn.RunTransactionWithResult<BILLLOCKRESULT>(cn =>
            {
                if (IsBillLocked(cn, branchCode, billType, billId))
                    return BILLLOCKRESULT.ALREADY_LOCKED;
                int rows = cn.ExecuteNonQuery($"Insert into {BillLockTableName}(FBranchCode,FBillType,FBillId,FDate)Values(#BranchCode#,#BillType#,#BillId#,#NOW#)",
                    new { BranchCode = branchCode, BillType = billType, BillId = billId });
                return rows == 1 ? BILLLOCKRESULT.SUCCESS_LOCK : BILLLOCKRESULT.FAIL_LOCK;
            }).data;
        }
        public void UnLockBill(string branchCode, int billType, long billId)
        {
            RequestBase pars = new RequestBase();
            pars.SetValue("BranchCode", branchCode);
            pars.SetValue("BillType", billType);
            pars.SetValue("BillId", billId);
            _Conn.ExecuteNonQuery($"Delete from {BillLockTableName}  where FBranchCode=#BranchCode# and FBillType=#BillType# and FBillId=#BillId#", pars);
        }
    }
    /// <summary>
    /// 最终是继承这个类
    /// </summary>
    public abstract class BillNoHelper : IBillOp
    {
        public abstract int BillNum { get; }
        public abstract string BillName { get; }
        public virtual string Prefix { get { return ""; } }
        public abstract int SerialBits { get; }
        public virtual char FillChar { get { return '0'; } }
        public abstract BILLFORMAT2 BillFormat { get; }
        public abstract string LastPrefix { get; }
        public virtual int LastNumber { get { return 0; } }
        private BillNoRepository _obj = null;
        protected string _braCode = "XXX";
        protected DbContext _connInfo = null;
        protected virtual string SqlBillTableName { get { return "t_BillNo"; } }
        protected virtual string SqlLockTableName { get { return "BillLock"; } }
        public BillNoHelper(DbContext connInfo, string BranchCode)
        {
            _braCode = BranchCode;
            _connInfo = connInfo;
            string sqlBillTableName = this.SqlBillTableName;
            string sqlLockTableName = this.SqlLockTableName;
            if (string.IsNullOrEmpty(sqlBillTableName) &&
                string.IsNullOrEmpty(sqlLockTableName))
            {
                _obj = new BillNoRepository(connInfo);
            }
            else
            {
                _obj = new BillNoRepository(connInfo, sqlBillTableName, sqlLockTableName);
            }
            _obj.AddBillSetting(_braCode, BillNum, BillName,
                Prefix, SerialBits, FillChar, BillFormat, LastPrefix, LastNumber);
        }
        public BillNoHelper(DbContext connInfo, string BranchCode, string prefix)
        {
            _braCode = BranchCode;
            _connInfo = connInfo;
            string sqlBillTableName = this.SqlBillTableName;
            string sqlLockTableName = this.SqlLockTableName;
            if (string.IsNullOrEmpty(sqlBillTableName) &&
                string.IsNullOrEmpty(sqlLockTableName))
            {
                _obj = new BillNoRepository(connInfo);
            }
            else
            {
                _obj = new BillNoRepository(connInfo, sqlBillTableName, sqlLockTableName);
            }
            _obj.AddBillSetting(_braCode, BillNum, BillName,
                prefix, SerialBits, FillChar, BillFormat, LastPrefix, LastNumber);
        }
        public BILLLOCKRESULT LockBill(int Id)
        {
            return _obj.LockBill(_braCode, BillNum, Id);
        }
        public bool IsBillLocked(int Id)
        {
            return _obj.IsBillLocked(_braCode, BillNum, Id);
        }
        public void UnLockBill(int Id)
        {
            _obj.UnLockBill(_braCode, BillNum, Id);
        }
        public Tuple<int, string> GetAutoNum() => _obj.GetAutoNum(_braCode, BillNum);
        public string GetBillNo()
        {
            return _obj.GetBillNo(_braCode, BillNum);
        }
        public bool Increase()
        {
            return _obj.Increase(_braCode, BillNum);
        }
        public int GetInterId()
        {
            return _obj.GetInterId(_braCode, BillNum);
        }
    }
}
