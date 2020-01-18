using System;
namespace SmartCrud
{
    /// <summary>
    /// https://github.com/ccollie/snowflake-net/tree/master/Snowflake
    /// </summary>
    internal class DisposableAction : IDisposable
    {
        readonly Action _action;

        public DisposableAction(Action action)
        {
            if (action == null)
                throw new ArgumentNullException("action");
            _action = action;
        }

        public void Dispose()
        {
            _action();
        }
    }
    public class InvalidSystemClock : Exception
    {
        public InvalidSystemClock(string message) : base(message) { }
    }
    internal static class SnowflackExtension
    {
        public static Func<long> currentTimeFunc = InternalCurrentTimeMillis;

        public static long CurrentTimeMillis()
        {
            return currentTimeFunc();
        }

        public static IDisposable StubCurrentTime(Func<long> func)
        {
            currentTimeFunc = func;
            return new DisposableAction(() =>
            {
                currentTimeFunc = InternalCurrentTimeMillis;
            });
        }

        public static IDisposable StubCurrentTime(long millis)
        {
            currentTimeFunc = () => millis;
            return new DisposableAction(() =>
            {
                currentTimeFunc = InternalCurrentTimeMillis;
            });
        }
        private static long InternalCurrentTimeMillis()
        {
            return (long)(DateTime.UtcNow - SmartCrudHelper.StartTime).TotalMilliseconds;
        }
    }
    public class IdWorker
    {
        public const long Twepoch = 1288834974657L;

        const int WorkerIdBits = 5;
        const int DatacenterIdBits = 5;
        const int SequenceBits = 12;
        const long MaxWorkerId = -1L ^ (-1L << WorkerIdBits);
        const long MaxDatacenterId = -1L ^ (-1L << DatacenterIdBits);

        private const int WorkerIdShift = SequenceBits;
        private const int DatacenterIdShift = SequenceBits + WorkerIdBits;
        public const int TimestampLeftShift = SequenceBits + WorkerIdBits + DatacenterIdBits;
        private const long SequenceMask = -1L ^ (-1L << SequenceBits);

        private long _WorkerId, _DatacenterId;


        private long _sequence = 0L;
        private long _lastTimestamp = -1L;
        public IdWorker(long sequence = 0L)
        {
            long workerId = 0, datacenterId = 0L;
            workerId = Math.Abs(System.Threading.Thread.CurrentThread.ManagedThreadId);
            datacenterId = Math.Abs(System.Net.Dns.GetHostName().GetHashCode());
            if (workerId > MaxWorkerId)
                _WorkerId = workerId % MaxWorkerId;
            else
                _WorkerId = workerId;
            if (datacenterId > MaxDatacenterId)
                _DatacenterId = datacenterId % MaxDatacenterId;
            else
                _DatacenterId = datacenterId;
            _sequence = sequence;
        }
        public IdWorker(long workerId, long datacenterId, long sequence = 0L)
        {
            _WorkerId = workerId;
            _DatacenterId = datacenterId;
            if(_WorkerId==0) _WorkerId= Math.Abs(System.Threading.Thread.CurrentThread.ManagedThreadId);
            if(_DatacenterId==0)_DatacenterId = Math.Abs(System.Net.Dns.GetHostName().GetHashCode());
            if (_WorkerId > MaxWorkerId)
                _WorkerId = _WorkerId % MaxWorkerId;
            if (_DatacenterId > MaxDatacenterId)
                _DatacenterId = _DatacenterId % MaxDatacenterId;
            _sequence = sequence;
            if (_WorkerId > MaxWorkerId || _WorkerId < 0)
            {
                throw new ArgumentException(String.Format("worker Id can't be greater than {0} or less than 0", MaxWorkerId));
            }
            if (_DatacenterId > MaxDatacenterId || _DatacenterId < 0)
            {
                throw new ArgumentException(String.Format("datacenter Id can't be greater than {0} or less than 0", MaxDatacenterId));
            }	
        }
        public long Sequence
        {
            get { return _sequence; }
            internal set { _sequence = value; }
        }

        // def get_timestamp() = System.currentTimeMillis

        readonly object _lock = new Object();

        public virtual long NextId()
        {
            lock (_lock)
            {
                var timestamp = TimeGen();

                if (timestamp < _lastTimestamp)
                {
                    //exceptionCounter.incr(1);
                    //log.Error("clock is moving backwards.  Rejecting requests until %d.", _lastTimestamp);
                    throw new InvalidSystemClock(String.Format(
                        "Clock moved backwards.  Refusing to generate id for {0} milliseconds", _lastTimestamp - timestamp));
                }

                if (_lastTimestamp == timestamp)
                {
                    _sequence = (_sequence + 1) & SequenceMask;
                    if (_sequence == 0)
                    {
                        timestamp = TilNextMillis(_lastTimestamp);
                    }
                }
                else
                {
                    _sequence = 0;
                }

                _lastTimestamp = timestamp;
                var id = ((timestamp - Twepoch) << TimestampLeftShift) |
                         (_DatacenterId << DatacenterIdShift) |
                         (_WorkerId << WorkerIdShift) | _sequence;

                return id;
            }
        }

        protected virtual long TilNextMillis(long lastTimestamp)
        {
            var timestamp = TimeGen();
            while (timestamp <= lastTimestamp)
            {
                timestamp = TimeGen();
            }
            return timestamp;
        }

        protected virtual long TimeGen()
        {
            return SnowflackExtension.CurrentTimeMillis();
        }
    }
}
