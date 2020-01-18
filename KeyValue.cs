using System;

namespace SmartCrud
{
    [Serializable]
    public class KeyValue<TKey, TValue>
    {
        public TKey Key { get; set; }
        public TValue Value { get; set; }
        public KeyValue()
        {
            this.Key = default(TKey);
            this.Value = default(TValue);
        }
        public KeyValue(TKey key, TValue value)
        {
            this.Key = key;
            this.Value = value;
        }
        public override string ToString()
        {
            return this.Key.ToString();
        }
    }
}
