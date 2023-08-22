using TimeZoneConverter;

namespace Api.ReferenceLib.Services
{
    public class TimezoneService
    {
        public static DateTimeOffset Convert()
        {
            TimeZoneInfo tzi = TZConvert.GetTimeZoneInfo("Asia/Bangkok");
            DateTimeOffset currentTime = DateTimeOffset.UtcNow.ToOffset(tzi.GetUtcOffset(DateTimeOffset.UtcNow));
            return currentTime;
        }

        public static DateTimeOffset FromMili(long ts)
        {
            // long unixTimestampMilliseconds = 1620563285000; // Unix timestamp in milliseconds
            DateTimeOffset dateTimeOffset = DateTimeOffset.FromUnixTimeMilliseconds(ts);
            TimeZoneInfo tzi = TZConvert.GetTimeZoneInfo("Asia/Bangkok");
            DateTimeOffset currentTime = new DateTimeOffset(dateTimeOffset.DateTime, tzi.GetUtcOffset(dateTimeOffset.DateTime));
            // Console.WriteLine($"From Mili: {dateTimeOffset.ToString()}");
            return currentTime;
        }
    }
}