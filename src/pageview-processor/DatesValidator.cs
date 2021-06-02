using System;
using System.Globalization;

namespace pageview_processor
{
    internal static class DatesValidator
    {
        internal static (bool isValid, DateTime from, DateTime to) ValidateAndGet(string format, string from, string to)
        {
            (bool isValid, DateTime date) Parse(string date)
            {
                if (date is null) return (true, DateTime.UtcNow);

                var isValid = DateTime.TryParseExact(date, format,
                    CultureInfo.InvariantCulture, DateTimeStyles.None, out var parseDate);

                return (isValid, parseDate);
            }

            var fromDate = Parse(from);
            var toDate = Parse(to);

            return (fromDate.isValid && toDate.isValid && (fromDate.date <= toDate.date), fromDate.date, toDate.date);
        }
    }
}
