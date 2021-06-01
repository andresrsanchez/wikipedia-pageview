using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using pageview_processor;
using System;

namespace pageview_processor_tests
{
    [TestClass]
    public class validate_dates_should
    {
        [TestMethod]
        public void fail_with_bad_dates()
        {
            (bool isValid, _, _) = DatesValidator.ValidateAndGet(
                WikipediaDumpsProcessor.FORMAT, "20202020-000000", "20202120-000000");
            isValid.Should().BeFalse();
        }

        [TestMethod]
        public void get_current_date_hour_if_null()
        {
            (bool isValid, _, _) = DatesValidator.ValidateAndGet(WikipediaDumpsProcessor.FORMAT, null, null);
            isValid.Should().BeTrue();
        }

        [TestMethod]
        public void get_correctly_dates()
        {
            var from = DateTime.UtcNow.AddHours(-6).ToString(WikipediaDumpsProcessor.FORMAT);
            var to = DateTime.UtcNow.ToString(WikipediaDumpsProcessor.FORMAT);

            (bool isValid, DateTime dateFrom, DateTime dateTo) = DatesValidator.ValidateAndGet(
                WikipediaDumpsProcessor.FORMAT, from, to);

            isValid.Should().BeTrue();

            var fromEquality = from.Split('-')[0]
                == dateFrom.ToString(WikipediaDumpsProcessor.FORMAT).Split('-')[0];
            var toEquality = to.Split('-')[0]
                == dateTo.ToString(WikipediaDumpsProcessor.FORMAT).Split('-')[0];

            fromEquality.Should().BeTrue();
            toEquality.Should().BeTrue();
        }
    }
}
