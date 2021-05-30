using FluentAssertions;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using pageview_processor;
using System;

namespace pageview_processor_tests
{
    [TestClass]
    public class get_blacklist_of_wikipedia_dumps_should
    {
        [TestMethod]
        public void process_correctly()
        {
            var blackList = @"
zh.s Template:Annotate
zh.s User:Klangtao
zh.voy File:The_stage_in_Xue_Fucheng_Residence.jpg
zh.voy Wikivoyage:%E5%88%A0%E9%99%A4%E8%A1%A8%E5%86%B3
";
            var result = BlackListOfWikipediaDumps.Process(blackList);
            result.Should().HaveCount(2);

            result.TryGetValue("zh.s", out var zhs).Should().BeTrue();
            result.TryGetValue("zh.voy", out var zhvoy).Should().BeTrue();

            zhs.Contains("Template:Annotate").Should().BeTrue();
            zhs.Contains("User:Klangtao").Should().BeTrue();

            zhvoy.Contains("File:The_stage_in_Xue_Fucheng_Residence.jpg").Should().BeTrue();
            zhvoy.Contains("Wikivoyage:%E5%88%A0%E9%99%A4%E8%A1%A8%E5%86%B3").Should().BeTrue();
        }

        [TestMethod]
        public void process_correctly_with_line_endings()
        {
            var blackList = "zh.s Template:Annotate\r\nzh.s User:Klangtao";
            var result = BlackListOfWikipediaDumps.Process(blackList);
            result.Should().HaveCount(1);

            result.TryGetValue("zh.s", out var zhs).Should().BeTrue();

            zhs.Contains("Template:Annotate").Should().BeTrue();
            zhs.Contains("User:Klangtao").Should().BeTrue();
        }

        [TestMethod]
        public void fail_because_of_bad_line_format()
        {
            var blackList = @"zh.s Template:Annotate badlineone badlinetwo badlinethree";
            Action act = () => BlackListOfWikipediaDumps.Process(blackList);

            act.Should().Throw<Exception>();
        }
    }
}
