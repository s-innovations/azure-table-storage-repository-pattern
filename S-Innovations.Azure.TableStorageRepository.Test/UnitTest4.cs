using System;
using Microsoft.VisualStudio.TestTools.UnitTesting;
using System.Linq.Expressions;

namespace SInnovations.Azure.TableStorageRepository.Test
{
    [TestClass]
    public class UnitTest4
    {
        [TestMethod]
        public void TestMethod1()
        {

            var obj = new Tuple<string, string>("hej", "med");
            test<Tuple<string, string>,object>(m => new { m.Item1, m.Item2 });

        }
        private void test<T,T1>( Expression<Func<T, T1>> expression)
        {

            var expre = expression.Body as NewExpression;


        }
    }
}
