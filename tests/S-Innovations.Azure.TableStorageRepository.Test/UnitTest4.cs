//using System;
//using Microsoft.VisualStudio.TestTools.UnitTesting;
//using System.Linq.Expressions;

//namespace SInnovations.Azure.TableStorageRepository.Test
//{
//    [TestClass]
//    public class UnitTest4
//    {
//        private class testc
//        {

//        }
//        [TestMethod]
//        public void TestMethod1()
//        {

//            var obj = new Tuple<string, string>("hej", "med");
//            test<Tuple<string, string>,object>(m => new { m.Item1, m.Item2 });

//            var entityType = typeof(testc);
//          //  var lazy = typeof(Lazy<>).MakeGenericType(entityType);E
//          ////  var func = LambdaExpression.Lambda(Expression..Constant(GetValue())).Compile();
//          //  var func = Delegate.CreateDelegate(typeof(Func<testc>), this, "GetValue");
            
//          //  var a = Activator.CreateInstance(lazy, new Object[]{ func }) as Lazy<testc>;
//          //  test2(a);
//           // var b = new Lazy<Tuple<string,string>>()

//        }

//        private testc GetValue()
//        {
//            throw new Exception("test");
//            return new testc();
//        }
//        private void test2(Lazy<testc> obj){

//        }
//        private void test<T,T1>( Expression<Func<T, T1>> expression)
//        {

//            var expre = expression.Body as NewExpression;


//        }
//    }
//}
