using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;
namespace TestAssessment
{
    class Program
    {
        
        static void Main(string[] args)
        {
           
            Controller controller = new Controller(@"C:\Users\motaz\Desktop\task\exhibitA-input.csv", "10/08/2016");
            // make the quary
            controller.quarySequential();
            // print the result
            Console.WriteLine(controller.ToString());

            Controller controller1 = new Controller(@"C:\Users\motaz\Desktop\task\exhibitA-input.csv", "10/08/2016");
            // make the quary
            controller1.quaryParallel();
            // print the result
            Console.WriteLine(controller1.ToString());
        }
    }
}
