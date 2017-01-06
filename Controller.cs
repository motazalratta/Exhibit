using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.Diagnostics;

namespace TestAssessment
{
    class Controller
    {
        /// <summary>
        /// this Dictionary contains the result of quary. 
        /// the Key represents DISTINCT_PLAY_COUNT
        /// the value represents CLIENT_COUNT
        /// </summary>
        private Dictionary<int, int> output = new Dictionary<int, int>();
        /// <summary>
        /// the path of input file
        /// </summary>
        private string filename;
        /// <summary>
        /// the date for quary
        /// </summary>
        private string date;
        /// <summary>
        /// this array contains the lines of the input file after loading it
        /// </summary>
        private string[] lines;

        public Controller(string filename, string date)
        {
            this.filename = filename;
            this.date = date;
        }

        /// <summary>
        /// this function for loading the input's file and split it to lines
        /// </summary>
        private void loadFile()
        {
            // Get the file's text.
            string whole_file = System.IO.File.ReadAllText(filename);

            // Split into lines.
            lines = whole_file.Split(new char[] { '\n' },
                StringSplitOptions.RemoveEmptyEntries);
        }

        /// <summary>
        /// makes the first step in analyzing lines (in specific range in lines array)
        /// aggregates each client with all played songs without duplication
        /// </summary>
        /// 
        /// <param name="from">the start index in line array</param>
        /// <param name="to">the end index in line array</param> 
        /// 
        private Dictionary<int, HashSet<int>> DataAnalyse(int from, int to)
        {
            //intermediate Dictionary the key for client id and the value for played song ids
            Dictionary<int, HashSet<int>> internalDictionary = new Dictionary<int, HashSet<int>>();
            // for each line in range 
            for (int r = from; r < to; r++)
            {
                string[] columns = lines[r].Split('\t');
                // check the line's date 
                // we remove all \r because some lines without time so in this case the comparison failed
                // (for example the line '44BB191B015A3964E053CF0A000AB546	6082	743	10/08/2016\r')
                if (columns[3].Replace("\r", "").Split(' ')[0] != date)
                {
                    continue;
                }
                // add the song id to the HashSet of the client id
                InternalDictionaryAdd(internalDictionary, Int32.Parse(columns[2]), Int32.Parse(columns[1]));
            }
            return internalDictionary;
        }


        /// <summary>
        /// this method for adding a song id to the HashSet of the client id
        /// </summary>
        private void InternalDictionaryAdd(Dictionary<int, HashSet<int>> internalDictionary, int client, int song)
        {
            // if the client already added
            if (internalDictionary.ContainsKey(client))
            {
                HashSet<int> list = internalDictionary[client];
                list.Add(song);
            }
            else
            {
                HashSet<int> list = new HashSet<int>();
                list.Add(song);
                internalDictionary.Add(client, list);
            }
        }

        /// <summary>
        /// makes the second step in analyzing lines 
        /// for each distinct play count calc the number of clients
        /// </summary>
        private void BuildOutput(Dictionary<int, HashSet<int>> internalDictionary)
        {
            foreach (KeyValuePair<int, HashSet<int>> kvp in internalDictionary)
            {
                int playCount = kvp.Value.Count;
                if (output.ContainsKey(playCount))
                {
                    output[playCount]++;
                }
                else
                {
                    output.Add(playCount, 1);
                }
            }

        }
        /// <summary>
        /// print the output ictionary
        /// </summary>
        public override string ToString()
        {
            string display = "DISTINCT_PLAY_COUNT\tCLIENT_COUNT\n";
            foreach (KeyValuePair<int, int> kvp in output)
            {
                display = display + kvp.Key + "\t" + kvp.Value + "\n";
            }
            display = display+ "The maximum number of distinct songs played " + output.Keys.Max()+"\n";
            return display;
        }
        /// <summary>
        /// the quary in Sequential way
        /// </summary>
        public Dictionary<int, int> quarySequential()
        {

            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            loadFile();
            BuildOutput(DataAnalyse(0, lines.Length));

            stopWatch.Stop();

            Console.WriteLine("Sequential Time required: " + stopWatch.Elapsed);
            return output;
        }

        /// <summary>
        /// the quary in Parallel way
        /// </summary>
        public Dictionary<int, int> quaryParallel()
        {
            Stopwatch stopWatch = new Stopwatch();
            stopWatch.Start();

            loadFile();

            //divide the array to four equal tasks
            Task<Dictionary<int, HashSet<int>>> task1 = Task.Factory.StartNew<Dictionary<int, HashSet<int>>>(() => DataAnalyse(0, lines.Length / 4));
            Task<Dictionary<int, HashSet<int>>> task2 = Task.Factory.StartNew<Dictionary<int, HashSet<int>>>(() => DataAnalyse(lines.Length / 4, lines.Length / 2));
            Task<Dictionary<int, HashSet<int>>> task3 = Task.Factory.StartNew<Dictionary<int, HashSet<int>>>(() => DataAnalyse(lines.Length / 2, lines.Length / 2 + lines.Length / 4));
            Task<Dictionary<int, HashSet<int>>> task4 = Task.Factory.StartNew<Dictionary<int, HashSet<int>>>(() => DataAnalyse(lines.Length / 2 + lines.Length / 4, lines.Length));

            Task[] tasks = new Task[] {
               task1,task2,task3,task4
            };
            
            // wait all tasks to finish
            Task.WaitAll(tasks);
            
            // marge the result of tasks
            Dictionary<int, HashSet<int>> Merged = marge(new Dictionary<int, HashSet<int>>[] { task1.Result, task2.Result, task3.Result, task4.Result });

            BuildOutput(Merged);

            stopWatch.Stop();
            Console.WriteLine("Parallel Time required: " + stopWatch.Elapsed);
            return output;
        }
        /// <summary>
        /// marges internal dictionary 
        /// this method used only in parallel way
        /// </summary>
        public Dictionary<int, HashSet<int>> marge(Dictionary<int, HashSet<int>>[] list)
        {
            Dictionary<int, HashSet<int>> Merged = new Dictionary<int, HashSet<int>>();
            foreach (var item in list)
            {
                foreach (KeyValuePair<int, HashSet<int>> TaskResult in item)
                {
                    if (Merged.ContainsKey(TaskResult.Key))
                    {
                        HashSet<int> list1 = Merged[TaskResult.Key];
                        list1.UnionWith(TaskResult.Value);
                    }
                    else
                    {
                        Merged.Add(TaskResult.Key, TaskResult.Value);
                    }
                }
            }
            return Merged;
        }

    }
}
