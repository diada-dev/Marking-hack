using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;
using System.IO;

namespace DataMan_service{
    class Logger{
        private StreamWriter f; 
        public bool logging;
        public Logger(bool b){
            logging = b;
            if (b) {
                f = File.AppendText("c:\\DataMan\\data.log");
                f.AutoFlush = true;
            }
        }

        public void write(string s){
            if (logging) {
                f.WriteLine(DateTime.Now.ToString() + ": " + s);
            }
        }
        ~Logger(){
            if (logging) {
                f.Close();
            }
        }
    }
}
