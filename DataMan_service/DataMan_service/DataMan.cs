using System;
using System.Collections.Generic;
using System.ComponentModel;
using System.Data;
using System.Diagnostics;
using System.Linq;
using System.ServiceProcess;
using System.Text;
using System.Threading.Tasks;
using System.Net;
using System.Threading;
using System.IO;
using Npgsql;
using Newtonsoft.Json;
using Cognex.DataMan.SDK;
using Cognex.DataMan.SDK.Discovery;
using Cognex.DataMan.SDK.Utils;


namespace DataMan_service{
    public partial class DataMan : ServiceBase{

        private struct LineStruct{
            public bool Active;
            public int Timeout;
            public string Name;
            public string IPAdress;
            public string Type;
            public Nullable<DateTime> DeleteAt;
            public int RecconectAtempt; //Number of reconnect to try
            public int RecconectDelay;  //Delay between attempt to recconect(sec*5)

            [JsonIgnore]
            public string State;
            public DataManSystem _system;
            public ResultCollector _rc;
            public bool CoonectionProgress;
        }

        private struct Parameters{
            public int Rest_ListenPort;
            public int MaxThreads;
            public int RediscoveryTime; //Rediscovery every (sec); <= 0 - to disable
            public int RecconectAtempt; //Number of reconnect to try
            public int RecconectDelay;  //Delay between attempt to recconect(sec*5)
            public bool Logging;        //Enable log
        }

        private class SQL_Con{
            public string Host;
            public string User;
            public string Pass;
            public string Path;

            [JsonIgnore]
            public bool Active;

            private NpgsqlConnection conn;

            public SQL_Con(){
                Active = false;
            }

            ~SQL_Con(){
                if (Active) conn.Close();
            }

            public void Open(){
                string connStr = "Host=" + Host + ";" +
                    "Username=" + User + ";" +
                    "Password=" + Pass + ";" +
                    "Database=" + Path + ";";
                try{
                    conn = new NpgsqlConnection(connStr);
                    conn.Open();
                    Active = true;
                }
                catch { Active = false; }
            }

            public void WriteLine(LineStruct _line){
                if (Active){
                    string a = "";
                    if (_line.Active) a = "True";
                    else a = "False";

                    string del = "";
                    if (_line.DeleteAt == null) del = "null";
                    else del = _line.DeleteAt.ToString();

                    string query = "INSERT INTO public.\"Lines\" VALUES(DEFAULT, " +
                        a + ", " +
                        _line.Timeout.ToString() + ", " +
                        "'" + _line.Name + "', " +
                        "'" + _line.IPAdress + "', " +
                        "'" + _line.Type + "', " +
                        del +
                        ")";

                    var cmd = new NpgsqlCommand(query, conn);
                    NpgsqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read()) { };
                    reader.Close();
                }
            }

            public void ReadLines(ref List<LineStruct> _listlines, ref Logger lg){
                if (Active){

                    lg.write("Starting read lines.");

                    string query = "SELECT * FROM public.\"Lines\" ORDER BY 1";
                    var cmd = new NpgsqlCommand(query, conn);
                    NpgsqlDataReader reader = cmd.ExecuteReader();
                    while (reader.HasRows){
                        while (reader.Read()){
                            LineStruct _line = new LineStruct();

                            _line.Active = reader.GetBoolean(1);
                            _line.Timeout = reader.GetInt32(2);
                            _line.Name = reader.GetString(3);
                            _line.IPAdress = reader.GetString(4);
                            _line.Type = reader.GetString(5);

                            var ts = reader.GetValue(6);
                            if (ts.GetType().Name == "DBNull") _line.DeleteAt = null;
                            else _line.DeleteAt = (DateTime)reader.GetValue(6);

                            _listlines.Add(_line);
                        }
                        reader.NextResult();
                    }
                    reader.Close();
                }

                //lg.write("Read lines\n" + JsonConvert.SerializeObject(_listlines, Formatting.Indented, new JsonSerializerSettings { }));
                lg.write("Read lines");
            }

            public void Write(string sysname, string result){
                if (Active){
                    //INSERT INTO public."Codes" VALUES(DEFAULT, '11/3/2021 16:10','123456789')

                    result = result.Replace("\\", "\\\\");
                    //result = result.Replace("%", "\\%");
                    result = result.Replace("\'", "\\\'");
                    string query = "INSERT INTO public.\"Codes\" VALUES(DEFAULT, '" + DateTime.Now.ToString() + "', E'" + result + "', '" + sysname + "', '" + DateTime.Now.ToString() + "')";

                    var cmd = new NpgsqlCommand(query, conn);
                    NpgsqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read()) { };
                    reader.Close();
                }
            }
            public void test(string read_result){
                if (Active){
                    //read_result = read_result.Replace("%", "\\%");
                    //read_result = read_result.Replace("\'", "\\\'");


                    string query = "INSERT INTO public.\"Codes\" VALUES((E'" + read_result + "'), DEFAULT)";

                    var cmd = new NpgsqlCommand(query, conn);
                    NpgsqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read()) { };
                    reader.Close();
                }
            }
        }

        #region ListenerConfig
        private readonly HttpListener _listener;
        private readonly Thread _listenerThread;
        private readonly ManualResetEvent _stop, _idle;
        private Semaphore _busy;
        #endregion

        private SQL_Con _sql = new SQL_Con();
        private List<LineStruct> _lines = new List<LineStruct>();
        private Parameters _param = new Parameters();

        private EthSystemDiscoverer _ethSystemDiscoverer = null;
        private SerSystemDiscoverer _serSystemDiscoverer = null;

        System.Timers.Timer _timer;
        System.Timers.Timer _timerRecon;
        System.Timers.Timer _timerC;

        int findlines = 0;

        private Logger lg;

        #region ConfigFile
        private void ReadConfig(){
            StreamReader file = File.OpenText("c:\\DataMan\\config.ini");
            string conf;

            var serializer = new JsonSerializer();
            int part = 0;
            string INIParam = "";
            string INISQL = "";

            while ((conf = file.ReadLine()) != null){
                switch (conf){
                    case "[SQL]":
                        part = 1;
                        break;

                    case "[PARAMETERS]":
                        part = 2;
                        break;

                    default:
                        switch (part){
                            case 1:
                                INISQL = INISQL + conf + "\n";
                                break;
                            case 2:
                                INIParam = INIParam + conf + "\n";
                                break;
                        }

                        break;
                }
            }

            file.Close();

            _sql = JsonConvert.DeserializeObject<SQL_Con>(INISQL);
            _param = JsonConvert.DeserializeObject<Parameters>(INIParam);

            if (_param.Rest_ListenPort == 0) _param.Rest_ListenPort = 8080;
            if (_param.MaxThreads == 0) _param.MaxThreads = 5;
        }

        /*
        private void SaveConfig(){

            StreamWriter file = File.CreateText("c:\\DataMan\\config.ini");
            file.WriteLine("[PARAMETERS]");
            string js = JsonConvert.SerializeObject(_param, Formatting.Indented, new JsonSerializerSettings { });
            file.WriteLine(js);

            file.WriteLine("[SQL]");
            js = JsonConvert.SerializeObject(_sql, Formatting.Indented, new JsonSerializerSettings { });
            file.WriteLine(js);

            file.WriteLine("[LINES]");
            js = JsonConvert.SerializeObject(_lines, Formatting.Indented, new JsonSerializerSettings { });
            file.WriteLine(js);

            file.Close();
           
        }
        */
        #endregion

        public DataMan(){
            InitializeComponent();

            /*
            _stop = new ManualResetEvent(false);
            _idle = new ManualResetEvent(false);
            _listener = new HttpListener();
            _listenerThread = new Thread(HandleRequests);
            */
            //_syncContext = SynchronizationContext.Current;
        }

        protected override void OnStart(string[] args){
            ReadConfig();
            /*
            _param.Logging = true;
            _param.RediscoveryTime = 300;
            _param.RecconectAtempt = 3;
            _param.RecconectDelay = 60;

            StreamWriter file = File.CreateText("c:\\DataMan\\config.tmp");
            file.WriteLine("[PARAMETERS]");
            string js = JsonConvert.SerializeObject(_param, Formatting.Indented, new JsonSerializerSettings { });
            file.WriteLine(js);

            file.WriteLine("[SQL]");
            js = JsonConvert.SerializeObject(_sql, Formatting.Indented, new JsonSerializerSettings { });
            file.WriteLine(js);

            file.Close();
            */

            lg = new Logger(_param.Logging);

            if (_param.RediscoveryTime > 0) {
                _timer = new System.Timers.Timer(_param.RediscoveryTime * 1000);
                _timer.Elapsed += RediscoveryTimer;
                _timer.AutoReset = true;
                _timer.Enabled = true;
            }

            _timerRecon = new System.Timers.Timer(_param.RediscoveryTime * 5000);
            _timerRecon.Elapsed += ReconnectTimer;
            _timerRecon.AutoReset = true;
            _timerRecon.Enabled = true;

            _timerC = new System.Timers.Timer(1000);
            _timerC.Elapsed += CTimer;
            _timerC.AutoReset = true;
            _timerC.Enabled = true;

            _sql.Open();
           // _sql.test("123'abcd%efg'%hijk");

            _sql.ReadLines(ref _lines, ref lg);

            // Create discoverers to discover ethernet and serial port systems.
            _ethSystemDiscoverer = new EthSystemDiscoverer();
            _serSystemDiscoverer = new SerSystemDiscoverer();

            // Subscribe to the system discoved event.
            _ethSystemDiscoverer.SystemDiscovered += new EthSystemDiscoverer.SystemDiscoveredHandler(OnEthSystemDiscovered);
            _serSystemDiscoverer.SystemDiscovered += new SerSystemDiscoverer.SystemDiscoveredHandler(OnSerSystemDiscovered);

            _systemDiscovery();

            /*
            _listener.Prefixes.Add(String.Format(@"http://+:{0}/", _param.Rest_ListenPort));
            _listener.Start();
            _listenerThread.Start();
             */

            lg.write("Service started.");
        }

        protected override void OnStop(){
            for (int x=0; x < _lines.Count; x++){
                if ((_lines[x]._system!=null)&& (_lines[x]._system.State == Cognex.DataMan.SDK.ConnectionState.Connected)){
                    _lines[x]._system.Disconnect();
                }
            }


            if (_param.RediscoveryTime > 0){
                _timer.Dispose();
            }
            //_sql.

            /*
            _stop.Set();
            _listenerThread.Join();
            _idle.Reset();

            //семафоры
            _busy.WaitOne();
            if (_param.MaxThreads != 1 + _busy.Release())
                _idle.WaitOne();

            _listener.Stop();
            */

            lg.write("Service stoped. Goodbye.");
        }

        #region ListenerService
        private void onStart(object sender, EventArgs e){
            _listener.Prefixes.Add(String.Format(@"http://+:{0}/", _param.Rest_ListenPort));
            _listener.Start();
            _listenerThread.Start();
        }

        private void onStop(object sender, EventArgs e){
            _stop.Set();
            _listenerThread.Join();
            _idle.Reset();

            //aquire and release the semaphore to see if anyone is running, wait for idle if they are.
            _busy.WaitOne();
            if (_param.MaxThreads != 1 + _busy.Release())
                _idle.WaitOne();

            _listener.Stop();
        }

        private void HandleRequests(){
            while (_listener.IsListening)
            {
                var context = _listener.BeginGetContext(ListenerCallback, null);

                if (0 == WaitHandle.WaitAny(new[] { _stop, context.AsyncWaitHandle }))
                    return;
            }
        }

        private void ListenerCallback(IAsyncResult ar){
            _busy.WaitOne();
            try
            {
                HttpListenerContext context;
                try { context = _listener.EndGetContext(ar); }
                catch (HttpListenerException) { return; }

                if (_stop.WaitOne(0, false))
                    return;

                //Console.WriteLine("{0} {1}", context.Request.HttpMethod, context.Request.RawUrl);
                //context.Response.SendChunked = true;
                /*
                               using (TextWriter tw = new StreamWriter(context.Response.OutputStream)){
                                   tw.WriteLine("<html><body><h1>Hello World</h1>");
                                   for (int i = 0; i < 5; i++){
                                       tw.WriteLine("<p>{0} @ {1}</p>", i, DateTime.Now);
                                       tw.Flush();
                                       Thread.Sleep(1000);
                                   }
                                   tw.WriteLine("</body></html>");
                               }
               */
            }
            finally
            {
                if (_param.MaxThreads == 1 + _busy.Release())
                    _idle.Set();
            }
        }
        #endregion


        #region Device Discovery Events
         private void RediscoveryTimer(object sender, EventArgs e) {
            //GC.Collect(); //garbage force
            _systemDiscovery();
        }

        private void ReconnectTimer(object sender, EventArgs e){
            for (int x = 0; x < _lines.Count; x++){
                if ((_lines[x].Active) && (_lines[x]._system != null) && (_lines[x].RecconectDelay > 0)){
                    LineStruct _line = _lines[x];

                    _line.RecconectDelay--;
                    if (_line.RecconectDelay == 0) {
                        _line.CoonectionProgress = true;

                        findlines++;
                        _line._system.Connect();

                        if (_line.RecconectAtempt > 0){
                            _line.RecconectAtempt--;
                            _line.RecconectDelay = _param.RecconectDelay;
                        }

                    }

                    _lines[x] = _line;

                }
            }
        }

        private void _systemDiscovery(){
            _ethSystemDiscoverer.Discover();
            _serSystemDiscoverer.Discover();
        }

        private void DataManAdd(object system_info){
            try{
                string Addr = "";
                int x;

                ISystemConnector _connector = null;

                if (system_info is EthSystemDiscoverer.SystemInfo){
                    EthSystemDiscoverer.SystemInfo eth_system_info = system_info as EthSystemDiscoverer.SystemInfo;
                    EthSystemConnector conn = new EthSystemConnector(eth_system_info.IPAddress, eth_system_info.Port);

                    Addr = eth_system_info.IPAddress.ToString();
                    conn.UserName = "admin";
                    conn.Password = "";

                    _connector = conn;
                }
                else if (system_info is SerSystemDiscoverer.SystemInfo){
                    SerSystemDiscoverer.SystemInfo ser_system_info = system_info as SerSystemDiscoverer.SystemInfo;
                    SerSystemConnector conn = new SerSystemConnector(ser_system_info.PortName, ser_system_info.Baudrate);

                    Addr = ser_system_info.PortName.ToString();
                    _connector = conn;
                }

                for (x = 0; x < _lines.Count; x++){
                    if (_lines[x].IPAdress == Addr) break;
                }

                LineStruct _line;
                if (x == _lines.Count){
                    _line = new LineStruct();
                    _line.IPAdress = Addr;
                    _line.Active = true;
                    _line.Timeout = 5000;
                    _line.Type = "self";

                    _line.State = "New";

                    _sql.WriteLine(_line);

                    _lines.Add(_line);
                    lg.write("Add new line " + _lines[x].IPAdress);
                }
                else {
                    _line = _lines[x];
                    //lg.write("Find active line\n" + JsonConvert.SerializeObject(_lines[x], Formatting.Indented, new JsonSerializerSettings { }));
                    lg.write("Find active line " + _lines[x].IPAdress);
                }

                //if (_line._system.State == Cognex.DataMan.SDK.ConnectionState.Connected) return;
                if (_line.State == "Connected") return;

                _line._system = new Cognex.DataMan.SDK.DataManSystem(_connector);
                _line._system.DefaultTimeout = _line.Timeout;

                // Subscribe to events that are signalled when the system is connected / disconnected.
                _line._system.SystemConnected += new Cognex.DataMan.SDK.SystemConnectedHandler(OnSystemConnected);
                _line._system.SystemDisconnected += new Cognex.DataMan.SDK.SystemDisconnectedHandler(OnSystemDisconnected);
                _line._system.SystemWentOnline += new Cognex.DataMan.SDK.SystemWentOnlineHandler(OnSystemWentOnline);
                _line._system.SystemWentOffline += new Cognex.DataMan.SDK.SystemWentOfflineHandler(OnSystemWentOffline);
                _line._system.KeepAliveResponseMissed += new Cognex.DataMan.SDK.KeepAliveResponseMissedHandler(OnKeepAliveResponseMissed);
                /*               
               _system.BinaryDataTransferProgress += new Cognex.DataMan.SDK.BinaryDataTransferProgressHandler(OnBinaryDataTransferProgress);
               */

                // Subscribe to events that are signalled when the deveice sends auto-responses.
                Cognex.DataMan.SDK.ResultTypes requested_result_types = Cognex.DataMan.SDK.ResultTypes.ReadString;
                _line._rc = new ResultCollector(_line._system, requested_result_types);
                _line._rc.ComplexResultArrived += Results_ComplexResultArrived;
                //_line._rc.PartialResultDropped += Results_PartialResultDropped;

                _line._system.SetKeepAliveOptions(true, 3000, 1000);

                if (_line.Active){
                    _line.RecconectAtempt = _param.RecconectAtempt;
                    _line.RecconectDelay = _param.RecconectDelay;

                    _line.CoonectionProgress = true;
                    findlines++;
                    _line._system.Connect();
                    lg.write("Starting connect to line.");
                }
                else _line.State = "Disabled";

                try{
                    _line._system.SetResultTypes(requested_result_types);
                }
                catch { }

                _lines[x] = _line;
            }
            catch { }
        }

        private void OnEthSystemDiscovered(EthSystemDiscoverer.SystemInfo systemInfo){
            DataManAdd(systemInfo);
        }

        private void OnSerSystemDiscovered(SerSystemDiscoverer.SystemInfo systemInfo){
            DataManAdd(systemInfo);
        }

        #endregion

        #region Device Events

        private void CTimer(object sender, EventArgs e){
            if (findlines > 0) OnSystemConnected(null, null);
        }

        private void OnSystemConnected(object sender, EventArgs args){
            //lg.write("Connected sender:" + sender.ToString() +" arg:" + args.ToString());

            for (int x = 0; x < _lines.Count; x++){
                if ((_lines[x].CoonectionProgress) && (_lines[x]._system.State == Cognex.DataMan.SDK.ConnectionState.Connected)){
                    LineStruct _line = _lines[x];
                    _line.State = "Connected";

                    findlines--;

                    _line.RecconectAtempt = 0;
                    _line.RecconectDelay = 0;

                    switch (_line.Type){
                        case "single":
                            _line._system.SendCommand("SET TRIGGER.TYPE 0");
                            break;
                        case "presentation":
                            _line._system.SendCommand("SET TRIGGER.TYPE 1");
                            break;
                        case "manual":
                            _line._system.SendCommand("SET TRIGGER.TYPE 2");
                            break;
                        case "burst":
                            _line._system.SendCommand("SET TRIGGER.TYPE 3");
                            break;
                        case "self":
                            _line._system.SendCommand("SET TRIGGER.TYPE 4");
                            break;
                        case "continuous":
                            _line._system.SendCommand("SET TRIGGER.TYPE 5");
                            break;
                        default:
                            _line._system.SendCommand("SET TRIGGER.TYPE 4");
                            _line.Type = "self";
                            break;
                    }

                    _line._system.SendCommand("TRIGGER ON");

                    _line.CoonectionProgress = false;

                    lg.write("Connected line " + _line.Name);

                    _lines[x] = _line;
                }
            }



        }

        private void OnSystemDisconnected(object sender, EventArgs args){
            for (int x = 0; x < _lines.Count; x++){
                if (_lines[x]._system == (DataManSystem)sender){
                    LineStruct _line = _lines[x];
                    _line.State = "Disconnected";
                    _lines[x] = _line;

                    lg.write("Disconnected line " + _line.Name);

                    break;
                }
            } 
        }

        private void OnKeepAliveResponseMissed(object sender, EventArgs args){
            for (int x = 0; x < _lines.Count; x++){
                if (_lines[x]._system == (DataManSystem)sender){
                    LineStruct _line = _lines[x];
                    _line.State = "Keep-alive response missed";

                    _line.RecconectAtempt = _param.RecconectAtempt;
                    _line.RecconectDelay = _param.RecconectDelay;

                    _lines[x] = _line;
                break;
                }
            }
        }

        private void OnSystemWentOnline(object sender, EventArgs args){
            for (int x = 0; x < _lines.Count; x++){
                if (_lines[x]._system == (DataManSystem)sender){
                    LineStruct _line = _lines[x];
                    _line.State = "Went online";
                    _lines[x] = _line;
                    break;
                }
            } 
        }

        private void OnSystemWentOffline(object sender, EventArgs args){
            for (int x = 0; x < _lines.Count; x++){
                if (_lines[x]._system == (DataManSystem)sender){
                    LineStruct _line = _lines[x];
                    _line.State = "Went offline";
                    _lines[x] = _line;
                    break;
                }
            }
        }
        #endregion

        /*
        void ReportDroppedResult(ResultInfo e){
            List<string> dropped = new List<string>();

            if (e.ReadString != null){
                dropped.Add(String.Format("read string (ResultId={0})", e.ResultId));
            }
            if (e.XmlResult != null) dropped.Add(String.Format("xml result (ResultId={0})", e.ResultId));
        }

        /*
        void Results_PartialResultDropped(object sender, ResultInfo e){
            if (e.SubResults != null){
                foreach (var sub_result in e.SubResults){
                    ReportDroppedResult(sub_result);
                }
            }
            ReportDroppedResult(e);
        }
        */
        /*
        private void ShowResult(string sysname, string read_result){
            string read_result = "";

            object _currentResultInfoSyncLock = new object();
            lock (_currentResultInfoSyncLock){
                if (!String.IsNullOrEmpty(e.ReadString)){

                    lg.write("Arrived data " + read_result + " {" + sysname + "}");
                    /*
                    read_result = e.ReadString;
                    //

                    read_resuArrived datalt = read_result.Replace("%", "\\%");
                    //read_result = read_result.Replace("\\", "\\\\");
                    //read_result = read_result.Replace("\'", "\\\'");

                    //read_result = read_result.Replace("%", "");
                    read_result = read_result.Replace("\\", "");
                    read_result = read_result.Replace("\'", "");

                    //INSERT INTO public."Codes" VALUES(DEFAULT, '11/3/2021 16:10','123456789')
                    string query = "INSERT INTO public.\"Codes\" VALUES(DEFAULT, '" + DateTime.Now.ToString() + "', '" + read_result + "', '" + sysname + "')";

                    var cmd = new NpgsqlCommand(query, conn);
                    NpgsqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read()) { };
                    reader.Close();

                    //read_result = read_result.Replace("\\", "\\\\");
                    //read_result = read_result.Replace("%", "\\%");
                    //read_result = read_result.Replace("\'", "\\\'");
                    //read_result = read_result.Replace("\\", "");
                    //read_result = read_result.Replace("%", "");
                    //read_result = read_result.Replace("\'", "");
                    _sql.Write(sysname, read_result);



                //}
            //}
        }
        */

        private void Results_ComplexResultArrived(object sender, ResultInfo e){
            string Addr = "";
            string read_result = e.ReadString;

            foreach (LineStruct _line in _lines){
                if (_line._rc == (ResultCollector)sender) {
                    Addr = _line.IPAdress;
                    lg.write("Arrived data " + read_result + " {" + Addr + "}");
                    _sql.Write(Addr, read_result);
                    break;
                }
            }
        }
    }
}
