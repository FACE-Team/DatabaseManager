using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading;
using MySql.Data.MySqlClient;
using System.Configuration;
using System.Timers;
using System.Diagnostics;

using YarpManagerCS;

namespace DataBaseManager
{
    class Program
    {
       static protected MySqlConnection connection;

        static private string server = ConfigurationManager.AppSettings["ServerMysql"].ToString();
        static private string db = ConfigurationManager.AppSettings["database"].ToString();
        static private string user = ConfigurationManager.AppSettings["user"].ToString();
        static private string psw = ConfigurationManager.AppSettings["psw"].ToString();

        static private YarpPort yarpPortCommand;
        static private YarpPort yarpPortReply;
        static private YarpPort yarpPortStatus;
        static private System.Threading.Thread senderThread = null;
        static private System.Timers.Timer yarpReceiverCommand;
        static private string receiveCommandString = "";
        static private string status = "Activo";
        static private string stringReply = "";


        static private bool pauseKeyRead = false;

        static void Main(string[] args)
        {
            var dllDirectory = System.IO.Path.GetDirectoryName(System.Reflection.Assembly.GetExecutingAssembly().Location) + "/lib";
            Environment.SetEnvironmentVariable("PATH", Environment.GetEnvironmentVariable("PATH") + ";" + dllDirectory);

            InitYarp();
         
            connectionDb();

            if (connection != null)
                connection.Open();

          

            if (connection != null)
            {
                printMenu();
            }

            ConsoleKeyInfo cki;
            do
            {

               
                    cki = Console.ReadKey(false); // show the key as you read it

                    if (!pauseKeyRead)
                    {
                        switch (cki.KeyChar.ToString())
                        {
                            case "c":
                                closeConnection();
                                break;
                            case "v":
                                listTables();
                                printMenu();
                                break;
                            case "q":
                                pauseKeyRead = true;
                                getCommand();
                                printMenu();
                                break;
                        }

                    }

            } while (cki.Key != ConsoleKey.Escape);


         
        }


        static private void InitYarp()
        {
            #region define port


            yarpPortCommand = new YarpPort();
            yarpPortCommand.openInputPort("/DataBaseManager/Command:i");

            yarpPortReply = new YarpPort();
            yarpPortReply.openSender("/DataBaseManager/Reply:o");

            yarpPortStatus = new YarpPort();
            yarpPortStatus.openSender("/DataBaseManager/Status:o");


            #endregion

            #region define timer or thread

            senderThread = new System.Threading.Thread(SendData);
            senderThread.Start();


            yarpReceiverCommand = new System.Timers.Timer();
            yarpReceiverCommand.Interval = 200;
            yarpReceiverCommand.Elapsed += new ElapsedEventHandler(ReceiveCommand);
            yarpReceiverCommand.Start();


            #endregion

        

        }

        

       

        static void ReceiveCommand(object sender, ElapsedEventArgs e)
        {
            if (status == "Activo")
            {
                yarpPortCommand.receivedData(out receiveCommandString);

                if (receiveCommandString != null && receiveCommandString != "")
                {
                    try
                    {
                        status = "Occupied";
                        Console.WriteLine(receiveCommandString);//check winner data

                        ThreadPool.QueueUserWorkItem(ExecuteQuery); 
                     
                    }
                    catch (Exception exc)
                    {
                        Console.WriteLine("Error XML : " + exc.Message);
                    }


                }
            }
        }

        static void connectionDb() 
        {
            string connectionString = "SERVER=" + server + ";" + "DATABASE=" + db + ";" + "UID=" + user + ";" + "PASSWORD=" + psw + ";";
            connection = new MySqlConnection(connectionString);
        }


        static private void ExecuteQuery(object state)  // Seçili olan tablodaki person_oid leri çekmek için fonksiyon
        {

            stringReply = "";

            MySqlCommand command = connection.CreateCommand();
            command.CommandText = receiveCommandString.Replace('"', ' ');
            try
            {
                MySqlDataReader Reader = command.ExecuteReader();
                
                stringReply = "|";
                while (Reader.Read())
                {
                    for (int i = 0; i < Reader.FieldCount; i++)
                        stringReply += Reader.GetValue(i).ToString() + " | ";

                }

                Reader.Close();
                Console.WriteLine(stringReply);
                Console.WriteLine();

                Console.WriteLine("Num port: " + yarpPortReply.getOutputConnectionCount());

            
            }
            catch (Exception ex) 
            {
                Console.WriteLine(ex.Message);
            }

        
            Console.WriteLine(status);
        }

        static private void SendData()
        {
            while (true)
            {

                if (yarpPortReply.getOutputConnectionCount() == 1)
                {
                    yarpPortReply.sendData(stringReply);
                    yarpReceiverCommand.Stop();
                }
                else if (status != "Activo" && yarpPortReply.getOutputConnectionCount() == 0)
                {
                    status = "Activo";
                    printMenu();
                    yarpReceiverCommand.Start();
                }

                yarpPortStatus.sendData(status);

            }
        }

        static private void listTables()
        {

            MySqlCommand cmd = connection.CreateCommand();
            cmd.CommandText = "SHOW TABLES";
            Console.WriteLine();
            Console.WriteLine(cmd.CommandText + "\n");

            try
            {
                if (connection != null)
                {
                    MySqlDataReader reader = cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        Console.Write("| ");
                        for (int i = 0; i < reader.FieldCount; i++)
                            Console.Write(reader.GetValue(i).ToString() + " | ");

                        Console.WriteLine();
                    }
                    reader.Close();
                }
                else
                    Console.WriteLine("Connection is close \n");

            }
            catch (MySqlException ex)
            {
                Console.WriteLine(ex.Number.ToString());
                Console.WriteLine(ex.Message);
            }

        }

        static private void getCommand() 
        {
            ConsoleKeyInfo keyinfo;

            Console.WriteLine("\n Write query\n");

            string query = Console.ReadLine();
            do
            {
                keyinfo = Console.ReadKey(false); // show the key as you read it

            } while (keyinfo.Key != ConsoleKey.Enter);

            if (query != "")
                ExecuteQuery(query);

            pauseKeyRead = false;
        }

        static void closeConnection() 
        {
            if (connection != null)
            {
                try
                {
                    connection.Clone();
                    connection = null;
                    Console.WriteLine("Connection is close \n");
                }
                catch (MySqlException ex)
                {
                    Console.WriteLine(ex.Message);
                    
                }
            }
            else
                Console.WriteLine("Connection is close \n");
        }

        static void printMenu() 
        {
            Console.WriteLine("\n Connection with Database is open\n");
            Console.WriteLine("\n\t v- View list Tables");
            Console.WriteLine("\n\t q- Write and Executive query");

            Console.WriteLine("\n\t c- Close connection");
            Console.WriteLine("\n\t esc.- Close application");
            Console.WriteLine();
        }
    }
}
