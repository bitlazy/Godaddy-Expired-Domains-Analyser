using System;
using System.Collections.Generic;
using System.Linq;
using System.IO;
using System.Net;
using System.Text;
using System.Collections;
using Ionic.Zip;
using System.Data;
using System.Data.Sql;
using System.Data.SqlClient;
using System.Threading;
using System.Threading.Tasks;
using System.Diagnostics;

namespace GodaddyAnalyser
{
    class Program
    {
        public static DataSet DomainDataSet = new DataSet();
        public static DataSet mozDomainDataSet = new DataSet();
        public static string proxyname = "";

        public static string[] ProxyFile = File.ReadAllLines(@"C:\GoDaddyFiles\Proxies.txt", Encoding.UTF8);

        public static SqlConnection dbCon = new SqlConnection(@"Data Source=localhost\sqlexpress;Initial Catalog=EXPIRED_DOMAINS;Integrated Security=True");
        public static string dynamicCon = @"Data Source=localhost\sqlexpress;Initial Catalog=EXPIRED_DOMAINS;Integrated Security=True";
        public static List<string> ProxyList = new List<string>();
        public static string[] domainarray;
        public static List<string> domainlist = new List<string>();

        static void Main(string[] args)
        {
            //Define Landing & exraction locations
            string LandingFolder = @"C:\GoDaddyFiles\Landing\";
            string ExtractFolder = @"C:\GoDaddyFiles\Extract\";

            //Get confirmation on file download
            Console.WriteLine("Do you want to download files from GD? (Press 1 - Yes : Any other Key - No)");
            string option = Console.ReadLine();

            //If file needs to be downloaded
            if (option == "1")
            {
                try
                {
                    //Declare a array list and pass all the file names that needs to be downloaded
                    ArrayList FileNames = new ArrayList();
                    FileNames.Add(@"tdnam_all_no_adult_listings.csv.zip");
                    FileNames.Add(@"tdnam_all_no_adult_listings2.csv.zip");
                    FileNames.Add(@"tdnam_all_no_adult_listings3.csv.zip");

                    //Create Landing folder if not exists
                    if (!(System.IO.Directory.Exists(LandingFolder)))
                        System.IO.Directory.CreateDirectory(LandingFolder);

                    //Create Extract folder if not exists
                    if (!(System.IO.Directory.Exists(ExtractFolder)))
                        System.IO.Directory.CreateDirectory(ExtractFolder);



                    //Call the DownloadFiles function to download the above mentioned files
                    foreach (string godaddyFile in FileNames)
                    {
                        Console.WriteLine("Download " + godaddyFile + " : In Progress");
                        int result = DownloadFiles(godaddyFile, LandingFolder);
                        if (result == 0)
                        {
                            Console.WriteLine("Download " + godaddyFile + " : Completed");
                            Console.WriteLine("Extract " + godaddyFile + " : In Progress");
                            using (ZipFile zip1 = ZipFile.Read(LandingFolder + godaddyFile))
                            {
                                // here, we extract every entry, but we could extract conditionally
                                // based on entry name, size, date, checkbox status, etc.  
                                foreach (ZipEntry e in zip1)
                                {
                                    e.Extract(ExtractFolder, ExtractExistingFileAction.OverwriteSilently);
                                }
                            }
                            Console.WriteLine("Extract " + godaddyFile + " : Completed");
                        }
                        else
                            Console.WriteLine("Download " + godaddyFile + " : Failed");
                    }

                    //Get the current PST time
                    TimeZoneInfo timeZoneInfo = TimeZoneInfo.FindSystemTimeZoneById("Pacific SA Standard Time");
                    DateTime PSTDateTime = TimeZoneInfo.ConvertTime(DateTime.Now, timeZoneInfo);


                    //Build the commands -- Dont execute now
                    SqlCommand truncateStage = new SqlCommand(@"TRUNCATE TABLE Godaddy_Auctions_STAGE", dbCon);
                    SqlCommand purgeDeadStage = new SqlCommand(@"DELETE FROM Godaddy_Auctions_STAGE WHERE CAST(SUBSTRING(TIMELEFT,1,19) AS DATETIME)<'" + PSTDateTime.ToString() + "'", dbCon);
                    SqlCommand StageToProd = new SqlCommand(@"EXEC SP_STAGETOPROD", dbCon);

                    //Open the DB connection
                    dbCon.Open();
                    //Truncate the Staging tables
                    truncateStage.ExecuteNonQuery();
                    Console.WriteLine("Truncate Staging Table : Completed");

                    //Read all the file names present in the extraction folder
                    string[] fileEntries = Directory.GetFiles(ExtractFolder);
                    Console.WriteLine("Import Files into Staging Table : Started");

                    //Bulk insert all the files into staging tables
                    foreach (string file in fileEntries)
                    {

                        SqlCommand BulkInsert = new SqlCommand(@"BULK INSERT Godaddy_Auctions_STAGE FROM '" + file + "' WITH ( FIRSTROW = 2,FIELDTERMINATOR = ',',ROWTERMINATOR = '\n',TABLOCK)", dbCon);
                        BulkInsert.ExecuteNonQuery();
                    }
                    Console.WriteLine("Import Files into Staging Table : Completed");

                    //Delete domains that are already expired
                    purgeDeadStage.ExecuteNonQuery();
                    Console.WriteLine("Delete dead records from Staging Table : Completed");

                    //Call the StageToProd stored proc to load the data from Staging to prod
                    Console.WriteLine("Transform From Stage to Prod : Started");
                    StageToProd.CommandTimeout = 300;
                    StageToProd.ExecuteNonQuery();
                    Console.WriteLine("Transform From Stage to Prod : Completed");

                    //Close the connection
                    dbCon.Close();

                }
                //If any exception occurs, catch it and throw
                catch (Exception ex)
                {
                    Console.WriteLine(ex.Message.ToString());
                }
            }



             //If skip file download and start PR check is selected
            
                /////////// PAGE RANK CHECK STARTS/////////////////////////////////////////

                Console.WriteLine("Page Rank Check: Started");

                //Select the list of lastly inserted domains that has PR -1 or -2
                SqlCommand getDomains = new SqlCommand(@"SELECT DOMAIN_NAME FROM dbo.Godaddy_Auctions_PROD(NOLOCK) WHERE PAGERANK<0 AND CRTE_TS=(SELECT MAX(CRTE_TS) FROM dbo.Godaddy_Auctions_PROD)", dbCon);


                SqlDataAdapter DomainAdapter = new SqlDataAdapter(getDomains);
                DomainAdapter.Fill(DomainDataSet);

                // Convert the list into an array. This code is very poor and needs to be refined. But works for now
                for (int i = 0; i < DomainDataSet.Tables[0].Rows.Count; i++)
                {

                    domainlist.Add(DomainDataSet.Tables[0].Rows[i]["DOMAIN_NAME"].ToString());
                }
                domainarray = domainlist.ToArray();

                // Read the proxy array into a list
                foreach (string s in ProxyFile)
                    ProxyList.Add(s);

                //
                parallelTasks p = new parallelTasks();



                Parallel.ForEach(domainarray, new ParallelOptions { MaxDegreeOfParallelism = 4 }, currentDomain =>
                {

                    SqlCommand updatePageRank;
                    proxyname = ProxyList.ElementAt(0).ToString();
                    int res = p.doStuff(currentDomain, proxyname);

                    Console.WriteLine(currentDomain);
                    using (SqlConnection connection = new SqlConnection(dynamicCon))
                    {
                        connection.Open();
                        if (res <= 0)
                        {
                            //Insert the result into DB

                            updatePageRank = new SqlCommand(@"UPDATE Godaddy_Auctions_PROD with (UPDLOCK,ROWLOCK) SET PAGERANK=" + res + ", VALID_PR='NA' WHERE DOMAIN_NAME='" + currentDomain + "'", connection);

                            //Remove Proxy only when the result is -1
                            if (res == -1)
                            {
                                Program.ProxyList.Remove(proxyname);
                            }
                        }

                        else if (res > 0 && res <= 20)
                        {
                            updatePageRank = new SqlCommand(@"UPDATE Godaddy_Auctions_PROD with (UPDLOCK,ROWLOCK) SET PAGERANK=" + (res - 10) + ", VALID_PR='Valid' WHERE DOMAIN_NAME='" + currentDomain + "'", connection);

                        }
                        else
                        {
                            updatePageRank = new SqlCommand(@"UPDATE Godaddy_Auctions_PROD with (UPDLOCK,ROWLOCK) SET PAGERANK=" + (res - 20) + ", VALID_PR='Invalid' WHERE DOMAIN_NAME='" + currentDomain + "'", connection);

                        }

                        try
                        {

                            updatePageRank.ExecuteNonQuery();
                        }
                        catch (Exception w)
                        {
                            Console.WriteLine("---------Update Query Error:" + w.Message.ToString() + "-------------");

                        }
                    }

                });


                Console.WriteLine("Page Rank Check: Completed");

                ///// START OF MOZ DATA CODE////////////////////////////////

                Console.WriteLine("MoZ Data Check: Started");


                SqlCommand mozDomains = new SqlCommand(@"SELECT DOMAIN_NAME FROM dbo.Godaddy_Auctions_PROD WHERE PAGERANK>0 AND VALID_PR='VALID' AND MOZRANK IS NULL AND CRTE_TS=(SELECT MAX(CRTE_TS) FROM Godaddy_Auctions_PROD)", dbCon);
                SqlDataAdapter mozDomainAdapter = new SqlDataAdapter(mozDomains);
                mozDomainAdapter.Fill(mozDomainDataSet);

                int batchCount = 10;

                int batchLoop = mozDomainDataSet.Tables[0].Rows.Count / batchCount;

                int remainderCount = mozDomainDataSet.Tables[0].Rows.Count % batchCount;

                for (int i = 1; i <= (batchCount - remainderCount); i++)
                    mozDomainDataSet.Tables[0].Rows.Add("dummy");



                for (int i = 0; i <= batchLoop; i++)
                {
                    int startIndex = i * batchCount;
                    parallelTasks.getMoz(startIndex);
                    Thread.Sleep(11000);

                }
                Console.WriteLine("MoZ Data Check: Completed");
                Console.WriteLine("All Tasks completed successfully!!!!!!");
            
        }

        public static int DownloadFiles(string FileName, String LandingFolder)
        {
            try
            {
                FtpWebRequest request = (FtpWebRequest)WebRequest.Create("ftp://ftp.godaddy.com/" + FileName);
                request.Method = WebRequestMethods.Ftp.DownloadFile;
                request.Credentials = new NetworkCredential("auctions", "");
                FtpWebResponse response = (FtpWebResponse)request.GetResponse();
                Stream responseStream = response.GetResponseStream();
                FileStream file = File.Create(LandingFolder + FileName);
                byte[] buffer = new byte[32 * 1024];
                int read;
                while ((read = responseStream.Read(buffer, 0, buffer.Length)) > 0)
                {
                    file.Write(buffer, 0, read);
                }

                file.Close();
                responseStream.Close();
                response.Close();
                return 0;
            }
            catch
            {
                return 1;
            }
        }


    }

    class parallelTasks
    {
        public int doStuff(string domainName, string proxy)
        {
            int validBit = 10;
            int invalidBit = 20;


            int res = GodaddyAnalyser.GetPR.MyPR(domainName, proxy);

            if (res > 0)
            {

                try
                {
                    HttpWebRequest myRequest = (HttpWebRequest)WebRequest.Create(@"http://www.google.com/search?q=info%3A" + domainName);

                    myRequest.Accept = "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8";
                    myRequest.UserAgent = "Mozilla/5.0 (Windows; U; Windows NT 6.1; en-US; rv:1.9.2.19) Gecko/20110707 Firefox/3.6.19";
                    myRequest.ProtocolVersion = HttpVersion.Version11;
                    myRequest.Method = "GET";
                    WebProxy webproxy = new WebProxy(proxy);
                    myRequest.Proxy = webproxy;
                    myRequest.Timeout = 7000;
                    //myRequest.ReadWriteTimeout = 5000;
                    string myResponse = new StreamReader(myRequest.GetResponse().GetResponseStream()).ReadToEnd();

                    if (myResponse.Contains(@"id=""resultStats"">1 "))
                        res = res + validBit;
                    else res = res + invalidBit;


                }

                catch (Exception ex)
                {


                    Console.WriteLine("Unable to check PR Validity : " + ex.Message.ToString());
                    res = -1;


                }

            }


            GC.Collect();
            return res;


        }

        public static void getMoz(int domainstartindex)
        {
            string Domain1 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex]["DOMAIN_NAME"].ToString();
            string Domain2 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 1]["DOMAIN_NAME"].ToString();
            string Domain3 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 2]["DOMAIN_NAME"].ToString();
            string Domain4 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 3]["DOMAIN_NAME"].ToString();
            string Domain5 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 4]["DOMAIN_NAME"].ToString();
            string Domain6 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 5]["DOMAIN_NAME"].ToString();
            string Domain7 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 6]["DOMAIN_NAME"].ToString();
            string Domain8 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 7]["DOMAIN_NAME"].ToString();
            string Domain9 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 8]["DOMAIN_NAME"].ToString();
            string Domain10 = "www." + Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + 9]["DOMAIN_NAME"].ToString();

            string result = "";
            string pythonScriptLocation = @"C:\GoDaddyFiles\MoZ-Python\mozmetrics.py";
            ProcessStartInfo start = new ProcessStartInfo();
            start.FileName = @"C:\Python27\python.exe";

            start.Arguments = string.Format("{0} {1} {2} {3} {4} {5} {6} {7} {8} {9} {10}", pythonScriptLocation, Domain1, Domain2, Domain3, Domain4, Domain5, Domain6, Domain7, Domain8, Domain9, Domain10);
            start.UseShellExecute = false;
            start.RedirectStandardOutput = true;
            using (Process process = Process.Start(start))
            {
                using (StreamReader reader = process.StandardOutput)
                {
                    result = reader.ReadToEnd();
                    result = result.Remove(result.IndexOf("]") - 1);
                    //Console.WriteLine(result);
                }
            }

            string[] rows = result.Split('}');
            int linenumber = 0;
            foreach (string line in rows)
            {

                string links = "", domain = Program.mozDomainDataSet.Tables[0].Rows[domainstartindex + linenumber]["DOMAIN_NAME"].ToString();
                decimal pa = 0, da = 0, mozrank = 0;
                string[] metrics = line.Split(',');

                if (metrics.Length > 2)
                {
                    if (metrics.Length == 6)
                    {
                        links = metrics[0].Replace("[{u'uid': ", "").Trim();
                        //domain = metrics[1].Replace(" u'uu': u'www.", "").Replace('/', ' ').Replace("'", "").Trim();
                        pa = Math.Round(Convert.ToDecimal(metrics[2].Replace(" u'upa': ", "")));
                        mozrank = Math.Round(Convert.ToDecimal(metrics[3].Replace(" u'umrp': ", "")), 2);
                        da = Math.Round(Convert.ToDecimal(metrics[5].Replace(" u'pda': ", "")));
                    }
                    else if (metrics.Length == 7)
                    {
                        links = metrics[1].Replace("{u'uid':", "").Trim();
                        // domain = metrics[2].Replace(" u'uu': u'www.", "").Replace('/', ' ').Replace("'", "").Trim();
                        pa = Math.Round(Convert.ToDecimal(metrics[3].Replace(" u'upa': ", "")));
                        mozrank = Math.Round(Convert.ToDecimal(metrics[4].Replace(" u'umrp': ", "")), 2);
                        da = Math.Round(Convert.ToDecimal(metrics[6].Replace(" u'pda': ", "")));
                    }
                    Console.WriteLine("Domain:" + domain);
                    Console.WriteLine("Links:" + links);
                    Console.WriteLine("Domain Authority:" + da);
                    Console.WriteLine("Page Authority:" + pa);
                    Console.WriteLine("Moz Rank:" + mozrank);
                    Console.WriteLine("------------------------");
                    SqlCommand updateDB = new SqlCommand(@"UPDATE Godaddy_Auctions_PROD SET MOZRANK=" + mozrank.ToString() + ",DOMAIN_AUTHORITY=" + da.ToString() + ",PAGE_AUTHORITY=" + pa.ToString() + ",TOTAL_LINKS=" + links + " WHERE DOMAIN_NAME='" + domain + "'", Program.dbCon);
                    Program.dbCon.Open();
                    updateDB.ExecuteNonQuery();
                    Program.dbCon.Close();
                }
                linenumber++;
            }

        }
    }

}

