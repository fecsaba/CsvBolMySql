using MySql.Data.MySqlClient;
using System;
using System.Collections.Generic;
using System.IO;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBolMySql
{
    class Program
    {
        private static string conStr = "Server=localhost;Database=utak;Uid=root;Pwd=;";

        static void Main(string[] args)
        {
            StreamReader reader = new StreamReader("fuvar.csv");
            List<UtCsv> Utak = new List<UtCsv>();
            string sor;
            string fejlec = reader.ReadLine();
            while ((sor = reader.ReadLine()) != null)
            {
                Utak.Add(new UtCsv(sor));
            }
            reader.Close();


            Dictionary<string, int> FizModok = new Dictionary<string, int>();
            int i = 1;
            foreach (var item in Utak )
            {
                if (!FizModok.ContainsKey(item.FizetesModja))
                {
                    FizModok.Add(item.FizetesModja, i);
                    i++;
                }
            }
            Console.WriteLine("fizmodok insert");
            if (isEmptyTable("fizmodok"))
            {

                foreach (var item in FizModok)
                {

                    InsertFizmodok(item.Key, item.Value);
                }

            }
            else Console.WriteLine("Fizmodok feltöltve");


            Console.WriteLine("utadatok insert");
            if (isEmptyTable("utadatok"))
            {

                foreach (var item in Utak)
                {

                    InsertUtadatok(item, FizModok[item.FizetesModja]);
                }

            }
            else Console.WriteLine("Utadatok feltöltve");
            Console.WriteLine("Kész");
            Console.ReadKey();


        }

        private static void InsertUtadatok(UtCsv model, int index)
        {
            using (var con = new MySqlConnection(conStr))
            {
                con.Open();
                string sql = "INSERT INTO utadatok (taxiid, indulasideje, idotartam, tavolsag, viteldij, borravalo, fizmodid) " +
                        "VALUES(@taxiid, @indulasideje, @idotartam, @tavolsag, @viteldij, @borravalo, @fizmodid)";
                using (var cmd = new MySqlCommand(sql, con))
                {
                    cmd.Parameters.AddWithValue("@taxiid", model.TaxiId);
                    cmd.Parameters.AddWithValue("@indulasideje", model.IndulasIdeje);
                    cmd.Parameters.AddWithValue("@idotartam", model.Idotartam);
                    cmd.Parameters.AddWithValue("@tavolsag", model.Tavolsag);
                    cmd.Parameters.AddWithValue("@viteldij", model.Viteldij);
                    cmd.Parameters.AddWithValue("@borravalo", model.Borravalo);
                    cmd.Parameters.AddWithValue("@fizmodid", index);
                    cmd.ExecuteNonQuery();
                }
            }
        }

        private static bool isEmptyTable(string tableName)
        {
            bool res = false;
            using (var con = new MySqlConnection(conStr))
            {
                con.Open();
                
                using (var cmd = new MySqlCommand("SELECT * from " + tableName , con))
                {
                    using (var reader = cmd.ExecuteReader())
                    {
                        res = !reader.HasRows;
                    }
                }

            }

            return res;
        }

        private static void InsertFizmodok(string nev, int index)
        {
            using (var con = new MySqlConnection(conStr))
            {
                con.Open();
                string sql = "INSERT INTO fizmodok (id, megnevezes) " +
                        "VALUES(@id, @megnevezes)";
                using (var cmd = new MySqlCommand(sql, con))
                {
                    cmd.Parameters.AddWithValue("@id", index);
                    cmd.Parameters.AddWithValue("@megnevezes", nev);
                    cmd.ExecuteNonQuery();
                }
                
            }
        }
    }
}
