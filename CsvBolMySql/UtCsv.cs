using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace CsvBolMySql
{
    class UtCsv
    {
        public int TaxiId { get; set; }
        public string IndulasIdeje { get; set; }
        public int Idotartam { get; set; }
        public float Tavolsag { get; set; }
        public float Viteldij { get; set; }
        public float Borravalo { get; set; }
        public string FizetesModja { get; set; }

        public UtCsv(string sor)
        {
            string[] mezok = sor.Split(';');
            TaxiId = int.Parse(mezok[0]);
            IndulasIdeje = mezok[1];
            Idotartam = int.Parse(mezok[2]);
            Tavolsag = float.Parse(mezok[3]);
            Viteldij = float.Parse(mezok[4]);
            Borravalo = float.Parse(mezok[5]);
            FizetesModja = mezok[6];
        }
    }
}
