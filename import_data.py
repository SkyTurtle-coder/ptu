import mysql.connector
from mysql.connector import errorcode
import requests
from Bio.SeqUtils.ProtParam import ProteinAnalysis

# ============================================
# MySQL-Konfiguration
# ============================================

MYSQL_HOST = "localhost"
MYSQL_USER = "root"
MYSQL_PASSWORD = "VaPh_Schatz_2301"
MYSQL_DB_NAME = "column_finder"


# ============================================
# UniProt-Konfiguration
# ============================================

# reviewed:true   -> Swiss-Prot, kuratierte Eintraege
# organism_id:9606 -> Human
UNIPROT_QUERY = "reviewed:true AND organism_id:9606"
MAX_RESULTS = 450

UNIPROT_BASE_URL = "https://rest.uniprot.org/uniprotkb/search"


# ============================================
# UniProt-Funktionen
# ============================================

def fetch_uniprot_proteins(query, max_results=200):
    """
    Holt Proteindaten von UniProt als Liste von Dictionaries.
    Nutzt das TSV-Format der UniProt-REST-API.
    """
    params = {
        "query": query,
        "format": "tsv",
        "fields": ",".join([
            "accession",
            "protein_name",
            "gene_names",
            "organism_name",
            "length",
            "mass",
            "sequence"
        ]),
        "size": max_results
    }

    print("Frage UniProt-API ab...")
    resp = requests.get(UNIPROT_BASE_URL, params=params)
    resp.raise_for_status()

    lines = resp.text.strip().split("\n")
    header = lines[0].split("\t")
    data = []

    for line in lines[1:]:
        cols = line.split("\t")
        entry = dict(zip(header, cols))
        data.append(entry)

    print(f"{len(data)} Proteine von UniProt geholt.")
    return data


def compute_pi(sequence):
    """
    Berechnet den theoretischen isoelektrischen Punkt (pI)
    aus der Aminosaeuresequenz. Gibt None zurueck, wenn
    die Sequenz ungeeignet ist.
    """
    try:
        seq = sequence.replace(" ", "").replace("\n", "")
        if len(seq) < 5:
            return None
        analysis = ProteinAnalysis(seq)
        return float(analysis.isoelectric_point())
    except Exception:
        return None


# ============================================
# MySQL-Verbindung / DB-Anlage
# ============================================

def get_mysql_connection(with_database=False):
    """
    Stellt eine Verbindung zu MySQL her.
    Wenn with_database=True, wird direkt die angegebene Datenbank benutzt.
    """
    config = {
        "host": MYSQL_HOST,
        "user": MYSQL_USER,
        "password": MYSQL_PASSWORD,
    }
    if with_database:
        config["database"] = MYSQL_DB_NAME
    return mysql.connector.connect(**config)


def ensure_database_exists():
    """
    Legt die Datenbank an, falls sie noch nicht existiert.
    """
    try:
        conn = get_mysql_connection(with_database=False)
        cur = conn.cursor()
        cur.execute(
            f"CREATE DATABASE IF NOT EXISTS {MYSQL_DB_NAME} "
            "DEFAULT CHARACTER SET utf8mb4 "
            "COLLATE utf8mb4_unicode_ci;"
        )
        conn.commit()
        cur.close()
        conn.close()
        print(f"Datenbank '{MYSQL_DB_NAME}' ist bereit.")
    except mysql.connector.Error as err:
        print("Fehler beim Erstellen der Datenbank:", err)
        raise


def init_db():
    """
    Oeffnet eine Verbindung zur existierenden Datenbank und legt Tabellen an.
    """
    conn = get_mysql_connection(with_database=True)
    cur = conn.cursor()

    # Protein-Tabelle
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS protein (
        id INT AUTO_INCREMENT PRIMARY KEY,
        uniprot_id VARCHAR(20),
        name TEXT NOT NULL,
        gene_name VARCHAR(255),
        organism VARCHAR(255),
        length INT,
        mw_kda DOUBLE,
        pI DOUBLE,
        tag VARCHAR(50),
        description TEXT
        ) ENGINE=InnoDB;

        """
    )

    # Chromatographie-Saeulen-Tabelle
    cur.execute(
        """
        CREATE TABLE IF NOT EXISTS chromatography_column (
            id INT AUTO_INCREMENT PRIMARY KEY,
            name VARCHAR(150) NOT NULL,
            type VARCHAR(50),
            resin VARCHAR(100),
            ph_min DOUBLE,
            ph_max DOUBLE,
            description TEXT
        ) ENGINE=InnoDB;
        """
    )

    conn.commit()
    cur.close()
    return conn


# ============================================
# Daten einfuegen
# ============================================

def insert_default_columns(conn):
    """
    Fuegt ein paar typische Chromatographiesaeulen ein,
    falls die Tabelle noch leer ist.
    """
    cur = conn.cursor()
    cur.execute("SELECT COUNT(*) FROM chromatography_column;")
    count = cur.fetchone()[0]
    if count > 0:
        print("Chromatographie-Saeulen bereits vorhanden, ueberspringe Insert.")
        cur.close()
        return

    print("Fuege Standard-Chromatographiesaeulen ein...")
    columns = [
        ("HisTrap FF", "IMAC", "Ni-NTA", 7.0, 8.0,
         "Affinity purification of His-tagged proteins"),
        ("GSTrap 4B", "Affinity", "Glutathione Sepharose 4B", 7.0, 8.0,
         "Affinity purification of GST-tagged proteins"),
        ("Strep-Tactin Sepharose", "Affinity", "Strep-Tactin", 7.0, 8.5,
         "Affinity purification of Strep-tagged proteins"),
        ("HiTrap Q HP", "IEX-Anion", "Q-Sepharose", 6.0, 9.0,
         "Anion exchange chromatography for proteins with pI < buffer pH"),
        ("HiTrap SP HP", "IEX-Cation", "SP-Sepharose", 4.0, 7.5,
         "Cation exchange chromatography for proteins with pI > buffer pH"),
        ("Superdex 75 Increase 10/300 GL", "SEC",
         "Cross-linked agarose/dextran", 3.0, 10.0,
         "Size exclusion chromatography for 3-70 kDa proteins"),
        ("Superdex 200 Increase 10/300 GL", "SEC",
         "Cross-linked agarose/dextran", 3.0, 10.0,
         "Size exclusion chromatography for ~10-600 kDa proteins"),
    ]

    cur.executemany(
        """
        INSERT INTO chromatography_column
        (name, type, resin, ph_min, ph_max, description)
        VALUES (%s, %s, %s, %s, %s, %s);
        """,
        columns
    )
    conn.commit()
    cur.close()
    print("Chromatographie-Saeulen eingefuegt.")


def insert_proteins(conn, proteins):
    """
    Schreibt UniProt-Proteine in die MySQL-Datenbank.
    Tag setzen wir vorerst auf 'none'.
    """
    cur = conn.cursor()

    print("Fuege Proteine in die Datenbank ein...")
    inserted = 0

    for p in proteins:
        uniprot_id = p.get("Entry")
        name = p.get("Protein names")
        gene_name = p.get("Gene Names")
        organism = p.get("Organism")
        length = p.get("Length")
        mass = p.get("Mass")
        seq = p.get("Sequence")

        try:
            length_int = int(length) if length is not None else None
        except ValueError:
            length_int = None

        try:
            mw_kda = float(mass) / 1000.0 if mass is not None else None
        except ValueError:
            mw_kda = None

        pI_val = compute_pi(seq) if seq else None

        cur.execute(
            """
            INSERT INTO protein
            (uniprot_id, name, gene_name, organism, length, mw_kda, pI, tag, description)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON DUPLICATE KEY UPDATE
                name = VALUES(name),
                gene_name = VALUES(gene_name),
                organism = VALUES(organism),
                length = VALUES(length),
                mw_kda = VALUES(mw_kda),
                pI = VALUES(pI),
                tag = VALUES(tag),
                description = VALUES(description);
            """,
            (
                uniprot_id,
                name,
                gene_name,
                organism,
                length_int,
                mw_kda,
                pI_val,
                "none",   # Standard: kein Tag
                None      # description kannst du spaeter ergaenzen
            )
        )
        inserted += 1

    conn.commit()
    cur.close()
    print(f"{inserted} Proteine eingefuegt.")


def dedupe_proteins(conn):
    """
    Entfernt doppelte Proteine basierend auf uniprot_id.
    Behaelt den Datensatz mit der kleinsten id.
    """
    cur = conn.cursor()
    cur.execute(
        """
        DELETE p1 FROM protein p1
        JOIN protein p2
          ON p1.uniprot_id = p2.uniprot_id
         AND p1.id > p2.id;
        """
    )
    deleted = cur.rowcount
    conn.commit()

    try:
        cur.execute("ALTER TABLE protein ADD UNIQUE KEY uq_protein_uniprot (uniprot_id);")
        conn.commit()
    except mysql.connector.Error:
        # Index existiert bereits oder konnte nicht gesetzt werden
        pass

    cur.close()
    if deleted:
        print(f"{deleted} doppelte Proteine entfernt.")


# ============================================
# View mit Heuristik fuer Saeulenwahl
# ============================================

def create_protein_view_with_recommendation(conn):
    """
    Legt eine VIEW an, die fuer jedes Protein eine empfohlene Saeule
    nach der Heuristik berechnet.
    """
    cur = conn.cursor()

    # Falls es die View schon gibt, erst loeschen
    cur.execute("DROP VIEW IF EXISTS protein_with_recommendation;")

    cur.execute(
        """
        CREATE VIEW protein_with_recommendation AS
        SELECT
            p.*,
            CASE
                WHEN p.tag LIKE '%His%' THEN 'HisTrap FF (IMAC, Ni-NTA)'
                WHEN p.tag LIKE '%GST%' THEN 'GSTrap 4B (Affinity)'
                WHEN p.tag LIKE '%Strep%' THEN 'Strep-Tactin Sepharose (Affinity)'
                WHEN p.pI IS NOT NULL AND p.pI < 7.0 THEN 'HiTrap Q HP (Anion exchange)'
                WHEN p.pI IS NOT NULL AND p.pI > 7.0 THEN 'HiTrap SP HP (Cation exchange)'
                ELSE 'Superdex 200 Increase (SEC)'
            END AS recommended_column,
            CASE
                WHEN p.mw_kda IS NOT NULL AND p.mw_kda <= 70 THEN 'Superdex 75 Increase (SEC polishing)'
                WHEN p.mw_kda IS NOT NULL AND p.mw_kda > 70 THEN 'Superdex 200 Increase (SEC polishing)'
                ELSE NULL
            END AS polishing_column
        FROM protein p;
        """
    )

    conn.commit()
    cur.close()
    print("View 'protein_with_recommendation' erstellt.")


# ============================================
# Main
# ============================================

def main():
    # 0) DB anlegen (falls nicht vorhanden)
    ensure_database_exists()

    # 1) Verbindung zur DB + Tabellen anlegen
    conn = init_db()

    # 2) Saeulen einfuegen (falls leer)
    insert_default_columns(conn)

    # 3) UniProt-Daten holen
    proteins = fetch_uniprot_proteins(UNIPROT_QUERY, MAX_RESULTS)

    # 4) Proteine einfuegen
    insert_proteins(conn, proteins)

    # 4b) Duplikate bereinigen
    dedupe_proteins(conn)

    # 5) View mit Heuristik
    create_protein_view_with_recommendation(conn)

    conn.close()
    print("Fertig. MySQL-Datenbank ist bereit.")


if __name__ == "__main__":
    main()
