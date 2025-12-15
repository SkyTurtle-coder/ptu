from flask import Flask, render_template, request, abort, jsonify, redirect, url_for
import mysql.connector
import os
import urllib.parse

app = Flask(__name__)

# DB-Konfiguration (ausschliesslich ueber Umgebungsvariablen ueberschreibbar)
DB_HOST = os.environ.get("DB_HOST", "127.0.0.1")
DB_USER = os.environ.get("DB_USER", "root")
DB_PASSWORD = os.environ.get("DB_PASSWORD", "")
DB_NAME = os.environ.get("DB_NAME", "column_finder")


def get_db_connection():
    return mysql.connector.connect(
        host=DB_HOST,
        user=DB_USER,
        password=DB_PASSWORD,
        database=DB_NAME,
        connection_timeout=5,
    )


def cytiva_url(column_name: str) -> str:
    """Erzeugt einen Cytiva-Link fuer bekannte Saeulen; sonst generischer Suchlink."""
    if not column_name:
        return ""

    mapping = {
        "HiTrap Q HP (Anion exchange)":
            "https://www.cytivalifesciences.com/en/de/products/items/hitrap-q-hp-anion-exchange-chromatography-column-p-00607",
        "HiTrap SP HP (Cation exchange)":
            "https://www.cytivalifesciences.com/en/de/products/items/hitrap-sp-hp-cation-exchange-chromatography-column-p-00794",
        "HisTrap FF (IMAC, Ni-NTA)":
            "https://www.cytivalifesciences.com/en/de/products/items/histrap-ff-p-00251",
        "GSTrap 4B (Affinity)":
            "https://www.cytivalifesciences.com/en/de/products/items/gstrap-4b-columns-p-00307",
        "Strep-Tactin Sepharose (Affinity)":
            "https://www.cytivalifesciences.com/en/de/products/items/strep-tactin-xt-4flow-p-08318",
        "Superdex 75 Increase (SEC)":
            "https://www.cytivalifesciences.com/en/de/products/items/superdex-75-increase-p-06188",
        "Superdex 200 Increase (SEC)":
            "https://www.cytivalifesciences.com/en/de/products/items/superdex-200-increase-small-scale-size-exclusion-chromatography-columns-p-06190",
        "Superdex 75 Increase (SEC polishing)":
            "https://www.cytivalifesciences.com/en/de/products/items/superdex-75-increase-p-06188",
        "Superdex 200 Increase (SEC polishing)":
            "https://www.cytivalifesciences.com/en/de/products/items/superdex-200-increase-small-scale-size-exclusion-chromatography-columns-p-06190",
    }

    if column_name in mapping:
        return mapping[column_name]

    from urllib.parse import quote_plus
    return f"https://www.cytivalifesciences.com/en/de/search?q={quote_plus(column_name)}"


@app.context_processor
def inject_helpers():
    # Stellt die Hilfsfunktion in Templates bereit
    return {"cytiva_url": cytiva_url}


@app.route("/")
def index():
    protein_count = None
    pi_buckets = {"lt6": 0, "btw6_8": 0, "gt8": 0}
    mw_buckets = {"lt50": 0, "btw50_100": 0, "gt100": 0}
    pi_total = 0
    mw_total = 0
    error_message = None
    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute("SELECT COUNT(*) AS cnt FROM protein;")
        protein_count = cur.fetchone()["cnt"]

        cur.execute(
            """
            SELECT
              SUM(CASE WHEN pI IS NOT NULL AND pI < 6 THEN 1 ELSE 0 END) AS lt6,
              SUM(CASE WHEN pI IS NOT NULL AND pI >= 6 AND pI <= 8 THEN 1 ELSE 0 END) AS btw6_8,
              SUM(CASE WHEN pI IS NOT NULL AND pI > 8 THEN 1 ELSE 0 END) AS gt8
            FROM protein;
            """
        )
        row = cur.fetchone()
        if row:
            pi_buckets = {k: row.get(k) or 0 for k in ["lt6", "btw6_8", "gt8"]}
            pi_total = sum(pi_buckets.values())

        cur.execute(
            """
            SELECT
              SUM(CASE WHEN mw_kda IS NOT NULL AND mw_kda < 50 THEN 1 ELSE 0 END) AS lt50,
              SUM(CASE WHEN mw_kda IS NOT NULL AND mw_kda >= 50 AND mw_kda <= 100 THEN 1 ELSE 0 END) AS btw50_100,
              SUM(CASE WHEN mw_kda IS NOT NULL AND mw_kda > 100 THEN 1 ELSE 0 END) AS gt100
            FROM protein;
            """
        )
        row2 = cur.fetchone()
        if row2:
            mw_buckets = {k: row2.get(k) or 0 for k in ["lt50", "btw50_100", "gt100"]}
            mw_total = sum(mw_buckets.values())

    except Exception as err:
        error_message = f"DB-Fehler: {err}"
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return render_template(
        "index.html",
        protein_count=protein_count,
        pi_buckets=pi_buckets,
        mw_buckets=mw_buckets,
        pi_total=pi_total,
        mw_total=mw_total,
        error_message=error_message,
    )


@app.route("/proteins", methods=["GET"])
def results():
    search_query = request.args.get("search", "").strip()
    results = []
    error_message = None
    pi_global_min = None
    pi_global_max = None
    mw_global_min = None
    mw_global_max = None

    conn = None
    cur = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        like = f"%{search_query}%" if search_query else "%"
        cur.execute(
            """
            SELECT *
            FROM protein_with_recommendation
            WHERE name LIKE %s
               OR gene_name LIKE %s
               OR organism LIKE %s
            ORDER BY name
            LIMIT 100;
            """,
            (like, like, like),
        )
        results = cur.fetchall()

        # Global ranges for pI and MW to scale stats
        cur.execute(
            """
            SELECT
              MIN(pI) AS pi_min, MAX(pI) AS pi_max,
              MIN(mw_kda) AS mw_min, MAX(mw_kda) AS mw_max
            FROM protein;
            """
        )
        gr = cur.fetchone()
        if gr:
            pi_global_min = gr.get("pi_min")
            pi_global_max = gr.get("pi_max")
            mw_global_min = gr.get("mw_min")
            mw_global_max = gr.get("mw_max")

    except mysql.connector.Error as err:
        error_message = f"Datenbankfehler: {err}"
    except Exception as err:
        error_message = f"Fehler: {err}"
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return render_template(
        "results.html",
        results=results,
        search_query=search_query,
        error_message=error_message,
        pi_global_min=pi_global_min,
        pi_global_max=pi_global_max,
        mw_global_min=mw_global_min,
        mw_global_max=mw_global_max,
    )


def compute_recommendation_row(row, tag_choice=None):
    """Berechnet Empfehlung/Polishing samt URLs basierend auf Tag/pI/MW."""
    pi = row.get("pI")
    mw = row.get("mw_kda")

    # Default aus DB
    rec_text = row.get("recommended_column")
    pol_text = row.get("polishing_column")

    if tag_choice:
        tag_choice = tag_choice.strip()

    if tag_choice == "His":
        rec_text = "HisTrap FF (IMAC, Ni-NTA)"
    elif tag_choice == "GST":
        rec_text = "GSTrap 4B (Affinity)"
    elif tag_choice == "Strep":
        rec_text = "Strep-Tactin Sepharose (Affinity)"
    else:
        # fallback Heuristik pI
        if pi is not None:
            try:
                if float(pi) < 7:
                    rec_text = "HiTrap Q HP (Anion exchange)"
                elif float(pi) > 7:
                    rec_text = "HiTrap SP HP (Cation exchange)"
                else:
                    rec_text = "Superdex 200 Increase (SEC)"
            except Exception:
                pass

    # polishing Heuristik nach MW
    if mw is not None:
        try:
            if float(mw) <= 70:
                pol_text = "Superdex 75 Increase (SEC polishing)"
            else:
                pol_text = "Superdex 200 Increase (SEC polishing)"
        except Exception:
            pass

    return {
        "recommended_column": rec_text,
        "recommended_url": cytiva_url(rec_text) if rec_text else "",
        "polishing_column": pol_text,
        "polishing_url": cytiva_url(pol_text) if pol_text else "",
    }


@app.route("/proteins/<int:protein_id>")
def protein_detail(protein_id):
    """Detailseite fuer ein einzelnes Protein."""
    conn = None
    cur = None
    protein = None
    error_message = None
    tag_choice = request.args.get("tag", "").strip()

    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)

        cur.execute(
            """
            SELECT
                p.*
            FROM protein_with_recommendation p
            WHERE p.id = %s
            LIMIT 1;
            """,
            (protein_id,),
        )
        protein = cur.fetchone()
    except mysql.connector.Error as err:
        error_message = f"Datenbankfehler: {err}"
    except Exception as err:
        error_message = f"Fehler: {err}"
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    if not protein:
        abort(404)

    rec_data = compute_recommendation_row(protein, tag_choice=tag_choice)

    return render_template(
        "protein.html",
        protein=protein,
        rec_data=rec_data,
        tag_choice=tag_choice,
        error_message=error_message,
    )


@app.route("/api/proteins/<int:protein_id>")
def api_protein(protein_id):
    """Einfache JSON-API fuer ein Protein."""
    conn = None
    cur = None
    protein = None
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT
                p.*
            FROM protein_with_recommendation p
            WHERE p.id = %s
            LIMIT 1;
            """,
            (protein_id,),
        )
        protein = cur.fetchone()
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    if not protein:
        abort(404)

    rec_data = compute_recommendation_row(protein, tag_choice=request.args.get("tag", "").strip())
    protein.update(rec_data)
    return jsonify(protein)


@app.route("/api/example", methods=["GET"])
def api_example():
    """Gibt einen zufaelligen Protein-Namen (oder Gen/UniProt) zurueck."""
    conn = None
    cur = None
    choice = ""
    try:
        conn = get_db_connection()
        cur = conn.cursor(dictionary=True)
        cur.execute(
            """
            SELECT COALESCE(name, gene_name, uniprot_id) AS label
            FROM protein
            ORDER BY RAND()
            LIMIT 1;
            """
        )
        row = cur.fetchone()
        if row and row.get("label"):
            choice = row["label"]
    finally:
        if cur:
            cur.close()
        if conn:
            conn.close()

    return jsonify({"search": choice or ""})


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5000, debug=True)
