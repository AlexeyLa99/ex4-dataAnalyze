"""
תרגיל מספר 4 - אנליזה של ביג דאטה
מגישים:
יפתח אוחיון - 208532796
אלכסיי לייקוב - 321270589
"""

#------------------------------------------------------------
import sqlite3
import textwrap
import pandas as pd

#------------------------------------------------------------
def print_query(conn, q_number, query):
    """
    מדפיס את מבנה השאלה, מריץ את השאילתה,
    ומדפיס את כמות השורות + 5 ראשונות ו-5 אחרונות (או הכול אם פחות מ-10).
    """
    # מנקה הזחות מיותרות ומרווחים בקצוות
    query = textwrap.dedent(query).strip()

    # קו מפריד + כותרת השאלה
    print("=" * 45)
    print(f"Question: {q_number}")
    print("The query:\n")
    print(query)
    print()

    # הרצת השאילתה
    df = pd.read_sql_query(query, conn)

    # כמות שורות
    num_rows = len(df)
    print(f"Num of rows: {num_rows}")
    # הדפסת תוצאה – או הכול (אם <= 10) או 5 ראשונות ו-5 אחרונות
    if num_rows <= 10:
        print(df.to_string())
    else:
        print(df.head(5).to_string())
        print("...")
        print(df.tail(5).to_string())

    print()


#------------------------------------------------------------
# התחברות למסד הנתונים
DB_FILE = "World.db3"
conn = sqlite3.connect(DB_FILE)
cur = conn.cursor()

#------------------------------------------------------------

queries = [
    (
        "1",
        """
        SELECT
            Name,
            Continent,
            SurfaceArea,
            SUM(SurfaceArea) OVER (PARTITION BY Continent) AS ContinentTotal,
            SurfaceArea * 1.0 /
                SUM(SurfaceArea) OVER (PARTITION BY Continent) AS SurfacePercent
        FROM Country
        ORDER BY
            Continent ASC,
            SurfacePercent DESC;
        """
    ),
    (
        "2",
        """
        SELECT
            Name,
            CountryCode,
            Population,
            CASE
                WHEN Population > AVG(Population) OVER (PARTITION BY CountryCode)
                    THEN 'Above average'
                ELSE 'Not above average'
            END AS IsAboveCountryAvg
        FROM City
        ORDER BY
            CountryCode ASC,
            Population ASC
        """
    ),
    (
        "3",
        """
        SELECT 
        RANK() OVER (ORDER BY SurfaceArea DESC) AS Area_Rank,
        *
        FROM Country
        ORDER BY Area_Rank
        """
    ),
    (
        "4",
        """
        SELECT 
            DENSE_RANK() OVER (ORDER BY IndepYear ASC) AS IndepYear_Rank,
            *
        FROM Country
        ORDER BY IndepYear_Rank, Code
        """
    ),
    (
        "5",
        """
        SELECT 
            RANK() OVER (ORDER BY COUNT(City.ID) DESC) AS City_Rank,
            Country.*
        FROM Country
        LEFT JOIN City ON Country.Code = City.CountryCode
        GROUP BY Country.Code
        ORDER BY City_Rank DESC
        """
    ),
    (   "6",
        """
        SELECT 
            Name,
            Population,
            SUM(Population) OVER (ORDER BY Population DESC) AS Rolling_Population,
            (SUM(Population) OVER (ORDER BY Population DESC) * 100.0 / SUM(Population) OVER ()) AS Rolling_Percent
        FROM Country
        ORDER BY Population DESC
        """
    ),
    (
        "7",
        """
        WITH Population_Stats AS (
        SELECT 
            ROW_NUMBER() OVER (ORDER BY Population DESC) as Rn,
            (SUM(Population) OVER (ORDER BY Population DESC) * 100.0 / SUM(Population) OVER ()) as Rolling_Percent
        FROM Country
        )
        SELECT MIN(Rn) as Min_Count
        FROM Population_Stats
        WHERE Rolling_Percent >= 50
        """
    ),

]

#------------------------------------------------------------
# לולאה שעוברת על כל השאילתות ומדפיסה לפי הסדר
for q_num, q in queries:
    print_query(conn, q_num, q)

# סגירת חיבור לבסוף
conn.close()
