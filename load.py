import pandas as pd
from sqlalchemy import create_engine

def load_to_mysql(df, table_name, password):

    connection_url = f"mysql+mysqlconnector://root:{password}@localhost/olist_db"
    engine = create_engine(connection_url)
    
    # Convert Spark DF to Pandas if necessary
    if hasattr(df, "toPandas"):
        df = df.toPandas()
        
    # Write data to the SQL table
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Successfully loaded {len(df)} rows into table: {table_name}")