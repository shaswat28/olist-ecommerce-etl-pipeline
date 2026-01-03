import pandas as pd
from sqlalchemy import create_engine

def load_to_mysql(df, table_name, password):

    connection_url = f"mysql+mysqlconnector://root:{password}@localhost/olist_db"
    engine = create_engine(connection_url)
    
    # convert spark df to pandas if needed
    if hasattr(df, "toPandas"):
        df = df.toPandas()
        
    # write data to sql table
    df.to_sql(table_name, con=engine, if_exists='replace', index=False)
    print(f"Successfully loaded {len(df)} rows into table: {table_name}")