import pandas as pd
from sqlalchemy import create_engine, inspect
from langchain.vectorstores import PGVector
from sqlalchemy.orm import sessionmaker, scoped_session
from sqlalchemy.sql import text

class KnK(PGVector):
    def __init__(self, db_conn_str, db_name, embedding_function, uniquer_id_identifier=' ~knkid~') -> None:
        self.db_conn_str = db_conn_str
        self.db_name = db_name
        self.embedding_function = embedding_function
        super().__init__(
            embedding_function=self.embedding_function,
            collection_name=self.db_name,
            connection_string=self.db_conn_str)
        self.uniquer_id_identifier = uniquer_id_identifier
    
    def add_data(self, file, embedding_column):
        df = pd.read_csv(file)
        engine = create_engine(self.db_conn_str, echo=False)
        engine = create_engine(self.db_conn_str)

        #Create PG Vector Extension for a new database or if it is not installed
        list_of_ext = pd.read_sql('SELECT * FROM pg_extension;', self.db_conn_str)['extname'].to_list()
        if 'vector' not in list_of_ext:
            Session = scoped_session(sessionmaker(bind=engine))
            s = Session()
            s.execute(text('CREATE EXTENSION vector;'))
            
        if not inspect(engine).has_table(self.db_name):
            df['KnK_unique_ID'] = df.index
            df.to_sql(name=self.db_name, con=engine, index=False)
        else:
            query = f"""SELECT "KnK_unique_ID" FROM {self.db_name} ORDER BY "KnK_unique_ID" DESC LIMIT 1"""
            curr_index = pd.read_sql(query,f'{self.db_conn_str}').iloc[-1][-1] + 1
            
            df.index = [i for i in range(curr_index, curr_index+len(df))]
            df['KnK_unique_ID'] = df.index
            df.to_sql(name=self.db_name, con=engine, index=False, if_exists='append')
            self.last_index = curr_index+len(df)
        
        docs = (df[embedding_column] + self.uniquer_id_identifier + df['KnK_unique_ID'].map(str))
        super().add_texts(
            docs
        )
        
        return True
    
    def retrieve_data(self, query, num_of_results,search_type = 'similarity_search', columns = None):
        if search_type == 'similarity_search':
            results = super().similarity_search(query=query, k=num_of_results)
        
        ids = tuple((int(results[i].page_content.split(self.uniquer_id_identifier)[1]) for i in range(len(results))))
        
        sql_query = f"""
        SELECT *
        FROM {self.db_name}
        WHERE "KnK_unique_ID" in {ids}
        """
        
        result = pd.read_sql(sql=sql_query, con=self.db_conn_str)
        return result.drop("KnK_unique_ID", axis=1)