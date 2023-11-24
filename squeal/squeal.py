import pandas as pd
import sqlalchemy as sa
import pathlib
import tomllib
import rich
from rich import console as _console
from rich.progress import track as _track
import typer


console = _console.Console()
app = typer.Typer()



def _dfToTable(df):
    """Convert a pandas.DataFrame obj into a rich.Table obj.
    Args:
        pandas_dataframe (DataFrame): A Pandas DataFrame to be converted to a rich Table.
        rich_table (Table): A rich Table that should be populated by the DataFrame values.
        show_index (bool): Add a column with a row count to the table. Defaults to True.
        index_name (str, optional): The column name to give to the index column. Defaults to None, showing no value.
    Returns:
        Table: The rich Table instance passed, populated with the DataFrame values.

    from: https://gist.github.com/neelabalan/33ab34cf65b43e305c3f12ec6db05938

    """

    # if show_index:
    #     index_name = str(index_name) if index_name else ""
    #     rich_table.add_column(index_name)

    table = rich.table.Table()

    for column in df.columns:
        table.add_column(str(column))

    for index, value_list in enumerate(df.values.tolist()):
        row = []
        # row = [str(index)] if show_index else []
        row += [str(x) for x in value_list]
        table.add_row(*row)

    return table


class SQLConnection():
    def __init__(self, config=None, quiet=False):
        self.config = config
        self.quiet = quiet
        self.con = None

    def loadDefaultConfig(self):
        configDir = pathlib.Path('~/.config/squeal.toml').expanduser()
        with open(configDir, 'rb') as fin:
            config = tomllib.load(fin)
        defaultProfile = config['config']['default']
        config2 = config['profiles'][defaultProfile]
        self.config = config2

    def loadProfile(self, profile):
        configDir = pathlib.Path('~/.config/squeal.toml').expanduser()
        with open(configDir, 'rb') as fin:
            config = tomllib.load(fin)
        # defaultProfile = config['config']['default'] # 
        config2 = config['profiles'][profile]
        self.config = config2        

    def connect(self):
        if not self.quiet: console.log('Connecting...')
        if self.config is None:
            self.loadDefaultConfig()
        
        conUrl = sa.engine.URL.create(
            **self.config
        )
        self.con = sa.create_engine(conUrl).execution_options()
        return self

    def query(self, sql):
        if not self.quiet: console.log('Executing query...')
        
        # out = []

        # CHUNK_SIZE = 10000

        # i = 0

        # while True:

        #     SQL_OUT = sql + f' OFFSET {i*CHUNK_SIZE} FETCH NEXT {CHUNK_SIZE} ROWS ONLY'

        #     data = pd.read_sql_query(SQL_OUT, self.con)
        #     out.append(data)

        #     if len(data) < CHUNK_SIZE:
        #         break

        #     i += 1


        # data = pd.concat(out)
        
        data = pd.read_sql_query(sql, con=self.con)
        return data

    def __call__(self, sql, print=True):
        if self.con is None:
            self.connect()
        data = self.query(sql)

        if print:
            console.print(_dfToTable(data))

        return data

SQL = SQLConnection()

@app.command()
def query(
        sql:str = typer.Argument(None, help="SQL string or file to execute."),
        profile:str=typer.Option(None, help="The connection profile to use."),
        config:pathlib.Path=typer.Option("~/.config/squeal.toml", help="The config file to use."),
        download:pathlib.Path=typer.Option(None,
                                           help="Where to download the results to. Supports csv and parquet."),
        quiet:bool=typer.Option(False, help="Print out logging messages while executing.")):
    """
    Executes an SQL query and prints the results to stdout.
    """
    if '.sql' in sql.lower():
        console.log('Reading SQL file...')
        with open(sql,'r') as fin:
            sql = fin.read()

    if profile is not None:
        SQL.loadProfile(profile)

    SQL.quiet = quiet
    SQL.connect()
    
    df = SQL.query(sql)

    if download is None:
        console.print(_dfToTable(df))
    else:
        if '.parquet' in str(download).lower():
            df.to_parquet(download)
        elif '.csv' in str(download).lower():
            df.to_csv(download, index=False)

def main():
    app()
    

if __name__ == "__main__":
    main()
    # sql = SQLConnection(logging=True).connect()
    # console.print(sql("SELECT TOP 10 * FROM dw.DimBranch2"))
    
