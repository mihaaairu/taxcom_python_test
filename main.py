import json
import os
import asyncio
import pandas as pd

from pathlib import Path
from databases import Database
from dotenv import load_dotenv

load_dotenv()

FILE_1_PATH = './io_files/Тестовый файл1.txt'
FILE_2_PATH = './io_files/Тестовый файл2.txt'
JSON_PATH = './io_files/combined_table.json'

TABLE_COLUMN_NAMES = ['id', 'name', 'description']

db_connection = Database(f"postgresql+asyncpg://"
                         f"{os.environ['POSTGRES_USER']}:"
                         f"{os.environ['POSTGRES_PASSWORD']}@localhost:5432/"
                         f"{os.environ['POSTGRES_DB']}")


def prepare_data(file_1_path: Path | str, file_1_encoding: str,
                 file_2_path: Path | str, file_2_encoding: str) -> pd.DataFrame:
    """
    Load txt files, prepare them and combine in one dataframe.
    """
    # Reading files. Using dtype=str to save leading zeros in ID
    df_1 = pd.read_csv(file_1_path, delimiter=',', header=None, names=[*TABLE_COLUMN_NAMES], encoding=file_1_encoding,
                       dtype=str)
    df_2 = pd.read_csv(file_2_path, delimiter=';', header=None, names=['name', 'id'], encoding=file_2_encoding,
                       dtype=str)

    # Combining two files to one table
    combined_df = pd.concat([df_1, df_2])

    # Normalising values
    combined_df = combined_df.map(lambda x: x.strip().replace('"', '') if isinstance(x, str) else x)

    # Sorting table by the second text field
    return combined_df.sort_values(by='name').reset_index(drop=True)


def export_json(df: pd.DataFrame) -> None:
    """
    Saves dataframe as json file.
    """
    json_dict = df.to_dict(orient='records')
    with open(JSON_PATH, 'w', encoding='utf-8') as json_file:
        json.dump(json_dict, json_file, ensure_ascii=False, indent=4)


def import_json(json_file_path: Path | str) -> pd.DataFrame:
    """
    :param json_file_path: Path
    :return: Json object
    """
    return pd.read_json(json_file_path, encoding='utf-8', dtype=str)  # Using dtype=str to save leading zeros in ID


async def upsert_json_to_database(json_df: pd.DataFrame) -> None:
    """
    Updates or inserts json to database
    :param json_df: Source dataframe
    """
    query = f"""
        INSERT INTO {os.environ['TABLE_NAME']} (id, name, description)
        VALUES (:id, :name, :description)
        ON CONFLICT (id) DO UPDATE SET
            name = EXCLUDED.name,
            description = EXCLUDED.description;
        """
    await db_connection.execute_many(query, json_df.to_dict('records'))


async def select_from_database() -> pd.DataFrame:
    query = f"SELECT * FROM {os.environ['TABLE_NAME']}"
    return pd.DataFrame([dict(row) for row in await db_connection.fetch_all(query)])


async def main():
    # Prepare data using txt files.
    prepared_data_df = prepare_data(FILE_1_PATH, 'utf-8', FILE_2_PATH, 'cp1251')
    # Save dataframe to json file in JSON_PATH
    export_json(prepared_data_df)

    # Read json from file.
    json_df = import_json(JSON_PATH)
    # Write it to database
    await db_connection.connect()
    await upsert_json_to_database(json_df)
    # Check the results
    print(await select_from_database())
    await db_connection.disconnect()


if __name__ == "__main__":
    asyncio.run(main())


