import logging
import os

import psycopg2
from dotenv import load_dotenv

from functions_for_data_moving import (
    conn_postgres,
    conn_sqlite,
    sqlite_to_postgres_film_work,
    sqlite_to_postgres_genre,
    sqlite_to_postgres_genre_film_work,
    sqlite_to_postgres_person,
    sqlite_to_postgres_person_film_work,
)

load_dotenv()

if __name__ == "__main__":
    dsl = {
        "dbname": os.environ.get("DB_NAME"),
        "user": os.environ.get("DB_USER"),
        "password": os.environ.get("DB_PASSWORD"),
        "host": os.environ.get("DB_HOST"),
        "port": os.environ.get("DB_PORT"),
    }

    with conn_sqlite("db.sqlite") as conn_sqlt:
        curs_sqlt = conn_sqlt.cursor()
        with conn_postgres(dsl) as conn_pstgr:
            curs_pstgr = conn_pstgr.cursor()
            try:
                curs_pstgr.execute("""CREATE SCHEMA IF NOT EXISTS content;""")
                curs_pstgr.execute(
                    """CREATE TABLE IF NOT EXISTS content.film_work (
                    id uuid PRIMARY KEY,
                    title TEXT NOT NULL,
                    description TEXT,
                    creation_date DATE,
                    file_path TEXT,
                    rating FLOAT,
                    type TEXT not null,
                    created_at timestamp with time zone,
                    modified_at timestamp with time zone);"""
                )
                curs_pstgr.execute(
                    """CREATE TABLE IF NOT EXISTS content.person (
                    id uuid PRIMARY KEY,
                    full_name TEXT NOT NULL,
                    created_at timestamp with time zone,
                    modified_at timestamp with time zone);"""
                )
                curs_pstgr.execute(
                    """CREATE TABLE IF NOT EXISTS content.genre (
                    id uuid PRIMARY KEY,
                    name TEXT NOT NULL,
                    description TEXT,
                    created_at timestamp with time zone,
                    modified_at timestamp with time zone);"""
                )
                curs_pstgr.execute(
                    """CREATE TABLE IF NOT EXISTS content.genre_film_work (
                    id uuid PRIMARY KEY,
                    genre_id uuid NOT NULL,
                    film_work_id uuid NOT NULL,
                    created_at timestamp with time zone);"""
                )
                curs_pstgr.execute(
                    """CREATE TABLE IF NOT EXISTS content.person_film_work (
                    id uuid PRIMARY KEY,
                    person_id uuid NOT NULL,
                    film_work_id uuid NOT NULL,
                    role TEXT NOT NULL,
                    created_at timestamp with time zone);"""
                )

            except:
                logging.exception("Схема, таблицы и индексы уже созданы")

            try:
                count_sqlt = sqlite_to_postgres_film_work(conn_sqlt, conn_pstgr, 3)
                curs_pstgr.execute(
                    """
                SELECT COUNT(*) FROM content.film_work"""
                )
                count_pstgr = curs_pstgr.fetchone()
                assert count_sqlt[0] == count_pstgr[0]

                count_sqlt = sqlite_to_postgres_person(conn_sqlt, conn_pstgr, 3)
                curs_pstgr.execute(
                    """
                SELECT COUNT(*) FROM content.person"""
                )
                count_pstgr = curs_pstgr.fetchone()
                assert count_sqlt[0] == count_pstgr[0]

                count_sqlt = sqlite_to_postgres_genre(conn_sqlt, conn_pstgr, 3)
                curs_pstgr.execute(
                    """
                SELECT COUNT(*) FROM content.genre"""
                )
                count_pstgr = curs_pstgr.fetchone()
                assert count_sqlt[0] == count_pstgr[0]

                count_sqlt = sqlite_to_postgres_genre_film_work(conn_sqlt, conn_pstgr, 3)
                curs_pstgr.execute(
                    """
                SELECT COUNT(*) FROM content.genre_film_work"""
                )
                count_pstgr = curs_pstgr.fetchone()
                assert count_sqlt[0] == count_pstgr[0]

                count_sqlt = sqlite_to_postgres_person_film_work(conn_sqlt, conn_pstgr, 3)
                curs_pstgr.execute(
                    """
                SELECT COUNT(*) FROM content.person_film_work"""
                )
                count_pstgr = curs_pstgr.fetchone()
                assert count_sqlt[0] == count_pstgr[0]

                curs_pstgr.execute(
                    """
                CREATE UNIQUE INDEX film_work_genre_idx ON content.genre_film_work (genre_id, film_work_id);
                """
                )
                curs_pstgr.execute(
                    """
                CREATE UNIQUE INDEX film_work_person_idx ON content.person_film_work
                (film_work_id, person_id, role);
                """
                )
            except psycopg2.errors.InFailedSqlTransaction:
                logging.exception("Данные уже перенесены")

            conn_pstgr.commit()
