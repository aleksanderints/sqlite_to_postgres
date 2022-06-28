import sqlite3
from contextlib import contextmanager

import psycopg2
from psycopg2.extras import DictCursor, execute_values

from dataclasses_for_data_moving import FilmWork, Genre, GenreFilmWork, Person, PersonFilmWork


@contextmanager
def conn_sqlite(db_path: str):
    conn = sqlite3.connect(db_path)
    yield conn
    conn.close()


@contextmanager
def conn_postgres(dsl: dict):
    conn = psycopg2.connect(**dsl, cursor_factory=DictCursor)
    yield conn
    conn.close()


def sqlite_to_postgres_film_work(conn_sqlt: sqlite3.Connection, conn_pstgr: psycopg2.connect, n: int) -> int:
    """Возвращает количество строк в таблице базe sqlite.

    Args:
        conn_sqlt: sqlite3.Connection,
        conn_pstgr: psycopg2.connect,
        n: Количество строк для fetchmany.

    Returns:
        Количество строк в таблице базe sqlite.
    """
    curs_sqlt = conn_sqlt.cursor()
    curs_sqlt.execute(
        """
    SELECT COUNT(*) FROM film_work"""
    )
    count_sqlt = curs_sqlt.fetchone()
    curs_pstgr = conn_pstgr.cursor()
    curs_sqlt.execute("select * from film_work")
    while data := curs_sqlt.fetchmany(n):
        data_list = []
        for s in data:
            (
                id,
                title,
                description,
                creation_date,
                file_path,
                rating,
                type,
                created_at,
                modified_at,
            ) = s
            class_inst = FilmWork(
                id=id,
                title=title,
                description=description,
                creation_date=creation_date,
                file_path=file_path,
                rating=rating,
                type=type,
                created_at=created_at,
                modified_at=modified_at,
            )
            data_list.append(
                (
                    class_inst.id,
                    class_inst.title,
                    class_inst.description,
                    class_inst.creation_date,
                    class_inst.rating,
                    class_inst.type,
                    class_inst.created_at,
                    class_inst.modified_at,
                )
            )
        execute_values(
            curs_pstgr,
            "INSERT INTO content.film_work (id, title, description, creation_date, rating, type, created_at, modified_at) VALUES %s on conflict (id) do nothing",
            data_list,
        )
    return count_sqlt


def sqlite_to_postgres_person(conn_sqlt: sqlite3.Connection, conn_pstgr: psycopg2.connect, n: int) -> int:
    """Возвращает количество строк в таблице базe sqlite.

    Args:
        conn_sqlt: sqlite3.Connection,
        conn_pstgr: psycopg2.connect,
        n: Количество строк для fetchmany.

    Returns:
        Количество строк в таблице базe sqlite.
    """
    curs_sqlt = conn_sqlt.cursor()
    curs_sqlt.execute(
        """
    SELECT COUNT(*) FROM person"""
    )
    count_sqlt = curs_sqlt.fetchone()
    curs_pstgr = conn_pstgr.cursor()
    curs_sqlt.execute("select * from person")
    while data := curs_sqlt.fetchmany(n):
        data_list = []
        for s in data:
            (id, full_name, created_at, modified_at) = s
            class_inst = Person(id=id, full_name=full_name, created_at=created_at, modified_at=modified_at)
            data_list.append(
                (
                    class_inst.id,
                    class_inst.full_name,
                    class_inst.created_at,
                    class_inst.modified_at,
                )
            )
        execute_values(
            curs_pstgr,
            "INSERT INTO content.person (id, full_name, created_at, modified_at) VALUES %s on conflict (id) do nothing",
            data_list,
        )
    return count_sqlt


def sqlite_to_postgres_genre(conn_sqlt: sqlite3.Connection, conn_pstgr: psycopg2.connect, n: int) -> int:
    """Возвращает количество строк в таблице базe sqlite.

    Args:
        conn_sqlt: sqlite3.Connection,
        conn_pstgr: psycopg2.connect,
        n: Количество строк для fetchmany.

    Returns:
        Количество строк в таблице базe sqlite.
    """
    curs_sqlt = conn_sqlt.cursor()
    curs_sqlt.execute(
        """
    SELECT COUNT(*) FROM genre"""
    )
    count_sqlt = curs_sqlt.fetchone()
    curs_pstgr = conn_pstgr.cursor()
    curs_sqlt.execute("select * from genre")
    while data := curs_sqlt.fetchmany(n):
        data_list = []
        for s in data:
            (id, name, description, created_at, modified_at) = s
            class_inst = Genre(
                id=id,
                name=name,
                description=description,
                created_at=created_at,
                modified_at=modified_at,
            )
            data_list.append(
                (
                    class_inst.id,
                    class_inst.name,
                    class_inst.description,
                    class_inst.created_at,
                    class_inst.modified_at,
                )
            )
        execute_values(
            curs_pstgr,
            "INSERT INTO content.genre (id, name, description, created_at, modified_at) VALUES %s on conflict (id) do nothing",
            data_list,
        )
    return count_sqlt


def sqlite_to_postgres_genre_film_work(conn_sqlt: sqlite3.Connection, conn_pstgr: psycopg2.connect, n: int) -> int:
    """Возвращает количество строк в таблице базe sqlite.

    Args:
        conn_sqlt: sqlite3.Connection,
        conn_pstgr: psycopg2.connect,
        n: Количество строк для fetchmany.

    Returns:
        Количество строк в таблице базe sqlite.
    """
    curs_sqlt = conn_sqlt.cursor()
    curs_sqlt.execute(
        """
    SELECT COUNT(*) FROM genre_film_work"""
    )
    count_sqlt = curs_sqlt.fetchone()
    curs_pstgr = conn_pstgr.cursor()
    curs_sqlt.execute("select * from genre_film_work")
    while data := curs_sqlt.fetchmany(n):
        data_list = []
        for s in data:
            (id, film_work_id, genre_id, created_at) = s
            class_inst = GenreFilmWork(id=id, genre_id=genre_id, film_work_id=film_work_id, created_at=created_at)
            data_list.append(
                (
                    class_inst.id,
                    class_inst.genre_id,
                    class_inst.film_work_id,
                    class_inst.created_at,
                )
            )
        execute_values(
            curs_pstgr,
            "INSERT INTO content.genre_film_work (id, genre_id, film_work_id, created_at) VALUES %s on conflict do nothing",
            data_list,
        )
    return count_sqlt


def sqlite_to_postgres_person_film_work(conn_sqlt: sqlite3.Connection, conn_pstgr: psycopg2.connect, n: int) -> int:
    """Возвращает количество строк в таблице базe sqlite.

    Args:
        conn_sqlt: sqlite3.Connection,
        conn_pstgr: psycopg2.connect,
        n: Количество строк для fetchmany.

    Returns:
        Количество строк в таблице базe sqlite.
    """
    curs_sqlt = conn_sqlt.cursor()
    curs_sqlt.execute(
        """
    SELECT COUNT(*) FROM person_film_work"""
    )
    count_sqlt = curs_sqlt.fetchone()
    curs_pstgr = conn_pstgr.cursor()
    curs_sqlt.execute("select * from person_film_work")
    while data := curs_sqlt.fetchmany(n):
        data_list = []
        for s in data:
            (id, film_work_id, person_id, role, created_at) = s
            class_inst = PersonFilmWork(
                id=id,
                person_id=person_id,
                film_work_id=film_work_id,
                role=role,
                created_at=created_at,
            )
            data_list.append(
                (
                    class_inst.id,
                    class_inst.person_id,
                    class_inst.film_work_id,
                    class_inst.role,
                    class_inst.created_at,
                )
            )
        execute_values(
            curs_pstgr,
            "INSERT INTO content.person_film_work (id, person_id, film_work_id, role, created_at) VALUES %s on conflict (id) do nothing",
            data_list,
        )
    return count_sqlt
