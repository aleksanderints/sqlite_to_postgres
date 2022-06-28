import datetime
import uuid
from dataclasses import dataclass, field

from dotenv import load_dotenv

load_dotenv()


@dataclass
class FilmWork:
    title: str
    description: str
    creation_date: datetime.date = field()
    file_path: str
    type: str
    created_at: datetime.datetime
    modified_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    rating: float = field(default=0.0)


@dataclass
class FilmWork:
    title: str
    description: str
    creation_date: datetime.date = field()
    file_path: str
    type: str
    created_at: datetime.datetime
    modified_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
    rating: float = field(default=0.0)


@dataclass
class Person:
    full_name: str
    created_at: datetime.datetime
    modified_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class Genre:
    name: str
    description: str
    created_at: datetime.datetime
    modified_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class GenreFilmWork:
    film_work_id: str
    genre_id: str
    created_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)


@dataclass
class PersonFilmWork:
    film_work_id: str
    person_id: str
    role: str
    created_at: datetime.datetime
    id: uuid.UUID = field(default_factory=uuid.uuid4)
