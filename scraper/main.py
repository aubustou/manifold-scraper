from __future__ import annotations

import argparse
import hashlib
import logging
from datetime import datetime
from pathlib import Path

from sqlalchemy import Boolean, Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.orm import Session, declarative_base, sessionmaker

session: Session

logger = logging.getLogger(__name__)


class MockSession:
    def add(self, model):
        logger.info("Adding %s", model)
        for column in model.__table__.columns:
            logger.info("  %s: %s", column.name, getattr(model, column.name))

    def commit(self):
        logger.info("Committing")

    def query(self, model):
        return self

    def filter_by(self, **kwargs):
        return self

    def first(self):
        return None


# Define the table model
Base = declarative_base()


class Model(Base):
    __tablename__ = "models"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    path = Column(String, nullable=False)
    library_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    preview_file_id = Column(Integer)
    creator_id = Column(Integer)
    notes = Column(Text)
    caption = Column(Text)
    collection_id = Column(Integer)
    slug = Column(String)
    license = Column(String)


class Creator(Base):
    """Table "public.creators"
       Column   |              Type              | Collation | Nullable |               Default
    ------------+--------------------------------+-----------+----------+--------------------------------------
     id         | bigint                         |           | not null | nextval('creators_id_seq'::regclass)
     name       | character varying              |           | not null |
     created_at | timestamp(6) without time zone |           | not null |
     updated_at | timestamp(6) without time zone |           | not null |
     notes      | text                           |           |          |
     caption    | text                           |           |          |
     slug       | character varying              |           |          |
    Indexes:
        "creators_pkey" PRIMARY KEY, btree (id)
        "index_creators_on_name" UNIQUE, btree (name)
        "index_creators_on_slug" UNIQUE, btree (slug)
    Referenced by:
        TABLE "models" CONSTRAINT "fk_rails_3b8b50d3f3" FOREIGN KEY (creator_id) REFERENCES creators(id)
    """

    __tablename__ = "creators"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    notes = Column(Text)
    caption = Column(Text)
    slug = Column(String)


class Collection(Base):
    """Table "public.collections"
        Column     |              Type              | Collation | Nullable |                 Default
    ---------------+--------------------------------+-----------+----------+-----------------------------------------
     id            | bigint                         |           | not null | nextval('collections_id_seq'::regclass)
     name          | character varying              |           |          |
     notes         | text                           |           |          |
     caption       | text                           |           |          |
     created_at    | timestamp(6) without time zone |           | not null |
     updated_at    | timestamp(6) without time zone |           | not null |
     collection_id | bigint                         |           |          |
     slug          | character varying              |           |          |
    Indexes:
        "collections_pkey" PRIMARY KEY, btree (id)
        "index_collections_on_collection_id" btree (collection_id)
        "index_collections_on_name" UNIQUE, btree (name)
        "index_collections_on_slug" UNIQUE, btree (slug)
    Foreign-key constraints:
        "fk_rails_63724415e9" FOREIGN KEY (collection_id) REFERENCES collections(id)
    Referenced by:
        TABLE "collections" CONSTRAINT "fk_rails_63724415e9" FOREIGN KEY (collection_id) REFERENCES collections(id)
        TABLE "models" CONSTRAINT "fk_rails_cdf016e15c" FOREIGN KEY (collection_id) REFERENCES collections(id)
    """

    __tablename__ = "collections"
    id = Column(Integer, primary_key=True)
    name = Column(String)
    notes = Column(Text)
    caption = Column(Text)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    collection_id = Column(Integer)
    slug = Column(String)


class ModelFile(Base):
    """Table "public.model_files"
             Column          |              Type              | Collation | Nullable |                 Default
    -------------------------+--------------------------------+-----------+----------+-----------------------------------------
     id                      | bigint                         |           | not null | nextval('model_files_id_seq'::regclass)
     filename                | character varying              |           |          |
     model_id                | bigint                         |           | not null |
     created_at              | timestamp(6) without time zone |           | not null |
     updated_at              | timestamp(6) without time zone |           | not null |
     presupported            | boolean                        |           | not null | false
     y_up                    | boolean                        |           | not null | false
     digest                  | character varying              |           |          |
     notes                   | text                           |           |          |
     caption                 | text                           |           |          |
     size                    | bigint                         |           |          |
     presupported_version_id | bigint                         |           |          |
    Indexes:
        "model_files_pkey" PRIMARY KEY, btree (id)
        "index_model_files_on_digest" btree (digest)
        "index_model_files_on_filename_and_model_id" UNIQUE, btree (filename, model_id)
        "index_model_files_on_model_id" btree (model_id)
        "index_model_files_on_presupported_version_id" btree (presupported_version_id)
    Foreign-key constraints:
        "fk_rails_a411caf13d" FOREIGN KEY (model_id) REFERENCES models(id)
        "fk_rails_b5ac05b6e3" FOREIGN KEY (presupported_version_id) REFERENCES model_files(id)
    Referenced by:
        TABLE "model_files" CONSTRAINT "fk_rails_b5ac05b6e3" FOREIGN KEY (presupported_version_id) REFERENCES model_files(id)
    """

    __tablename__ = "model_files"
    id = Column(Integer, primary_key=True)
    filename = Column(String)
    model_id = Column(Integer, nullable=False)
    created_at = Column(DateTime, nullable=False)
    updated_at = Column(DateTime, nullable=False)
    presupported = Column(Boolean, nullable=False)
    y_up = Column(Boolean, nullable=False)
    digest = Column(String)
    notes = Column(Text)
    caption = Column(Text)
    size = Column(Integer)
    presupported_version_id = Column(Integer)


def sanitize_path(root_folder, path):
    return path.relative_to(root_folder)


def insert_model_files(session, model_id, root_folder):
    for path in root_folder.iterdir():
        if path.is_file():
            logger.info("Processing file %s", path.name)
            model_file = ModelFile(
                filename=path.name,
                model_id=model_id,
                created_at=datetime.now(),
                updated_at=datetime.now(),
                presupported=False,
                y_up=False,
                digest=calculate_digest(path),
                notes="",
                caption="",
                size=path.stat().st_size,
                presupported_version_id=None,
            )
            session.add(model_file)
            session.commit()
        elif path.is_dir():
            logger.info("Processing directory %s", path.name)
            insert_model_files(session, model_id, path)


def calculate_digest(file):
    """In JS it's Digest::SHA512.new.file(pathname).hexdigest"""
    return hashlib.sha512(file.read_bytes()).hexdigest()


# Function to recursively traverse the folder structure and insert data into the table
def insert_folders(session, root_folder, library_id):
    for creator_path in root_folder.iterdir():
        logger.info("Processing creator %s", creator_path.name)
        for collection_path in creator_path.iterdir():
            logger.info("Processing collection %s", collection_path.name)
            for model_path in collection_path.iterdir():
                if not model_path.is_dir():
                    continue
                logger.info("Processing model %s", model_path.name)

                creator, collection, model_name, uuid = get_infos(model_path)

                creator_id = get_or_create_creator(session, creator).id
                collection_id = get_or_create_collection(session, collection, None).id
                model = Model(
                    name=model_path.name,
                    path=str(sanitize_path(root_folder, model_path)),
                    library_id=library_id,
                    created_at=datetime.now(),
                    updated_at=datetime.now(),
                    creator_id=creator_id,
                    collection_id=collection_id,
                )
                session.add(model)
                session.commit()
                insert_model_files(session, model.id, model_path)


def get_infos(path):
    creator, collection, model_name = path.parts[-3:]
    model_name, uuid = model_name.rsplit("-", 1)
    return creator, collection, model_name, uuid


# Function to create or retrieve a creator from the database
def get_or_create_creator(session, name):
    creator = session.query(Creator).filter_by(name=name).first()
    if creator:
        return creator
    else:
        creator = Creator(
            name=name,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        session.add(creator)
        session.commit()
        return creator


# Function to create or retrieve a collection from the database
def get_or_create_collection(session, name, collection_id):
    collection = (
        session.query(Collection)
        .filter_by(name=name, collection_id=collection_id)
        .first()
    )
    if collection:
        return collection
    else:
        collection = Collection(
            name=name,
            collection_id=collection_id,
            created_at=datetime.now(),
            updated_at=datetime.now(),
        )
        session.add(collection)
        session.commit()
        return collection


def init_engine(uri):
    engine = create_engine(uri)
    Session_ = sessionmaker(bind=engine)
    session = Session_()
    return session


def main():
    parser = argparse.ArgumentParser(
        description="Scrape data from a folder structure and insert it into a database."
    )
    parser.add_argument(
        "root_folder", type=Path, help="The root folder to scrape data from."
    )
    parser.add_argument(
        "library_id", type=int, help="The ID of the library to associate the data with."
    )
    parser.add_argument("psql_uri", type=str, help="The URI of the PostgreSQL database")
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Don't insert data into the database, just print it",
    )
    args = parser.parse_args()

    logging.basicConfig(level=logging.INFO)

    # Initialize the database connection
    if args.dry_run:
        session = MockSession()
    else:
        session = init_engine(args.psql_uri)

    # Call the function to insert data into the table
    insert_folders(session, args.root_folder, args.library_id)


if __name__ == "__main__":
    main()
