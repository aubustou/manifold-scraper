import argparse
import datetime
from pathlib import Path

from sqlalchemy import Column, DateTime, Integer, String, Text, create_engine
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import Session, sessionmaker

session: Session

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


def sanitize_path(root_folder, path):
    return path.relative_to(root_folder)


# Function to recursively traverse the folder structure and insert data into the table
def insert_folders(session, root_folder, library_id):
    for creator in root_folder.iterdir():
        for collection in creator.iterdir():
            for model in collection.iterdir():
                if model.is_dir():
                    creator, collection, model_name, uuid = get_infos(model)

                    creator_id = get_or_create_creator(session, creator.name).id
                    collection_id = get_or_create_collection(
                        session, collection.name, None
                    ).id
                    model = Model(
                        name=model.name,
                        path=str(sanitize_path(root_folder, model)),
                        library_id=library_id,
                        created_at=datetime.now(),
                        updated_at=datetime.now(),
                        creator_id=creator_id,
                        collection_id=collection_id,
                    )
                    session.add(model)
                    session.commit()


def get_infos(path):
    creator, collection, model_name = path.parts[-3:]
    model_name, uuid = model_name.split("-")
    return creator, collection, model_name, uuid


# Function to create or retrieve a creator from the database
def get_or_create_creator(session, name):
    creator = session.query(Creator).filter_by(name=name).first()
    if creator:
        return creator
    else:
        creator = Creator(
            name=name,
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
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
            created_at=datetime.datetime.now(),
            updated_at=datetime.datetime.now(),
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
    args = parser.parse_args()

    # Initialize the database connection
    session = init_engine(args.psql_uri)

    # Call the function to insert data into the table
    insert_folders(session, args.root_folder, args.library_id)


if __name__ == "__main__":
    main()
