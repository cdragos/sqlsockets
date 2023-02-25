from collections import defaultdict
from uuid import uuid4

from pydantic import BaseModel, Field
from pydantic.types import UUID4
from sqlalchemy import Column, ForeignKey, MetaData, Table, create_engine, text, insert, select
from sqlalchemy.dialects import postgresql


DATABASE_NAME = "sqlalchemy_test"

# Drop and recreate the tables each run for a quick development loop
psql_engine = create_engine("postgresql+psycopg2://localhost:5432/postgres")
with psql_engine.connect().execution_options(
   isolation_level="AUTOCOMMIT"
) as connection:
   connection.execute(text(f"DROP DATABASE IF EXISTS {DATABASE_NAME}"))
   connection.execute(text(f"CREATE DATABASE {DATABASE_NAME}"))

engine = create_engine(f"postgresql+psycopg2://localhost:5432/{DATABASE_NAME}")

# Create a few very simple pydantic models


class Child(BaseModel):
   id: UUID4 = Field(default_factory=uuid4)


class Parent(BaseModel):
   id: UUID4 = Field(default_factory=uuid4)
   children: list[Child]


## Write the schema definitions

metadata_obj = MetaData()

parent_table = Table(
   "parent",
   metadata_obj,
   Column("user_id", postgresql.UUID(), primary_key=True),
)

child_table = Table(
   "child",
   metadata_obj,
   Column(
      "id",
      postgresql.UUID(as_uuid=True),
      primary_key=True,
      default=uuid4
   ),
)

parent_child_table = Table(
   "parent_child",
   metadata_obj,
   Column(
      "parent_id",
      postgresql.UUID(as_uuid=True),
      ForeignKey("parent.user_id"),
      primary_key=True,
   ),
   Column(
      "child_id",
      postgresql.UUID(as_uuid=True),
      ForeignKey("child.id"),
      primary_key=True,
   ),
)


# Other requied tables
metadata_obj.create_all(engine)


# Write a database socket which can write and query pydantic models
class ParentSocket:

   def create_many(self, objs: list[Parent]) -> int:
      """Bulk creats a list of parent objects and returns the number inserted."""
      if not objs:
         return 0

      parent_instances = []
      child_instances = []
      parent_child_instances = []

      inserted_children = set()
      inserted_parent_children = set()

      with engine.connect() as conn:
         for obj in objs:
            parent_id = str(obj.id)
            parent_instances.append({'user_id': parent_id})
            for child in obj.children:
               if child.id not in inserted_children:
                  child_instances.append({'id': str(child.id)})
                  inserted_children.add(child.id)
                  parent_child_instances.append({'parent_id': parent_id, 'child_id': str(child.id)})

         # Use a transaction to ensure all inserts are successful
         with conn.begin():
            # create the parent instances and return the ids
            query = insert(parent_table).values(parent_instances).returning(parent_table.c.user_id)
            result = conn.execute(query)

            if child_instances:
               # create the child instances
               query = insert(child_table).values(child_instances)
               conn.execute(query)
               # create the relationship between parent and child instances
               query = insert(parent_child_table).values(parent_child_instances)
               conn.execute(query)

            parent_ids = result.fetchall()
            return len(parent_ids)

   def query(self, id: UUID4 | list[UUID4]) -> list[Parent] | Parent:
      """Query either a single id or multiple ids from the Parent table and return pydantic objects."""
      with engine.connect() as conn:
         child_query = select(
            parent_child_table.c.parent_id,
            child_table.c.id,
         ).select_from(
            child_table.join(parent_child_table, child_table.c.id == parent_child_table.c.child_id)
         )
         if isinstance(id, list):
            child_query = child_query.where(
               parent_child_table.c.parent_id.in_(id)
            )
         else:
            child_query = child_query.where(
               parent_child_table.c.parent_id == id
            )
         # Use the sqlalchemy result proxy to return a list of dictionaries
         rows = conn.execute(child_query).mappings().fetchall()

         # Use a defaultdict to group the children by parent
         parents = defaultdict(list)
         for row in rows:
            parents[row["parent_id"]].append(row["id"])

         # Create the pydantic objects
         results = []
         for parent_id, children in parents.items():
            results.append(Parent(id=parent_id, children=[Child(id=child_id) for child_id in children]))

         return results if isinstance(id, list) else results[0]


parent_socket = ParentSocket()

# Create several children
parent1 = Parent(children=[Child(), Child()])
parent2 = Parent(children=[Child()])
parent3 = Parent(children=[Child(), parent1.children[0]])

assert parent_socket.create_many([parent1, parent2, parent3]) == 3
assert parent_socket.query(id=parent1.id).id == parent1.id

# Write any additional tests as desired!
assert len(parent_socket.query(id=parent1.id).children) == 2
assert parent_socket.query(id=parent1.id).children[0].id == parent1.children[0].id
assert parent_socket.query(id=parent1.id).children[1].id == parent1.children[1].id
assert parent_socket.query(id=[parent1.id, parent2.id]) == [parent1, parent2]
