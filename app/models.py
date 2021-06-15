from pydantic import BaseModel
from typing import List, Union


class Column(BaseModel):
    col_names: List[str]
    col_types: List[str]
    col_values: List


class InsertActivity(Column):
    operation: str
    table: str


class DeleteActivity(BaseModel):
    operation: str
    table: str
    old_value: Column


class Payload(BaseModel):
    activities: List[Union[InsertActivity, DeleteActivity]]
