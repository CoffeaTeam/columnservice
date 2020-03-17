from enum import Enum
from pydantic import BaseModel, Extra
from bson import ObjectId
from pydantic.json import ENCODERS_BY_TYPE

ENCODERS_BY_TYPE[ObjectId] = str


class DatasetSource(str, Enum):
    dbs_global = "dbs_global"
    dbs_phys03 = "dbs_phys03"
    user = "user"


class DatasetType(str, Enum):
    data = "data"
    mc = "mc"


class Dataset(BaseModel):
    class Config:
        extra = Extra.allow

    name: str
    source: DatasetSource = DatasetSource.dbs_global
    dataset_type: DatasetType
