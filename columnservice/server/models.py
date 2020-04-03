from enum import Enum
from pydantic import BaseModel


class CatalogAlgorithmType(str, Enum):
    prefix = "prefix"


class CatalogAlgorithm(BaseModel):
    algo: CatalogAlgorithmType
    prefix: str
