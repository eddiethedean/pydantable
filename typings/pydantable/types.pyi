from typing import Any

from pydantic import GetCoreSchemaHandler
from pydantic_core import CoreSchema

class WKB(bytes):
    @classmethod
    def __get_pydantic_core_schema__(
        cls, source_type: Any, handler: GetCoreSchemaHandler
    ) -> CoreSchema: ...
