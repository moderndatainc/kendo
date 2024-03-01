from typing import List, Optional
from pydantic import BaseModel

class ICaughtException(BaseModel):
    message: Optional[str] = None