from dataclasses import dataclass
from typing import Optional


@dataclass
class User:
    """
    User entity class.

    Attributes:
        name (str): The name of the user.
        project (str): The project associated with the user.
        role (str): The role of the user.
        email (str): The email of the user.
        last_login (str): The last login time of the user.
    """
    name: str
    project: str
    role: str
    email: Optional[str] = None
    last_login: Optional[str] = None
    