from typing import List


class PyFileStatus:
    path: str
    length: int
    isdir: bool
    permission: int
    owner: str
    group: str
    modification_time: int
    access_time: int


class Client:
    def __init__(url: str) -> Client: ...

    def get_file_info(self, path: str) -> PyFileStatus: ...

    def list_status(self, path: str, recursive: bool) -> List[PyFileStatus]: ...