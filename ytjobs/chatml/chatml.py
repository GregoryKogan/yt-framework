from typing import Optional
from pathlib import Path
from dataclasses import dataclass
import uuid


@dataclass
class ImageInfo:
    path: str
    width: int
    height: int

    @property
    def name(self):
        return Path(self.path).name


class ChatMLSingleConversation:
    def __init__(self, data: Optional[list] = None):
        self._data = data or [
            {"role": "system", "content": "", "trainable": False, "message_index": 0},
        ]
        self.index = self.data[-1]["message_index"]

    def add_qa(self, q: str, a: str, images: Optional[list[ImageInfo]] = None):
        images = images or []
        self.index += 1
        self._data.append(
            {
                "role": "user",
                "content": q,
                "files": [
                    {
                        "id": str(uuid.uuid4()),
                        "name": image.name,
                        "type": "image",
                        "embed": True,
                        "path": image.path,
                        "width": image.width,
                        "height": image.height,
                    }
                    for image in images
                ],
                "trainable": False,
                "message_index": self.index,
            }
        )
        self.index += 1
        self._data.append(
            {
                "role": "assistant",
                "content": a,
                "trainable": True,
                "message_index": self.index,
            }
        )

    @property
    def data(self):
        return self._data
