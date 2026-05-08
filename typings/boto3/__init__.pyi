from typing import TypedDict

class S3ListObject(TypedDict):
    Key: str

class S3ListObjectsV2Response(TypedDict, total=False):
    Contents: list[S3ListObject]
    IsTruncated: bool
    NextContinuationToken: str

class S3Body:
    def read(self) -> bytes: ...

class S3GetObjectResponse(TypedDict):
    Body: S3Body

class S3Client:
    def list_objects_v2(self, **kwargs: object) -> S3ListObjectsV2Response: ...
    def get_object(self, **kwargs: object) -> S3GetObjectResponse: ...
    def put_object(self, **kwargs: object) -> dict[str, object]: ...
    def head_object(self, **kwargs: object) -> dict[str, object]: ...

def client(service_name: str, *args: object, **kwargs: object) -> S3Client: ...
