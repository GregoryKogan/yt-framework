class ClientError(Exception):
    response: dict[str, object]
