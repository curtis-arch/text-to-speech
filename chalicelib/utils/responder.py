import json
from typing import Any, Dict

from chalice import Response


class Responder(object):
    """
    Utility class to create Chalice responses.
    """

    @staticmethod
    def respond(payload: Dict[Any, Any], status_code: int = 200) -> Response:
        """
        Returns a new json `Response` using the given `payload` and `status_code`.
        :param payload: a dict which will be converted to a json string
        :param status_code: a http status code
        :return: Response
        """
        if payload and "note_attributes" in payload:
            payload["note_attributes"] = [attr.to_dict() for attr in payload["note_attributes"]]

        return Response(
            body=json.dumps(payload),
            status_code=status_code,
            headers={"Content-Type": "application/json"}
        )

    @staticmethod
    def error(error_message: str, status_code: int) -> Response:
        """
        Returns a new json `Response` using the given `error_message` and `status_code`.
        :param error_message: the user friendly, english error message
        :param status_code: a http status code
        :return: Response
        """
        return Responder.respond(payload={"error": error_message}, status_code=status_code)