from datetime import datetime
import json


class DateTimeEncoder(json.JSONEncoder):
    """
    Json encoder that handles datetime objects and formats them to iso format.

    json_dict = {
        "datetime": datetime(2111, 1, 1, 1, 1, 1),
        "a": "b"
    }
    json_str = json.dumps(json_dict, cls=DateTimeEncoder)
    """

    def default(self, o):
        if isinstance(o, datetime):
            return o.isoformat()
        return json.JSONEncoder.default(self, o)
