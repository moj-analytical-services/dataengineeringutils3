from datetime import datetime
import json

from dataengineeringutils3.json import DateTimeEncoder


def test_json_encoder():
    json_dict = {
        "datetime": datetime(2111, 1, 1, 1, 1, 1)
    }
    json_str = json.dumps(json_dict, cls=DateTimeEncoder)
    assert json_str == """{"datetime": "2111-01-01T01:01:01"}"""
