from unittest.mock import Mock


def get_object_methods(obj):
    return [
        method_name for method_name in dir(obj) if callable(getattr(obj, method_name))
    ]


def mock_object(cls, *args, **kwargs):
    obj = cls(*args, **kwargs)
    mock_obj = Mock()
    for method_name in get_object_methods(obj):
        try:
            getattr(mock_obj, method_name).side_effect = getattr(obj, method_name)
        except AttributeError:
            pass
    return mock_obj
