import time
from unittest.mock import Mock


def get_object_attrs(obj):
    """Lists all attributes in an object"""
    return [attr_name for attr_name in dir(obj)]


def mock_object(cls, *args, **kwargs):
    """
    Creates a Mock object from a class and copies all attributes over so we can
    call `assert_called_with` etc on the object
    """
    obj = cls(*args, **kwargs)
    mock_obj = Mock()
    for attr_name in get_object_attrs(obj):
        try:
            getattr(mock_obj, attr_name).side_effect = getattr(obj, attr_name)
        except AttributeError:
            if attr_name == "__iter__":
                setattr(mock_obj, attr_name, getattr(obj, attr_name))
                mock_obj.__iter__.return_value = obj.__iter__()
                continue
    return mock_obj


def time_func(func, *args, **kwargs):
    start = time.time()
    func(*args, **kwargs)
    end = time.time()
    return end - start
