
import datetime
import re



def get_datetime_from_filename(filename):
    """
    Assuming a file has the date written in the filename or path,
    will get the first occurrence of this datetime string and return
    the datetime it stores.
    Format Year Month Day T Hour Minute Seconds all together and 0-padded
    example: _20191214T093535_
    """
    regex = re.compile('_(?P<datetime>\d{8}T\d{6})_')
    datestr = None
    match = regex.search(filename)
    if match is None:
        raise ValueError("Could not find any date time in {}".format(filename))
    datestr = match.group('datetime')
    return datetime.datetime.strptime(datestr, '%Y%m%dT%H%M%S')
