from configparser import ConfigParser


def dbconfig(filename,section):
    # create a parser
    parser = ConfigParser(interpolation=None)
    # read config file
    parser.read("resources/"+filename)

    # get section, default to postgresql
    db = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            db[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return db

def getconfig(filename,section):
    parser = ConfigParser(interpolation=None)
    # read config file
    parser.read("resources/" + filename)
    properties = {}
    if parser.has_section(section):
        params = parser.items(section)
        for param in params:
            properties[param[0]] = param[1]
    else:
        raise Exception('Section {0} not found in the {1} file'.format(section, filename))

    return properties