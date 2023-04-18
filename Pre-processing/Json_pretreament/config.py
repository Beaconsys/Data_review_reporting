import os
import lwfs_client
RAW_DATA_DIR = "/path1"
ANALYSE_DATA_PATH = "comp_analyse"

COMP_JSON_PATH = '/compjson'
FWD1_JSON_PATH = '/fwd1_compjson'
OST1_JSON_PATH = '/ost1_compjson'
JSON_PATH_MAP = {
    'comp': COMP_JSON_PATH,
    'fwd1': FWD1_JSON_PATH,
    'ost1': OST1_JSON_PATH,
}
FWD1_ANALYSE_PATH = '/fwd1_analyse/'
OST1_ANALYSE_PATH = '/ost1_analyse/'
COMP_ANALYSE_PATH = '/comp_analyse/'
ANALYSE_PATH_MAP = {
    'comp': COMP_ANALYSE_PATH,
    'fwd1': FWD1_ANALYSE_PATH,
    'ost1': OST1_ANALYSE_PATH,
}
