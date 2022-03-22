import logging
from functools import wraps
from .rpc import RPC


logger = logging.getLogger(__package__)


class NFSv4(RPC):
    pass