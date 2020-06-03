import confuse


_config = confuse.LazyConfig('azmeta', __name__)


def direct() -> confuse.Configuration:
    return _config
