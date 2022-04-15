import contextvars
import logging

import pytest
from basin3d.core.monitor import get_ctx_basin3d_where, get_ctx_synthesis_id, set_ctx_basin3d_where, set_ctx_synthesis_id


@pytest.fixture(scope="module")
def log():
    """Configure logging for the test fnction"""

    from basin3d.core.monitor import configure, get_logger
    configure(loggers={"basin3d": {"level": "DEBUG"}}, root={"level": "DEBUG", "handlers": ["console"]})
    return get_logger("basin3d")


def test_config_error():
    """Test when there is a logging config exception"""

    from basin3d.core.monitor import configure, get_logger
    pytest.raises(Exception, configure, loggers=object())


def test_log_where(caplog, log):

    """Test that the elapse time logging works and is accurate"""

    assert get_ctx_synthesis_id() is None
    assert get_ctx_basin3d_where() is None
    assert isinstance(set_ctx_basin3d_where(), contextvars.Token)

    logger = log
    set_ctx_basin3d_where("test_log_where")
    set_ctx_synthesis_id()
    assert get_ctx_synthesis_id() is not None
    assert get_ctx_basin3d_where() == "test_log_where"

    with caplog.at_level(logging.INFO, logger="basin3d"):

        logger.log(logging.INFO, "A message")

        assert len(caplog.messages) == 1
        assert caplog.messages[0] == "A message"
        assert hasattr(caplog.records[0], "basin3d_where")
        assert hasattr(caplog.records[0], "synthesis_id")
        assert caplog.records[0].basin3d_where == "test_log_where"
        assert caplog.records[0].synthesis_id != "*"


