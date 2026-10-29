# SPDX-License-Identifier: Apache-2.0
"""Shared test setup.

gRPC (the Spark Connect client) installs fork handlers that print to a child's stderr when the test process later
runs the CLI as a subprocess; the CLI tests compare stderr exactly. Fork support is off and gRPC logs only errors,
set before anything imports grpc.
"""
import gc
import os

import pytest

os.environ.setdefault("GRPC_ENABLE_FORK_SUPPORT", "false")
os.environ.setdefault("GRPC_VERBOSITY", "ERROR")


@pytest.fixture(autouse=True)
def _finalize_on_the_main_thread():
    """A Spark session a test drops is finalized here, on the main thread: the 3.5 client deadlocks when its
    finalizer runs inside one of the client's own worker threads."""
    yield
    gc.collect()
