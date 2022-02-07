#!/usr/bin/env python3
# thoth-document-sync
# Copyright(C) 2022 Red Hat, Inc.
#
# This program is free software: you can redistribute it and / or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This program is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with this program. If not, see <http://www.gnu.org/licenses/>.

"""Sync Thoth documents to an S3 API compatible remote."""

import json
import logging
import os
import subprocess
import threading
from concurrent.futures.thread import ThreadPoolExecutor
from datetime import date
from datetime import timedelta
from tempfile import NamedTemporaryFile
from typing import Optional

import click
from thoth.common import init_logging
from thoth.storages import __version__ as __thoth_storages_version__
from thoth.storages import AnalysisByDigest
from thoth.storages import AnalysisResultsStore
from thoth.storages import SolverResultsStore
from thoth.storages.result_base import ResultStorageBase

from prometheus_client import CollectorRegistry, Gauge, Counter, push_to_gateway

prometheus_registry = CollectorRegistry()

__version__ = "0.3.0"
__component_version__ = f"{__version__}+storages{__thoth_storages_version__}"

init_logging()
_LOGGER = logging.getLogger("thoth.document_sync")

_DEFAULT_CONCURRENCY = 16
_THOTH_DEPLOYMENT_NAME = os.environ["THOTH_DEPLOYMENT_NAME"]
_THOTH_METRICS_PUSHGATEWAY_URL = os.getenv("PROMETHEUS_PUSHGATEWAY_URL")

# Metrics Document sync
_METRIC_INFO = Gauge(
    "thoth_document_sync_job_info",
    "Thoth Document Sync Job information",
    ["env", "version"],
    registry=prometheus_registry,
)

_METRIC_DOCUMENTS_SYNC_NUMBER = Counter(
    "thoth_document_sync_job",
    "Thoth Document Sync Job number of synced documents status",
    ["sync_type", "env", "version", "result_type"],
    registry=prometheus_registry,
)

_METRIC_DOCUMENTS_SYNC_NUMBER_THREAD_LOCK = threading.Lock()

_METRIC_INFO.labels(_THOTH_DEPLOYMENT_NAME, __component_version__).inc()


def _sync_worker(adapter: ResultStorageBase, document_id: str, dst: str, *, force: bool = False) -> None:
    """Sync document logic."""
    destination = f"{dst}/{adapter.RESULT_TYPE}/{document_id}"
    proc = subprocess.run(["aws", "s3", "ls", destination], capture_output=True)

    if proc.returncode == 0 and not force:
        _LOGGER.info("Document %r is already present", document_id)
        with _METRIC_DOCUMENTS_SYNC_NUMBER_THREAD_LOCK:
            _METRIC_DOCUMENTS_SYNC_NUMBER.labels(
                sync_type="skipped",
                env=_THOTH_DEPLOYMENT_NAME,
                version=__component_version__,
                result_type=adapter.RESULT_TYPE,
            ).inc()
        return

    _LOGGER.info("Copying document to %r", destination)

    document = adapter.retrieve_document(document_id)
    with NamedTemporaryFile(mode="w") as temp:
        temp.write(json.dumps(document, sort_keys=True, indent=2))
        temp.flush()

        proc = subprocess.run(
            [
                "aws",
                "s3",
                "cp",
                temp.name,
                destination,
            ],
            capture_output=True,
        )
        if proc.returncode != 0:
            _LOGGER.error(
                "Failed to copy %r to %r (exit code %d): %s",
                document_id,
                destination,
                proc.returncode,
                proc.stderr,
            )
            with _METRIC_DOCUMENTS_SYNC_NUMBER_THREAD_LOCK:
                _METRIC_DOCUMENTS_SYNC_NUMBER.labels(
                    sync_type="error",
                    env=_THOTH_DEPLOYMENT_NAME,
                    version=__component_version__,
                    result_type=adapter.RESULT_TYPE,
                ).inc()
        else:
            _LOGGER.info("Document %r successfully copied to %r", document_id, destination)
            with _METRIC_DOCUMENTS_SYNC_NUMBER_THREAD_LOCK:
                _METRIC_DOCUMENTS_SYNC_NUMBER.labels(
                    sync_type="success",
                    env=_THOTH_DEPLOYMENT_NAME,
                    version=__component_version__,
                    result_type=adapter.RESULT_TYPE,
                ).inc()


@click.command()
@click.option("--debug", is_flag=True, help="Run in a debug mode", envvar="THOTH_SYNC_DEBUG")
@click.option(
    "--force",
    is_flag=True,
    help="Perform force copy of documents.",
    envvar="THOTH_DOCUMENT_SYNC_FORCE",
)
@click.option(
    "--concurrency",
    type=int,
    help="Number of concurrent workers syncing documents.",
    envvar="THOTH_DOCUMENT_SYNC_CONCURRENCY",
    default=_DEFAULT_CONCURRENCY,
)
@click.option(
    "--days",
    type=int,
    help="Number of days to the past to sync only more recent documents.",
    envvar="THOTH_DOCUMENT_SYNC_DAYS",
    default=None,
)
@click.argument(
    "dst",
    envvar="THOTH_DOCUMENT_SYNC_DST",
    type=str,
    metavar="s3://thoth/data/deployment",
)
def sync(
    dst: str,
    debug: bool = False,
    force: bool = False,
    concurrency: int = _DEFAULT_CONCURRENCY,
    days: Optional[int] = None,
) -> None:
    """Sync Thoth data to a remote with an S3 compatible interface."""
    if debug:
        _LOGGER.setLevel(logging.DEBUG)
        _LOGGER.debug("Debug mode is on.")

    _LOGGER.info("Running document syncing job in version %r", __component_version__)

    start_date = None
    if days:
        start_date = date.today() - timedelta(days=days)
        _LOGGER.info("Listing documents created since %r", start_date.isoformat())

    adapters = (AnalysisByDigest, AnalysisResultsStore, SolverResultsStore)
    try:
        for adapter_class in adapters:
            _LOGGER.info("Listing %r documents", adapter_class.RESULT_TYPE)
            adapter = adapter_class()
            adapter.connect()

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                for document_id in adapter.get_document_listing(start_date=start_date):
                    executor.submit(_sync_worker, adapter, document_id, dst, force=force)
    finally:
        if _THOTH_METRICS_PUSHGATEWAY_URL:
            try:
                _LOGGER.debug(f"Submitting metrics to Prometheus pushgateway {_THOTH_METRICS_PUSHGATEWAY_URL}")
                push_to_gateway(
                    _THOTH_METRICS_PUSHGATEWAY_URL,
                    job="document-sync-job",
                    registry=prometheus_registry,
                )
            except Exception as e:
                _LOGGER.exception(f"An error occurred pushing the metrics: {str(e)}")

    _LOGGER.info("Document sync job has finished successfully")


__name__ == "__main__" and sync()
