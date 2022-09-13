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

from __future__ import annotations

import logging
import os
import re
import itertools
from importlib_metadata import version
from concurrent.futures.thread import ThreadPoolExecutor
from concurrent.futures import as_completed
from datetime import date
from datetime import timedelta
from boto3 import client
from botocore.exceptions import ClientError

from enum import Enum
from collections import Counter, defaultdict
from typing import cast, Optional, Iterable, Tuple, Type, TYPE_CHECKING

if TYPE_CHECKING:
    from mypy_boto3_s3.client import S3Client

import click
from thoth.common import init_logging
from thoth.storages import AnalysisByDigest
from thoth.storages import AnalysisResultsStore
from thoth.storages import SolverResultsStore
from thoth.storages import CephStore
from thoth.storages.result_base import ResultStorageBase

from prometheus_client import CollectorRegistry, Gauge, Counter as PromCounter, push_to_gateway
from .lazy_set_ops import sorted_iter_set_difference

prometheus_registry = CollectorRegistry()

__component_version__ = f"{version('document-sync-job')}+storages{version('thoth-storages')}"

init_logging()
_LOGGER = logging.getLogger("thoth.document_sync")

_DEFAULT_CONCURRENCY = 16
_THOTH_DEPLOYMENT_NAME = os.environ["THOTH_DEPLOYMENT_NAME"]
_THOTH_METRICS_PUSHGATEWAY_URL = os.getenv("PROMETHEUS_PUSHGATEWAY_URL")
_CONFIGURED_SOLVERS = os.getenv("THOTH_DOCUMENT_SYNC_CONFIGURED_SOLVERS", "")

# Metrics Document sync
_METRIC_INFO = Gauge(
    "thoth_document_sync_job_info",
    "Thoth Document Sync Job information",
    ["env", "version"],
    registry=prometheus_registry,
)

_METRIC_DOCUMENTS_SYNC_NUMBER = PromCounter(
    "thoth_document_sync_job",
    "Thoth Document Sync Job number of synced documents status",
    ["sync_type", "env", "version", "result_type"],
    registry=prometheus_registry,
)

_METRIC_INFO.labels(_THOTH_DEPLOYMENT_NAME, __component_version__).inc()


class _SyncResult(Enum):
    SUCCESS = "success"
    SKIPPED = "skipped"
    ERROR = "error"


def _parse_s3_uri(ctx, param, value: str) -> Tuple[str, str]:
    _match = re.match(r"s3://([^/]*)/(.*)", value)
    if not (_match and len(_match.groups()) == 2):
        raise click.BadParameter("S3 uri is badly formatted")
    else:
        return cast(Tuple[str, str], _match.groups())


@click.command()
@click.option("--debug", is_flag=True, help="Run in a debug mode", envvar="THOTH_SYNC_DEBUG", default=False)
@click.option(
    "--force", is_flag=True, help="Perform force copy of documents.", envvar="THOTH_DOCUMENT_SYNC_FORCE", default=False
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
@click.option(
    "--s3-url",
    type=str,
    help="S3 service url for sync destination",
    envvar="AWS_S3_ENDPOINT_URL",
    default=None,
)
@click.argument(
    "dst", envvar="THOTH_DOCUMENT_SYNC_DST", type=str, metavar="s3://thoth/data/deployment", callback=_parse_s3_uri
)
def sync(
    dst: Tuple[str, str], debug: bool, force: bool, concurrency: int, days: Optional[int], s3_url: Optional[str]
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

    src_config = CephStore(f"{os.getenv('THOTH_CEPH_BUCKET_PREFIX', '')}/{_THOTH_DEPLOYMENT_NAME}")
    source = client(
        "s3",
        aws_access_key_id=src_config.key_id,
        aws_secret_access_key=src_config.secret_key,
        endpoint_url=src_config.host,
    )
    dest = client("s3", endpoint_url=s3_url)  # Defaults to using same config discovery than aws-cli.
    _LOGGER.debug("Source S3 client: %s", vars(source))
    _LOGGER.debug("Dest S3 client: %s", vars(dest))

    def _sync_one_key(key: str) -> None:
        # NOTE: boto3 sessions and resources are not thread-safe, clients are
        _LOGGER.debug(
            "Syncing source key (bucket='%s') '%s' to dest (bucket=%s) key '%s'",
            src_config.bucket,
            src_config.prefix + key,
            dst[0],
            dst[1] + key,
        )
        dest.upload_fileobj(
            source.get_object(Bucket=src_config.bucket, Key=src_config.prefix + key)["Body"], dst[0], dst[1] + key
        )

    total_count: defaultdict[S3Client, Counter[Type[ResultStorageBase]]] = defaultdict(Counter, {source: Counter()})

    def _s3_all_dates(
        client: S3Client,
        bucket: str,
        root_prefix: str,
        doc_prefix: str,
        adapter: Type[ResultStorageBase],
        start_date: Optional[date],
    ) -> Iterable[Iterable[str]]:

        paginator = client.get_paginator("list_objects_v2")

        def _s3_keys(date_prefix: str) -> Iterable[str]:
            nonlocal total_count
            prefix = f"{root_prefix}{doc_prefix}{date_prefix}"
            location = f"{client._endpoint}, bucket: '{bucket}', prefix: '{prefix}'"
            _LOGGER.debug("Listing objects at %s", location)
            for page_no, page in enumerate(paginator.paginate(Bucket=bucket, Prefix=prefix)):
                contents = page.get("Contents")
                if contents is not None:
                    total_count[client][adapter] += page["KeyCount"]
                    yield from (x["Key"][len(root_prefix) :] for x in contents if not x["Key"].endswith(".request"))
                elif page_no == 0:
                    _LOGGER.info("No objects at %s", location)

        if start_date is not None:
            yield from (_s3_keys(date_prefix) for date_prefix in adapter._iter_dates_prefix_addition(start_date))
        else:
            yield from ([_s3_keys("")])

    metrics: defaultdict[Type[ResultStorageBase], Counter[_SyncResult]] = defaultdict(
        Counter, {SolverResultsStore: Counter()}
    )

    try:
        adapters = itertools.chain((AnalysisByDigest, AnalysisResultsStore), itertools.repeat(SolverResultsStore))
        prefixs = [f"{AnalysisByDigest.RESULT_TYPE}/", f"{AnalysisResultsStore.RESULT_TYPE}/"] + [
            solver.strip() for solver in _CONFIGURED_SOLVERS.split() if solver.strip()
        ]
        for adapter, prefix in zip(adapters, prefixs):
            _LOGGER.info(f"Listing {adapter.RESULT_TYPE} documents with prefix {prefix}")

            src_documents = _s3_all_dates(source, src_config.bucket, src_config.prefix, prefix, adapter, start_date)
            dest_documents = _s3_all_dates(dest, *dst, prefix, adapter, start_date)
            documents_to_sync = itertools.chain.from_iterable(
                (sorted_iter_set_difference(*gens) for gens in zip(src_documents, dest_documents))  # aka zipWith
            )

            with ThreadPoolExecutor(max_workers=concurrency) as executor:
                for future in as_completed(
                    executor.submit(_sync_one_key, doc) for doc in documents_to_sync
                ):  # as_completed breaks lazyness. Will do for now
                    if future.exception():
                        _LOGGER.exception("An error occured while syncing a document: ", exc_info=future.exception())
                        result = _SyncResult.ERROR
                    else:
                        result = _SyncResult.SUCCESS
                    metrics[adapter][result] += 1

        _LOGGER.info("Document sync job has finished successfully")

    except ClientError as e:
        _LOGGER.exception("An error occured during the sync process: ", exc_info=True)
        _LOGGER.error("API response: %s", e.response)

    _LOGGER.info("Total count: %s", total_count)
    for _adapter in total_count[source].keys():
        metrics[_adapter][_SyncResult.SKIPPED] = total_count[source][_adapter] - (
            metrics[_adapter][_SyncResult.SUCCESS] + metrics[_adapter][_SyncResult.ERROR]
        )
    _LOGGER.info("metrics: %s", metrics)
    _LOGGER.info("Summary of the sync operation:",)
    for adapter in total_count[source].keys():
        _LOGGER.info("%s: " + ", ".join(itertools.repeat("%i %s", len(_SyncResult))), adapter.RESULT_TYPE,
                     *itertools.chain.from_iterable((metrics[adapter][r], r.value) for r in _SyncResult))

    if _THOTH_METRICS_PUSHGATEWAY_URL:

        for _adapter, by_result in metrics.items():
            for result, amount in by_result.items():
                _METRIC_DOCUMENTS_SYNC_NUMBER.labels(
                    sync_type=result.name,
                    env=_THOTH_DEPLOYMENT_NAME,
                    version=__component_version__,
                    result_type=_adapter,
                ).inc(amount)

        _LOGGER.info(f"Submitting metrics to Prometheus pushgateway {_THOTH_METRICS_PUSHGATEWAY_URL}")
        push_to_gateway(
            _THOTH_METRICS_PUSHGATEWAY_URL,
            job="document-sync-job",
            registry=prometheus_registry,
        )
