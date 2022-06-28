Thoth's document-sync-job
-------------------------

.. image:: https://img.shields.io/github/v/tag/thoth-station/document-sync-job?style=plastic
  :target: https://github.com/thoth-station/document-sync-job/tags
  :alt: GitHub tag (latest by date)

.. image:: https://quay.io/repository/thoth-station/document-sync-job/status
  :target: https://quay.io/repository/thoth-station/document-sync-job?tab=tags
  :alt: Quay - Build

Sync Thoth documents to an S3 API compatible remote.

This job makes sure Thoth documents required in deployments are properly copied
from one deployment to another. The job is designed for cases when one
deployment performs data calculation with ingestion and another is used to
serve the computed data.

The job accepts `Thoth parameters required to use Thoth's adapters
<https://github.com/thoth-station/storages#accessing-data-on-ceph>`__ (via
environment variables) to obtain data from the first deployment and uses AWS
CLI to copy data to the deployment which serves data. Thus the later deployment
is configured via the standard `AWS CLI configuration file
<https://docs.aws.amazon.com/cli/latest/userguide/cli-configure-files.html>`__
that should be present in ``~/.aws/config``.
