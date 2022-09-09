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


Development and Testing
-----------------------

The ``.env.template`` has environment variables that are required for document-sync-job execution. |br|
Create ``.env`` file out of the ``.env.template`` with adjusted environment variable values.

.. code-block:: console

  cp .env.template .env
  vim .env

As stated above the job accepts source s3 details as env vars and destination s3 details
as aws cli conf or env vars.

The s3 credentials can be accessed via cluster. |br|
Check the additional documentation on `thoth-application <https://github.com/thoth-station/thoth-application/blob/master/docs/environments.md>`_. |br|
Assign the credentials value to the env variable in the ``.env`` file.

If using s3 cluster different than s3.amazonaws.com. |br|
set the ``--endpoint https://<s3-url>`` in the list, at this `line <https://github.com/thoth-station/document-sync-job/blob/master/app.py#L102>`_.

For testing purpose,
  Developers can use PSI test instance as source and Operate-first smaug instance as destination. |br|
  There is old data available on ``prefix``: ``ocp-test``.

.. |br| raw:: html

      <br>
