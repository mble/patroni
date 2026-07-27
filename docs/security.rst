.. _security:

=======================
Security Considerations
=======================

A Patroni cluster has two interfaces to be protected from unauthorized access: the distributed configuration storage (DCS) and the Patroni REST API.

Protecting DCS
==============

Patroni and :ref:`patronictl` both store and retrieve data to/from the DCS.

Despite DCS doesn't contain any sensitive information, it allows changing some of Patroni/Postgres configuration. Therefore the very first thing that should be protected is DCS itself.

The details of protection depend on the type of DCS used. The authentication and encryption parameters (tokens/basic-auth/client certificates) for the supported types of DCS are covered in :ref:`settings <yaml_configuration>`.

The general recommendation is to enable TLS for all DCS communication.

Protecting the REST API
=======================

Protecting the REST API is a more complicated task.

The Patroni REST API is used by Patroni itself during the leader race, by the :ref:`patronictl` tool in order to perform failovers/switchovers/reinitialize/restarts/reloads, by HAProxy or any other kind of load balancer to perform HTTP health checks, and of course could also be used for monitoring.

From the point of view of security, REST API contains safe (``GET`` requests, only retrieve information) and unsafe (``PUT``, ``POST``, ``PATCH`` and ``DELETE`` requests, change the state of nodes) endpoints.

The unsafe endpoints can be protected with HTTP basic-auth by setting the ``restapi.authentication.username`` and ``restapi.authentication.password`` parameters. There is no way to protect the safe endpoints without enabling TLS.

When TLS for the REST API is enabled and a PKI is established, mutual authentication of the API server and API client is possible for all endpoints.

The ``restapi`` section parameters enable TLS client authentication to the server. Depending on the value of the ``verify_client`` parameter, the API server requires a successful client certificate verification for both safe and unsafe API calls (``verify_client: required``), or only for unsafe API calls (``verify_client: optional``), or for no API calls (``verify_client: none``).

The ``ctl`` section parameters enable TLS server authentication to the client (the :ref:`patronictl` tool which uses the same config as patroni). Set ``insecure: true`` to disable the server certificate verification by the client. See :ref:`settings <patronictl_settings>` for a detailed description of the TLS client parameters.

Protecting the PostgreSQL database proper from unauthorized access is beyond the scope of this document and is covered in https://www.postgresql.org/docs/current/client-authentication.html

.. _postgres_exec_prefix:

PostgreSQL execution prefix
===========================

``postgresql.postgres_exec_prefix`` is a list of arguments that Patroni prepends to every direct invocation of the ``postgres`` executable. It exists so that a deployment can confine PostgreSQL and all of its descendants to a restricted execution domain (for example one established with Landlock, seccomp, environment filtering, or descriptor closure) without placing Patroni or auxiliary processes in that same domain.

.. code:: YAML

   postgresql:
     postgres_exec_prefix:
       - /usr/local/libexec/pg-launcher
       - --profile
       - core-v1
       - --

With the configuration above, starting the postmaster executes:

.. code:: bash

   /usr/local/libexec/pg-launcher --profile core-v1 -- \
     /usr/lib/postgresql/18/bin/postgres -D /path/to/pgdata --config-file=/path/to/postgresql.conf

Patroni does not implement any isolation itself. It only guarantees that the execution paths listed below pass through the configured executable.

Scope
-----

The prefix is applied to direct executions of ``postgres``:

- normal postmaster startup;
- the ``postgres --single`` invocation used while rewinding, which can load ``shared_preload_libraries`` on PostgreSQL 15 and newer;
- ``postgres -C`` used to read a GUC value from the instance configuration;
- ``postgres --describe-config``;
- the runtime ``postgres --version`` probe.

The prefix is **not** applied to ``initdb``, ``pg_ctl``, ``pg_rewind``, ``pg_basebackup``, ``pg_controldata``, ``pg_isready``, ``pg_waldump``, the version probe performed by ``patroni --validate-config``, or to Patroni callback scripts.

``postgresql.bin_name.postgres`` is not a substitute for this option: it also changes the executable Patroni expects to find when validating ``postmaster.pid``. ``postgres_exec_prefix`` keeps the real PostgreSQL executable available for process discovery and identity checks.

Contract
--------

- The setting is optional. When it is absent, Patroni builds exactly the same command lines as before.
- It must be a non-empty list of non-empty strings, and its first element must be an absolute path to an executable regular file. Patroni performs no shell expansion, interpolation, or ``$PATH`` lookup, so shell syntax in the list will not work.
- It is only supported on POSIX platforms. On other platforms a configured value is rejected.
- It is local configuration. It cannot be set or changed through the dynamic configuration stored in the DCS.
- **Its arguments must not contain secrets.** They appear in Patroni logs, in the process command line, and in diagnostics.
- The prefix must eventually replace itself with the supplied PostgreSQL command using ``execve`` or an equivalent. A prefix that forks, daemonizes, or stays alive as a supervisor is not supported, because Patroni tracks the PID it started as the postmaster PID.
- If ``postgresql.bin_dir`` is not configured, Patroni passes ``postgres`` without a directory component, and the prefix is responsible for resolving it. Configure ``bin_dir`` to avoid relying on that.

Failure semantics
-----------------

The hook is fail-closed. Patroni never retries a failed prefixed command using the raw PostgreSQL binary:

- if the prefix cannot be executed, the requested PostgreSQL operation fails;
- a launcher that cannot install its policy is expected to exit non-zero before executing PostgreSQL;
- postmaster startup failures follow Patroni's existing start-failure state transitions, and utility failures follow the existing error path for that command;
- the launcher's stderr remains visible through the usual logging and subprocess paths.

Validation happens both in ``patroni --validate-config`` and at runtime. It is not a guarantee against later filesystem replacement: if the executable disappears or changes after validation, normal execution-failure semantics apply.

Reload behaviour
----------------

On a configuration reload, the new value is validated before being applied. An invalid value is rejected as a whole and the previously accepted value is retained. A valid addition, change, or removal applies to subsequent direct ``postgres`` executions only: a running postmaster is never restarted automatically, and Patroni logs a warning that the running instance keeps the prefix it was started with. Completing the rollout with a controlled PostgreSQL restart is the operator's responsibility.

Deployment responsibilities
---------------------------

The protection this hook offers depends on the deployment ensuring that:

- only the operator controls Patroni's local configuration;
- the prefix executable and its dependencies are immutable to the PostgreSQL UID, for example root-owned and read-only in the container image;
- the prefix installs its restrictions before loading untrusted PostgreSQL state or code, and fails before ``execve`` if any mandatory restriction cannot be installed;
- PostgreSQL cannot select a different policy through customer-controlled configuration;
- launcher arguments and error messages do not disclose credentials.

An attacker who can change Patroni's local configuration or replace the prefix executable is outside the protection this hook provides.
