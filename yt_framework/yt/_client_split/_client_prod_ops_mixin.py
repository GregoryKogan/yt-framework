"""Map/reduce/vanilla/sort and upload methods for production YT client."""

# pyright: reportAttributeAccessIssue=false, reportUnusedImport=false, reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

from yt.wrapper import FilePath, TablePath
from yt.wrapper import schema as yt_schema

if TYPE_CHECKING:
    from pathlib import Path

    from yt.wrapper import Operation
    from yt.wrapper.schema import TableSchema

from yt_framework.job_command import resolve_aliased_job as _resolve_aliased_job
from yt_framework.utils.ignore import YTIgnoreMatcher
from yt_framework.yt._client_prod_runtime import (
    _apply_command_leg_format,
    _optional_str_kw,
    _spec_builder_secure_vault,
    prod_assemble_map_spec_with_vault,
    prod_assemble_vanilla_spec_with_vault,
    prod_map_reduce_after_legs,
    prod_map_reduce_open_spec_builder,
    prod_merge_sort_spec_into_kwargs,
    prod_reduce_finish_reducer_leg,
    prod_reduce_open_spec_builder,
    prod_submit_operation_with_kwargs,
    prod_upload_directory_files,
)
from yt_framework.yt._client_split._client_prod_cmd_helpers import (
    _maybe_wrap_string_command_for_vault,
    _partition_and_maybe_wrap_leg,
)
from yt_framework.yt.client_base import OperationResources
from yt_framework.yt.max_row_weight import validate_max_row_weight
from yt_framework.yt.operation_secure_env import (
    merge_secure_vault,
    pop_secure_env_client_kwargs,
)


class ClientProdOpsMixin:
    """Mixin providing operation submission and file upload helpers."""

    def upload_file(
        self,
        local_path: Path,
        yt_path: str,
        *,
        create_parent_dir: bool = False,
    ) -> None:
        """Upload a file to YT.

        Args:
            local_path: Local file path to upload
            yt_path: YT destination path
            create_parent_dir: If True, create parent directory if it doesn't exist (default: False)

        """
        self.logger.info("Uploading %s → %s", local_path.name, yt_path)
        try:
            # Ensure parent directory exists before uploading if requested
            if create_parent_dir and "/" in yt_path:
                # Extract parent directory from yt_path (everything before the last '/')
                parent_dir = "/".join(yt_path.split("/")[:-1])
                if parent_dir:
                    self.logger.debug(
                        "Ensuring parent directory exists: %s",
                        parent_dir,
                    )
                    self.create_path(parent_dir, node_type="map_node")

            with local_path.open("rb") as f:
                self.client.write_file(
                    yt_path,
                    f,
                    force_create=True,
                    compute_md5=True,
                )
            self.logger.debug("Upload completed: %s", yt_path)
        except Exception:
            self.logger.exception("Failed to upload file")
            raise

    def upload_directory(
        self,
        local_dir: Path,
        yt_dir: str,
        pattern: str = "*",
    ) -> list[str]:
        """Upload a directory to YT cluster.

        Recursively uploads all files from a local directory to a YT directory,
        respecting .ytignore patterns if present.

        Args:
            local_dir: Local directory path to upload
            yt_dir: YT destination directory path
            pattern: File pattern to match (default: "*" for all files)

        Returns:
            List of uploaded YT file paths

        Raises:
            Exception: If directory upload fails

        """
        self.logger.info(
            "Uploading directory %s → %s",
            local_dir,
            yt_dir,
        )
        return prod_upload_directory_files(
            local_dir=local_dir,
            yt_dir=yt_dir,
            pattern=pattern,
            ignore_matcher=YTIgnoreMatcher(local_dir),
            create_path=self.create_path,
            upload_file=self.upload_file,
            logger=self.logger,
        )

    def run_map(
        self,
        command: object,
        input_table: str,
        output_table: str,
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: object = None,
        *,
        append: bool = False,
        **kwargs: object,
    ) -> Operation:
        """Run a map operation on YT cluster.

        Submits a map operation that processes each row of the input table independently
        and writes results to the output table. The operation runs on the YT cluster
        with the specified resources and dependencies.

        Args:
            command: Legacy mapper job argument (TypedJob instance or command string).
            input_table: Input YT table path.
            output_table: Output YT table path.
            files: List of (yt_path, local_path) tuples for dependencies.
            resources: Operation resource configuration (memory, CPU, GPU, etc.).
            env: Environment variables dictionary.
            output_schema: Optional output table schema for typed output.
            max_failed_jobs: Maximum failed jobs allowed before operation fails.
            docker_auth: Optional Docker authentication for private registries.
            job: Preferred mapper job alias.
            append: If True, append mapper output to an existing output table.
            **kwargs: SpecBuilder chain options (e.g. ``weight``, ``title``) and
                ``run_operation`` flags such as ``sync``.

        Returns:
            Operation: YT operation object that can be monitored and waited on.

        Raises:
            Exception: If operation submission fails.

        """
        self.logger.info("Submitting map operation")
        self.logger.info("  Input: %s", input_table)
        self.logger.info("  Output: %s", output_table)
        self.logger.info("  Append: %s", append)
        self.logger.info("  Output Schema: %s", output_schema)
        self.logger.info("  Command: %s", command)
        self.logger.info("  Files: %s", files)
        self.logger.info("  Resources: %s", resources)

        try:
            mapper_job = _resolve_aliased_job(
                legacy_name="command",
                legacy_value=command,
                preferred_name="job",
                preferred_value=job,
            )
            kwargs = dict(kwargs)
            environment_public_keys, use_plain, user_secure_vault = (
                pop_secure_env_client_kwargs(kwargs)
            )
            kwargs["max_row_weight"] = validate_max_row_weight(
                _optional_str_kw(kwargs.get("max_row_weight")),
            )
            file_paths = [
                FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
            ]

            public_env, secure_flat, mapper_job = _partition_and_maybe_wrap_leg(
                mapper_job,
                env,
                environment_public_keys=environment_public_keys,
                use_plain_environment_for_secrets=use_plain,
            )
            merged_vault = merge_secure_vault(
                secure_flat,
                docker_image=resources.docker_image,
                docker_auth=docker_auth,
                user_secure_vault=user_secure_vault,
            )

            output_path = TablePath(output_table, append=append, schema=output_schema)
            spec_builder = prod_assemble_map_spec_with_vault(
                input_table=input_table,
                output_path=output_path,
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                mapper_job=mapper_job,
                file_paths=file_paths,
                public_env=public_env,
                merged_vault=merged_vault,
                logger=self.logger,
            )
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit operation: run_operation returned None",
                log_message="Operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit operation")
            raise
        else:
            return operation

    def run_vanilla(
        self,
        command: object,
        files: list[tuple[str, str]],
        env: dict[str, str],
        task_name: str,
        job: object = None,
        **kwargs: object,
    ) -> Operation | None:
        """Run a vanilla operation on YT cluster.

        Submits a vanilla operation that runs a standalone job without input/output tables.
        The operation runs on the YT cluster with the specified resources and dependencies.

        ``resources``, ``docker_auth``, and ``max_failed_jobs`` are passed via ``kwargs``
        (see :func:`yt_framework.operations.vanilla.run_vanilla`).

        Args:
            command: Legacy command argument (typically bash command with script path).
            files: List of (yt_path, local_path) tuples for dependencies.
            env: Environment variables dictionary.
            task_name: Task name for the operation.
            job: Preferred command alias.
            **kwargs: Must include ``resources=OperationResources``; may include
                ``docker_auth``, ``max_failed_jobs``, and SpecBuilder / ``run_operation`` keys.

        Returns:
            Operation object or None when submission fails.

        Raises:
            TypeError: If ``resources`` is missing or not an :class:`OperationResources`.

        """
        kw = dict(kwargs)
        resources_obj = kw.pop("resources", None)
        if not isinstance(resources_obj, OperationResources):
            msg = "run_vanilla requires resources=OperationResources in kwargs"
            raise TypeError(msg)
        resources = resources_obj
        docker_auth = cast("dict[str, str] | None", kw.pop("docker_auth", None))
        max_failed_jobs = int(cast("Any", kw.pop("max_failed_jobs", 1)))
        kwargs = kw

        self.logger.info("Submitting vanilla operation")
        self.logger.info("  Task Name: %s", task_name)
        self.logger.info("  Command: %s", command)
        self.logger.info("  Files: %s", files)
        self.logger.info("  Resources: %s", resources)

        try:
            vanilla_job = _resolve_aliased_job(
                legacy_name="command",
                legacy_value=command,
                preferred_name="job",
                preferred_value=job,
            )
            kwargs = dict(kwargs)
            environment_public_keys, use_plain, user_secure_vault = (
                pop_secure_env_client_kwargs(kwargs)
            )
            kwargs["max_row_weight"] = validate_max_row_weight(
                _optional_str_kw(kwargs.get("max_row_weight")),
            )
            file_paths = [
                FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
            ]

            operation_description = kwargs.pop("operation_description", None)

            public_env, secure_flat, vanilla_job = _partition_and_maybe_wrap_leg(
                vanilla_job,
                env,
                environment_public_keys=environment_public_keys,
                use_plain_environment_for_secrets=use_plain,
            )
            merged_vault = merge_secure_vault(
                secure_flat,
                docker_image=resources.docker_image,
                docker_auth=docker_auth,
                user_secure_vault=user_secure_vault,
            )

            spec_builder = prod_assemble_vanilla_spec_with_vault(
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                task_name=task_name,
                vanilla_job=vanilla_job,
                file_paths=file_paths,
                public_env=public_env,
                merged_vault=merged_vault,
                logger=self.logger,
                operation_description=operation_description,
            )
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit operation: run_operation returned None",
                log_message="Operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit vanilla operation")
            raise
        else:
            return operation

    def run_map_reduce(
        self,
        mapper: object,
        reducer: object,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        sort_by: list[str] | None = None,
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        map_job: object = None,
        reduce_job: object = None,
        **kwargs: object,
    ) -> Operation:
        """Run a map-reduce operation on YT cluster."""
        self.logger.info("Submitting map-reduce operation")
        self.logger.info("  Input: %s -> Output: %s", input_table, output_table)
        self.logger.info("  Reduce by: %s", reduce_by)

        try:
            (
                kwargs,
                mapper_leg,
                reducer_leg,
                public_env,
                merged_vault,
                file_paths,
                source_table,
                dest_table,
            ) = self._prepare_map_reduce_runtime(
                mapper=mapper,
                reducer=reducer,
                map_job=map_job,
                reduce_job=reduce_job,
                env=env,
                resources=resources,
                docker_auth=docker_auth,
                files=files,
                input_table=input_table,
                output_table=output_table,
                output_schema=output_schema,
                kwargs=dict(cast("Any", kwargs)),
            )

            spec_builder = prod_map_reduce_open_spec_builder(
                source_table=source_table,
                dest_table=dest_table,
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                kwargs=kwargs,
            )

            spec_builder = cast(
                "Any",
                self._configure_map_reduce_leg(
                    spec_builder=spec_builder,
                    leg_name="mapper",
                    leg_command=mapper_leg,
                    file_paths=file_paths,
                    public_env=public_env,
                    resources=resources,
                ),
            )
            spec_builder = cast(
                "Any",
                self._configure_map_reduce_leg(
                    spec_builder=spec_builder,
                    leg_name="reducer",
                    leg_command=reducer_leg,
                    file_paths=file_paths,
                    public_env=public_env,
                    resources=resources,
                ),
            )

            spec_builder = prod_map_reduce_after_legs(
                spec_builder,
                merged_vault,
                reduce_by,
                sort_by,
                kwargs,
            )
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit map-reduce operation",
                log_message="Map-reduce operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit map-reduce operation")
            raise
        else:
            return operation

    def _prepare_map_reduce_runtime(
        self,
        *,
        mapper: object,
        reducer: object,
        map_job: object,
        reduce_job: object,
        env: dict[str, str],
        resources: OperationResources,
        docker_auth: dict[str, str] | None,
        files: list[tuple[str, str]],
        input_table: str,
        output_table: str,
        output_schema: TableSchema | None,
        kwargs: dict[str, Any],
    ) -> tuple[
        dict[str, Any],
        object,
        object,
        dict[str, str],
        dict[str, str],
        list[FilePath],
        TablePath,
        TablePath,
    ]:
        mutable_kwargs = dict(kwargs)
        environment_public_keys, use_plain, user_secure_vault = (
            pop_secure_env_client_kwargs(mutable_kwargs)
        )
        mutable_kwargs["max_row_weight"] = validate_max_row_weight(
            _optional_str_kw(mutable_kwargs.get("max_row_weight")),
        )
        mapper_leg = _resolve_aliased_job(
            legacy_name="mapper",
            legacy_value=mapper,
            preferred_name="map_job",
            preferred_value=map_job,
        )
        reducer_leg = _resolve_aliased_job(
            legacy_name="reducer",
            legacy_value=reducer,
            preferred_name="reduce_job",
            preferred_value=reduce_job,
        )
        public_env, secure_flat, mapper_leg = _partition_and_maybe_wrap_leg(
            mapper_leg,
            env,
            environment_public_keys=environment_public_keys,
            use_plain_environment_for_secrets=use_plain,
        )
        reducer_leg = _maybe_wrap_string_command_for_vault(reducer_leg, secure_flat)
        merged_vault = merge_secure_vault(
            secure_flat,
            docker_image=resources.docker_image,
            docker_auth=docker_auth,
            user_secure_vault=user_secure_vault,
        )
        file_paths = [
            FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
        ]
        source_table = TablePath(input_table)
        dest_table = TablePath(output_table, append=False, schema=output_schema)
        return (
            mutable_kwargs,
            mapper_leg,
            reducer_leg,
            public_env,
            merged_vault,
            file_paths,
            source_table,
            dest_table,
        )

    def _configure_map_reduce_leg(
        self,
        *,
        spec_builder: object,
        leg_name: Literal["mapper", "reducer"],
        leg_command: object,
        file_paths: list[FilePath],
        public_env: dict[str, str],
        resources: OperationResources,
    ) -> object:
        leg_builder = (
            (
                cast("Any", spec_builder).begin_mapper()
                if leg_name == "mapper"
                else cast("Any", spec_builder).begin_reducer()
            )
            .command(leg_command)
            .file_paths(file_paths)
            .environment(public_env)
            .memory_limit(resources.memory_gb * 1024**3)
            .cpu_limit(resources.cpu_limit)
            .gpu_limit(resources.gpu_limit)
        )
        leg_builder = cast("Any", _apply_command_leg_format(leg_builder, leg_command))
        if resources.docker_image:
            leg_builder = leg_builder.docker_image(resources.docker_image)
        if leg_name == "mapper":
            leg_builder.end_mapper()
        else:
            leg_builder.end_reducer()
        return spec_builder

    def run_reduce(
        self,
        reducer: object,
        input_table: str,
        output_table: str,
        reduce_by: list[str],
        files: list[tuple[str, str]],
        resources: OperationResources,
        env: dict[str, str],
        output_schema: TableSchema | None = None,
        max_failed_jobs: int = 1,
        docker_auth: dict[str, str] | None = None,
        job: object = None,
        **kwargs: object,
    ) -> Operation:
        """Run a reduce-only operation on YT cluster."""
        self.logger.info("Submitting reduce operation")
        self.logger.info("  Input: %s -> Output: %s", input_table, output_table)
        self.logger.info("  Reduce by: %s", reduce_by)

        try:
            reducer_leg = _resolve_aliased_job(
                legacy_name="reducer",
                legacy_value=reducer,
                preferred_name="job",
                preferred_value=job,
            )
            kwargs = dict(kwargs)
            environment_public_keys, use_plain, user_secure_vault = (
                pop_secure_env_client_kwargs(kwargs)
            )
            kwargs["max_row_weight"] = validate_max_row_weight(
                _optional_str_kw(kwargs.get("max_row_weight")),
            )
            public_env, secure_flat, reducer_leg = _partition_and_maybe_wrap_leg(
                reducer_leg,
                env,
                environment_public_keys=environment_public_keys,
                use_plain_environment_for_secrets=use_plain,
            )
            merged_vault = merge_secure_vault(
                secure_flat,
                docker_image=resources.docker_image,
                docker_auth=docker_auth,
                user_secure_vault=user_secure_vault,
            )
            file_paths = [
                FilePath(yt_path, file_name=local_path) for yt_path, local_path in files
            ]
            source_table = TablePath(input_table)
            dest_table = TablePath(output_table, append=False, schema=output_schema)

            spec_builder = prod_reduce_open_spec_builder(
                source_table=source_table,
                dest_table=dest_table,
                resources=resources,
                max_failed_jobs=max_failed_jobs,
                kwargs=kwargs,
            )
            spec_builder = prod_reduce_finish_reducer_leg(
                spec_builder,
                reducer_leg=reducer_leg,
                file_paths=file_paths,
                public_env=public_env,
                resources=resources,
            )
            spec_builder = cast(
                "Any", _spec_builder_secure_vault(spec_builder, merged_vault)
            )
            spec_builder = spec_builder.reduce_by(reduce_by)
            operation = prod_submit_operation_with_kwargs(
                self.client,
                self.logger,
                spec_builder,
                kwargs,
                none_message="Failed to submit reduce operation",
                log_message="Reduce operation submitted: %s",
            )
        except Exception:
            self.logger.exception("Failed to submit reduce operation")
            raise
        else:
            return operation

    def run_sort(
        self,
        table_path: str,
        sort_by: list[str],
        pool: str | None = None,
        pool_tree: str | None = None,
        **kwargs: object,
    ) -> None:
        """Sort a table in place by the given columns."""
        self.logger.info("Sorting table %s by %s", table_path, sort_by)
        try:
            sort_columns = [
                yt_schema.SortColumn(col, sort_order="ascending") for col in sort_by
            ]
            call_kw = prod_merge_sort_spec_into_kwargs(
                dict(cast("Any", kwargs)),
                pool=pool,
                pool_tree=pool_tree,
            )
            self.client.run_sort(table_path, sort_by=sort_columns, **call_kw)
            self.logger.info("Sort completed")
        except Exception:
            self.logger.exception("Failed to sort table")
            raise
