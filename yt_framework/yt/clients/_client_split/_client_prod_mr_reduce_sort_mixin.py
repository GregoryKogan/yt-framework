"""Map-reduce, reduce-only, and sort helpers for production YT client."""

# pyright: reportAttributeAccessIssue=false, reportUnusedImport=false, reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING, Any, Literal, cast

from yt.wrapper import FilePath, TablePath
from yt.wrapper import schema as yt_schema

if TYPE_CHECKING:
    from yt.wrapper import Operation
    from yt.wrapper.schema import TableSchema

    from yt_framework.yt.clients.client_base import OperationResources
    from yt_framework.yt.clients.operation_specs import (
        MapReduceSubmitSpec,
        ReduceSubmitSpec,
    )

from yt_framework.job_command import resolve_aliased_job as _resolve_aliased_job
from yt_framework.yt.clients._client_split._client_prod_cmd_helpers import (
    _partition_and_maybe_wrap_leg,
    maybe_wrap_cmd_for_vault,
)
from yt_framework.yt.support._client_prod_runtime import (
    _apply_command_leg_format,
    _optional_str_kw,
    _spec_builder_secure_vault,
    prod_map_reduce_after_legs,
    prod_merge_sort_spec_kwargs,
    prod_mr_open_spec_builder,
    prod_reduce_finish_reducer_leg,
    prod_reduce_open_spec_builder,
    prod_submit_operation_with_kwargs,
)
from yt_framework.yt.support.max_row_weight import validate_max_row_weight
from yt_framework.yt.support.operation_secure_env import (
    merge_secure_vault,
    pop_secure_env_client_kwargs,
)


class ClientProdMrReduceSortMixin:
    """Mixin for map-reduce, reduce-only, and sort operations."""

    def run_map_reduce_submit(self, spec: MapReduceSubmitSpec) -> Operation:
        """Run a map-reduce operation on YT cluster."""
        mapper = spec.mapper
        reducer = spec.reducer
        input_table = spec.input_table
        output_table = spec.output_table
        reduce_by = spec.reduce_by_list()
        files = spec.files_list()
        resources = spec.resources
        env = spec.env_dict()
        sort_by = spec.sort_by_list()
        output_schema = spec.output_schema
        max_failed_jobs = spec.max_failed_jobs
        docker_auth = spec.docker_auth_dict()
        map_job = spec.map_job
        reduce_job = spec.reduce_job
        kwargs = dict(spec.extras_dict())
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

            spec_builder = prod_mr_open_spec_builder(
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
        reducer_leg = maybe_wrap_cmd_for_vault(reducer_leg, secure_flat)
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

    def run_reduce_submit(self, spec: ReduceSubmitSpec) -> Operation:
        """Run a reduce-only operation on YT cluster."""
        reducer = spec.reducer
        input_table = spec.input_table
        output_table = spec.output_table
        reduce_by = spec.reduce_by_list()
        files = spec.files_list()
        resources = spec.resources
        env = spec.env_dict()
        output_schema = spec.output_schema
        max_failed_jobs = spec.max_failed_jobs
        docker_auth = spec.docker_auth_dict()
        job = spec.job
        kwargs = dict(spec.extras_dict())
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
            call_kw = prod_merge_sort_spec_kwargs(
                dict(cast("Any", kwargs)),
                pool=pool,
                pool_tree=pool_tree,
            )
            self.client.run_sort(table_path, sort_by=sort_columns, **call_kw)
            self.logger.info("Sort completed")
        except Exception:
            self.logger.exception("Failed to sort table")
            raise
