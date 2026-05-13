"""Map/reduce/vanilla/sort and upload methods for production YT client."""

# pyright: reportAttributeAccessIssue=false, reportUnusedImport=false, reportPrivateUsage=false

from __future__ import annotations

from typing import TYPE_CHECKING

from yt.wrapper import FilePath, TablePath

if TYPE_CHECKING:
    from pathlib import Path

    from yt.wrapper import Operation

    from yt_framework.yt.clients.operation_specs import MapSubmitSpec, VanillaSubmitSpec

from yt_framework.job_command import resolve_aliased_job as _resolve_aliased_job
from yt_framework.utils.ignore import YTIgnoreMatcher
from yt_framework.yt._client_prod_runtime import (
    _optional_str_kw,
    prod_map_spec_with_vault,
    prod_submit_operation_with_kwargs,
    prod_upload_directory_files,
    prod_vanilla_spec_with_vault,
)
from yt_framework.yt._client_split._client_prod_cmd_helpers import (
    _partition_and_maybe_wrap_leg,
)
from yt_framework.yt._client_split._client_prod_mr_reduce_sort_mixin import (
    ClientProdMrReduceSortMixin,
)
from yt_framework.yt.max_row_weight import validate_max_row_weight
from yt_framework.yt.operation_secure_env import (
    merge_secure_vault,
    pop_secure_env_client_kwargs,
)


class ClientProdOpsMixin(ClientProdMrReduceSortMixin):
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

    def run_map_submit(self, spec: MapSubmitSpec) -> Operation:
        """Run a map operation on YT cluster."""
        command = spec.command
        input_table = spec.input_table
        output_table = spec.output_table
        files = spec.files_list()
        resources = spec.resources
        env = spec.env_dict()
        output_schema = spec.output_schema
        max_failed_jobs = spec.max_failed_jobs
        docker_auth = spec.docker_auth_dict()
        job = spec.job
        append = spec.append
        kwargs = dict(spec.extras_dict())
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
            spec_builder = prod_map_spec_with_vault(
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

    def run_vanilla_submit(self, spec: VanillaSubmitSpec) -> Operation | None:
        """Run a vanilla operation on YT cluster."""
        command = spec.command
        files = spec.files_list()
        env = spec.env_dict()
        task_name = spec.task_name
        job = spec.job
        resources = spec.resources
        max_failed_jobs = spec.max_failed_jobs
        docker_auth = spec.docker_auth_dict()
        kwargs = dict(spec.extras_dict())
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

            spec_builder = prod_vanilla_spec_with_vault(
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
