#!/usr/bin/env python3
"""
YT Vanilla Job Script to untar archive and upload files to YT build folder.

This script is executed as a standalone YT vanilla job (not imported as a module).

This script:
1. Downloads the archive from YT (or uses local file if provided)
2. Extracts it to local filesystem
3. Uploads extracted files to YT build folder

Environment variables required:
- YT_BUILD_FOLDER: YT path to build folder
- YT_ARCHIVE_PATH: YT path to archive file
- ARCHIVE_LOCAL_NAME: Local filename of archive in sandbox (default: code.tar.gz)
"""

import tarfile
import tempfile
import os
import sys
from pathlib import Path
import yt.wrapper as yt


def main() -> None:
    """
    Main entry point for archive extraction and upload script.
    
    This function is executed as a standalone YT vanilla job. It:
    1. Downloads the archive from YT (or uses local file if provided)
    2. Extracts it to local filesystem
    3. Uploads extracted files to YT build folder
    
    Environment variables required:
        YT_BUILD_FOLDER: YT path to build folder
        YT_ARCHIVE_PATH: YT path to archive file
        ARCHIVE_LOCAL_NAME: Local filename of archive in sandbox (default: code.tar.gz)
    
    Returns:
        None (exits with code 0 on success, 1 on failure)
        
    Raises:
        SystemExit: If required environment variables are missing or operations fail.
    """
    yt_build_folder = os.environ.get("YT_BUILD_FOLDER")
    yt_archive_path = os.environ.get("YT_ARCHIVE_PATH")
    archive_local_name = os.environ.get("ARCHIVE_LOCAL_NAME", "code.tar.gz")

    if not yt_build_folder or not yt_archive_path:
        error_msg = f"Missing environment variables: YT_BUILD_FOLDER={yt_build_folder}, YT_ARCHIVE_PATH={yt_archive_path}"
        print(error_msg, file=sys.stderr)
        sys.exit(1)

    # Always extract code to build folder (where map operations expect it)
    # code_extraction_folder is only used for storing the untar script
    yt_upload_folder = yt_build_folder

    with tempfile.TemporaryDirectory() as tmpdir:
        # Archive is already in sandbox as code.tar.gz (provided via file_paths)
        archive_local_path = Path(archive_local_name)
        if not archive_local_path.exists():
            error_msg = f"Archive file not found in sandbox: {archive_local_path}"
            print(error_msg, file=sys.stderr)
            sys.exit(1)

        extract_dir = Path(tmpdir) / "extracted_code"
        extract_dir.mkdir()

        print(
            f"Using archive from sandbox: {archive_local_path} ({archive_local_path.stat().st_size / (1024*1024):.2f} MB)",
            file=sys.stderr,
        )

        print(f"Extracting archive to {extract_dir}", file=sys.stderr)
        try:
            with tarfile.open(archive_local_path, "r:gz") as tar:
                tar.extractall(extract_dir)
            print("Extracted archive", file=sys.stderr)
        except Exception as e:
            error_msg = f"Failed to extract archive: {e}"
            print(error_msg, file=sys.stderr)
            sys.exit(1)

        print(
            f"Uploading extracted files to YT folder: {yt_upload_folder}",
            file=sys.stderr,
        )
        uploaded_count = 0
        try:
            # Ensure the extraction folder exists
            yt.create(
                "map_node", yt_upload_folder, recursive=True, ignore_existing=True
            )

            for root, dirs, files in os.walk(extract_dir):
                for file in files:
                    local_file = Path(root) / file
                    rel_path = local_file.relative_to(extract_dir)
                    yt_path = f"{yt_upload_folder}/{rel_path}".replace("\\", "/")

                    parent = "/".join(yt_path.split("/")[:-1])
                    if parent:
                        yt.create(
                            "map_node", parent, recursive=True, ignore_existing=True
                        )

                    with open(local_file, "rb") as f:
                        yt.write_file(yt_path, f, force_create=True, compute_md5=True)

                    uploaded_count += 1

            print(
                f"Successfully uploaded {uploaded_count} files to {yt_upload_folder} (build folder)",
                file=sys.stderr,
            )
            sys.exit(0)
        except Exception as e:
            error_msg = f"Failed to upload files: {e}"
            print(error_msg, file=sys.stderr)
            sys.exit(1)


if __name__ == "__main__":
    main()
