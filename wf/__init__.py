"""
Download fastq files using parallel-fastq-dump
"""

import subprocess
from pathlib import Path

from latch import small_task, workflow
from latch.types import LatchFile, LatchDir


@small_task
def parallel_fastq_dump_task(sra_id: str, output_dir: LatchDir, threads: int = 4) -> LatchDir:

    # Defining outputs

    local_dir = "/root/dump_output/"

    # Defining the command
    _parallel_fastq_dump_cmd = [
        "parallel-fastq-dump",
        "--sra-id",
        str(sra_id),
        "--threads",
        str(threads),
        "--outdir",
        str(local_dir),
        "--split-files",
        "--gzip",

    ]

    subprocess.run(_parallel_fastq_dump_cmd, check=True)

    return LatchDir(str(local_dir), output_dir.remote_path)


@workflow
def parallel_fastq_dump(sra_id: str, output_dir: LatchDir, threads: int = 4) -> LatchDir:
    """Download fastq files

    __metadata__:
        display_name: Download fastq files using parallel-fastq-dump

        author:
            name:

            email:

            github:
        repository:

        license:
            id: MIT

    Args:

        sra_id:
          sra id

          __metadata__:
            display_name: SRA ID

        threads:
          Threads to run. *Default is at 4.

          __metadata__:
            display_name: Threads

        output_dir:
          Output directory

          __metadata__:
            display_name: Output directory
    """

    return parallel_fastq_dump_task(sra_id=sra_id, output_dir=output_dir, threads=threads)
