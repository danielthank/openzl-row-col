#!/usr/bin/env python3
"""
OpenZL Compressor Training Tool

Simplified version: Samples 5000 files per schema type and trains 3 compressors.
Creates training directories: data/otap/, data/otlp_metrics/, data/otlp_traces/
"""

import random
import shutil
import subprocess
import sys
from pathlib import Path


def discover_data_folders(data_dir: Path) -> dict[str, list[Path]]:
    """
    Discover all data folders and group by schema type.

    Returns:
        Dictionary mapping schema type to list of folder paths
        e.g., {"otap": [...], "otlp_metrics": [...], "otlp_traces": [...]}
    """
    schema_folders = {"otap": [], "otlp_metrics": [], "otlp_traces": []}

    for folder_path in sorted(data_dir.iterdir()):
        if not folder_path.is_dir():
            continue

        folder_name = folder_path.name

        # Filter out legacy folders (no suffix pattern)
        if folder_name.count("-") < 3:
            continue

        # Categorize by schema
        if "otelmetrics" in folder_name and "-otap-" in folder_name:
            schema_folders["otap"].append(folder_path)
        elif "otelmetrics" in folder_name and "-otlp-" in folder_name:
            schema_folders["otlp_metrics"].append(folder_path)
        elif "oteltraces" in folder_name and "-otap-" in folder_name:
            schema_folders["otap"].append(folder_path)
        elif "oteltraces" in folder_name and "-otlp-" in folder_name:
            schema_folders["otlp_traces"].append(folder_path)

    return schema_folders


def sample_files_for_schema(
    folders: list[Path], num_files: int = 5000, seed: int = 42
) -> list[Path]:
    """
    Sample random payload files from all folders for a schema.

    Args:
        folders: List of folder paths containing payloads
        num_files: Number of files to sample total
        seed: Random seed for reproducibility

    Returns:
        List of sampled payload file paths
    """
    # Collect all payload files from all folders
    all_payloads = []
    for folder in folders:
        all_payloads.extend(folder.glob("payload_*.bin"))

    if not all_payloads:
        raise ValueError(f"No payload files found in {len(folders)} folders")

    # Sample
    random.seed(seed)
    sample_size = min(num_files, len(all_payloads))
    sampled = random.sample(all_payloads, sample_size)

    print(
        f"  Sampled {len(sampled)} files from {len(all_payloads)} total payloads across {len(folders)} folders"
    )

    return sampled


def prepare_schema_train_dir(
    data_dir: Path, schema_name: str, sampled_files: list[Path]
) -> Path:
    """
    Create schema-specific training directory and copy files.

    Args:
        data_dir: Base data directory
        schema_name: Schema name (otap, otlp_metrics, otlp_traces)
        sampled_files: List of payload files to copy

    Returns:
        Path to training directory
    """
    # Create schema directory
    schema_dir = data_dir / schema_name
    schema_dir.mkdir(exist_ok=True)

    # Clean up existing files
    for existing_file in schema_dir.glob("payload_*.bin"):
        existing_file.unlink()

    # Copy sampled files
    print(f"  Copying {len(sampled_files)} files to {schema_dir.name}/...")
    for i, payload_file in enumerate(sampled_files):
        # Rename to avoid conflicts (payload_0000.bin, payload_0001.bin, ...)
        dest_name = f"payload_{i:04d}.bin"
        shutil.copy2(payload_file, schema_dir / dest_name)

    return schema_dir


def train_compressor(
    train_dir: Path, output_path: Path, mode: str, script_dir: Path
) -> str:
    """
    Train compressor using protobuf_cli.

    Args:
        train_dir: Path to directory containing sampled payloads
        output_path: Path for output trained.zlc file
        mode: Mode string for protobuf_cli (otlp_metrics, otlp_traces, otap)
        script_dir: Directory where this script is located

    Returns:
        Command output
    """
    # Construct command
    protobuf_cli = (
        script_dir.parent
        / "openzl"
        / "build-install"
        / "tools"
        / "protobuf"
        / "protobuf_cli"
    )

    if not protobuf_cli.exists():
        raise FileNotFoundError(f"protobuf_cli not found at {protobuf_cli}")

    # Verify directory has payload files
    payload_files = list(train_dir.glob("payload_*.bin"))
    if not payload_files:
        raise ValueError(f"No payload files found in {train_dir}")

    # Build command - pass the directory as input
    cmd = [
        str(protobuf_cli),
        "--mode",
        mode,
        "train",
        "--input",
        str(train_dir),
        "--output",
        str(output_path),
    ]

    print(f"  Running protobuf_cli train with {len(payload_files)} files...")
    print(f"  Command: {' '.join(cmd)}")

    # Execute command - show output in real-time
    result = subprocess.run(
        cmd,
        cwd=script_dir,
        capture_output=False,  # Let output go to terminal
        text=True,
    )

    if result.returncode != 0:
        raise RuntimeError(f"Training failed with return code {result.returncode}")

    return "Training completed successfully"


def main():
    """Main entry point."""
    script_dir = Path(__file__).parent.resolve()
    source_dir = script_dir.parent / "data" / "generated"
    data_dir = script_dir.parent / "data"

    if not source_dir.exists():
        print(f"Error: Source directory not found: {source_dir}")
        sys.exit(1)

    # Create data directory if it doesn't exist
    data_dir.mkdir(exist_ok=True)

    print("=" * 80)
    print("OpenZL Compressor Training - Simplified Mode")
    print("Sampling 5000 files per schema type for training")
    print("=" * 80)
    print()

    # Discover folders grouped by schema
    print("Discovering data folders...")
    schema_folders = discover_data_folders(source_dir)

    for schema_name, folders in schema_folders.items():
        print(f"  {schema_name}: {len(folders)} folders")

    print()

    # Process each schema
    success_count = 0
    failure_count = 0

    for schema_name in ["otap", "otlp_metrics", "otlp_traces"]:
        folders = schema_folders[schema_name]

        if not folders:
            print(f"[{schema_name}] No folders found, skipping...")
            print()
            continue

        print(f"[{schema_name}] Processing...")

        try:
            # Sample files from all folders for this schema
            sampled_files = sample_files_for_schema(folders, num_files=5000)

            # Prepare training directory
            train_dir = prepare_schema_train_dir(data_dir, schema_name, sampled_files)

            # Train compressor
            output_path = train_dir / "trained.zlc"
            output = train_compressor(train_dir, output_path, schema_name, script_dir)

            print(f"  ✓ Successfully created: {output_path}")
            success_count += 1

        except Exception as e:
            print(f"  ✗ Failed: {e}")
            failure_count += 1

        print()  # Blank line between schemas

    # Summary
    print("=" * 80)
    print("Training complete!")
    print(f"  Success: {success_count}/3")
    print(f"  Failures: {failure_count}/3")
    print()
    print("Output structure:")
    print("  data/otap/")
    print("    ├── payload_0000.bin ... payload_0009.bin (5000 training files)")
    print("    └── trained.zlc")
    print("  data/otlp_metrics/")
    print("    ├── payload_0000.bin ... payload_0009.bin")
    print("    └── trained.zlc")
    print("  data/otlp_traces/")
    print("    ├── payload_0000.bin ... payload_0009.bin")
    print("    └── trained.zlc")
    print("=" * 80)

    if failure_count > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
