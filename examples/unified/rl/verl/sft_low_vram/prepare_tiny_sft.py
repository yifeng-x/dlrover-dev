#!/usr/bin/env python3
# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0.

import argparse
from pathlib import Path

import pandas as pd


TRAIN_EXAMPLES = [
    (
        "Explain why the sky looks blue in one short paragraph.",
        "The sky looks blue because air molecules scatter shorter blue "
        "wavelengths of sunlight more than longer red wavelengths.",
    ),
    (
        "Write a polite reminder to submit a weekly report.",
        "Hi team, this is a friendly reminder to submit your weekly report "
        "before the end of the day. Thank you.",
    ),
    (
        "Convert this note into a task: review the deployment logs tomorrow.",
        "Task: Review the deployment logs tomorrow and record any errors or "
        "warnings that need follow-up.",
    ),
    (
        "Summarize: The service restarted twice, then recovered after the "
        "cache was cleared.",
        "The service had two restarts and recovered once the cache was cleared.",
    ),
    (
        "Give three tips for saving GPU memory during model fine-tuning.",
        "Use smaller micro batches, enable gradient checkpointing, and reduce "
        "sequence length where possible.",
    ),
    (
        "Rewrite in a professional tone: this config is broken, fix it.",
        "This configuration appears to be invalid. Please review and correct "
        "the related settings.",
    ),
    (
        "Answer briefly: what is Kubernetes used for?",
        "Kubernetes is used to deploy, schedule, scale, and manage containers.",
    ),
    (
        "Create a commit message for adding a small SFT training example.",
        "Add low-VRAM verl SFT example",
    ),
]

TEST_EXAMPLES = [
    (
        "Summarize: Training failed because the dataset path was missing.",
        "Training failed due to a missing dataset path.",
    ),
    (
        "Give one reason to use gradient checkpointing.",
        "Gradient checkpointing reduces activation memory during training.",
    ),
]


def write_split(examples, path: Path):
    rows = [{"question": question, "answer": answer} for question, answer in examples]
    pd.DataFrame(rows).to_parquet(path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--output-dir", default="/data/tiny_sft")
    args = parser.parse_args()

    output_dir = Path(args.output_dir)
    output_dir.mkdir(parents=True, exist_ok=True)

    write_split(TRAIN_EXAMPLES, output_dir / "train.parquet")
    write_split(TEST_EXAMPLES, output_dir / "test.parquet")
    print(f"Wrote tiny SFT parquet files to {output_dir}")


if __name__ == "__main__":
    main()
