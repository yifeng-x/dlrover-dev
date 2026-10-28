#!/usr/bin/env bash
# Copyright 2026 The DLRover Authors. All rights reserved.
# Licensed under the Apache License, Version 2.0.

set -euo pipefail

MODEL_PATH="${MODEL_PATH:-Qwen/Qwen2.5-0.5B-Instruct}"
DATA_DIR="${DATA_DIR:-${HOME}/data/tiny_sft}"
TRAIN_FILE="${TRAIN_FILE:-${DATA_DIR}/train.parquet}"
VAL_FILE="${VAL_FILE:-${DATA_DIR}/test.parquet}"
OUTPUT_DIR="${OUTPUT_DIR:-${HOME}/checkpoints/verl_sft_16g}"
PROJECT_NAME="${PROJECT_NAME:-verl_sft_low_vram}"
EXPERIMENT_NAME="${EXPERIMENT_NAME:-qwen2_5_0_5b_sft_16g}"
TOTAL_EPOCHS="${TOTAL_EPOCHS:-1}"

export PYTHONUNBUFFERED="${PYTHONUNBUFFERED:-1}"
export TOKENIZERS_PARALLELISM="${TOKENIZERS_PARALLELISM:-false}"

torchrun --standalone --nnodes=1 --nproc_per_node=1 \
    -m verl.trainer.sft_trainer \
    data.train_files="${TRAIN_FILE}" \
    data.val_files="${VAL_FILE}" \
    data.micro_batch_size_per_gpu=1 \
    data.messages_key=messages \
    model.path="${MODEL_PATH}" \
    model.use_remove_padding=True \
    optim.lr=1e-5 \
    engine=fsdp \
    trainer.default_local_dir="${OUTPUT_DIR}" \
    trainer.project_name="${PROJECT_NAME}" \
    trainer.experiment_name="${EXPERIMENT_NAME}" \
    trainer.logger='["console"]' \
    trainer.total_epochs="${TOTAL_EPOCHS}" \
    trainer.n_gpus_per_node=1 \
    trainer.nnodes=1 \
    "$@"
