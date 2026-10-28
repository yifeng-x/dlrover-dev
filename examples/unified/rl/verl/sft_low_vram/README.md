# 16GB Single-GPU verl SFT Example on Kubernetes

This example runs a small supervised fine-tuning job with verl on one NVIDIA
GPU. It is designed as a local Kubernetes smoke test for a 16GB laptop GPU.

Unlike the PPO/GRPO examples, SFT is a simple SPMD training workload. The
Kubernetes `ElasticJob` starts one GPU pod and runs verl's FSDP SFT trainer
through `torchrun`.

## What It Uses

- Model: `Qwen/Qwen2.5-0.5B-Instruct`
- Trainer: `verl.trainer.fsdp_sft_trainer`
- Fine-tuning mode: LoRA, rank 8
- Dataset: tiny parquet files with `question` and `answer` columns
- GPU: 1 x `nvidia.com/gpu`

## Files

- `run_sft_16g.sh`: low-VRAM SFT launch script.
- `prepare_tiny_sft.py`: writes a tiny instruction dataset.
- `elastic_job_16g.yaml`: Kubernetes `ElasticJob` for one GPU pod.
- `Dockerfile`: example image based on the official verl vLLM image.

## Build Image

From the repository root:

```bash
docker build -t localhost/dlrover-verl:latest \
  -f examples/unified/rl/verl/sft_low_vram/Dockerfile .
```

If your local Kubernetes runtime has a separate image store:

```bash
kind load docker-image localhost/dlrover-verl:latest
```

or:

```bash
minikube image load localhost/dlrover-verl:latest
```

## Submit

Make sure the DLRover `ElasticJob` CRD/controller and NVIDIA device plugin are
already installed:

```bash
kubectl apply -f examples/unified/rl/verl/sft_low_vram/elastic_job_16g.yaml
kubectl -n dlrover logs -f job/verl-sft-16g
```

If logs are not exposed through a Job object:

```bash
kubectl -n dlrover get pods -l elasticjob-name=verl-sft-16g
kubectl -n dlrover logs -f <worker-pod-name>
```

## Run Without Kubernetes

Inside an environment that already has `verl`, `torch`, `transformers`,
`pandas`, and `pyarrow` installed:

```bash
python examples/unified/rl/verl/sft_low_vram/prepare_tiny_sft.py \
  --output-dir /tmp/tiny_sft
DATA_DIR=/tmp/tiny_sft \
OUTPUT_DIR=/tmp/verl_sft_16g \
bash examples/unified/rl/verl/sft_low_vram/run_sft_16g.sh
```

## Low-VRAM Knobs

The default script is already conservative:

- `data.micro_batch_size_per_gpu=1`
- `data.train_batch_size=8`
- `data.max_length=512`
- `model.enable_gradient_checkpointing=True`
- `model.fsdp_config.offload_params=True`
- `model.lora_rank=8`

If 16GB still OOMs, first reduce sequence length:

```bash
bash examples/unified/rl/verl/sft_low_vram/run_sft_16g.sh \
  data.max_length=256 \
  data.train_batch_size=4
```

After the smoke test passes, replace `TRAIN_FILE` and `VAL_FILE` with your real
parquet files. Keep the same `question` and `answer` columns or update
`data.prompt_key` and `data.response_key`.
