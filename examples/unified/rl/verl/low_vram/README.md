# 16GB Single-GPU verl GRPO Example on Kubernetes

This example runs a small verl GRPO job through DLRover Unified on a local
Kubernetes cluster with one NVIDIA GPU. It is tuned for a 16GB laptop GPU, so
the goal is to validate the training loop rather than maximize throughput.

## What It Uses

- Model: `Qwen/Qwen2.5-0.5B-Instruct`
- Algorithm: GRPO, which avoids a critic worker and saves memory
- GPU: 1 x `nvidia.com/gpu`
- Dataset: a tiny GSM8K-style parquet dataset generated in the pod
- Rollout backend: vLLM with conservative memory settings

The sample also updates the generic verl DLRover example so that critic and
reference workers are only created when the verl config actually needs them.

## Files

- `run_grpo_16g.sh`: single-GPU low-VRAM GRPO launch script.
- `prepare_tiny_gsm8k.py`: writes a tiny parquet dataset for smoke testing.
- `elastic_job_16g.yaml`: Kubernetes `ElasticJob` for one GPU pod.
- `Dockerfile`: example image based on the official verl vLLM image.

## Build Image

From the repository root:

```bash
docker build -t localhost/dlrover-verl:latest \
  -f examples/unified/rl/verl/low_vram/Dockerfile .
```

If your local Kubernetes runtime cannot see Docker's local image store, load the
image into the cluster runtime, for example:

```bash
kind load docker-image localhost/dlrover-verl:latest
```

or:

```bash
minikube image load localhost/dlrover-verl:latest
```

## Submit

Make sure the DLRover `ElasticJob` CRD/controller and NVIDIA device plugin are
already installed, then submit:

```bash
kubectl apply -f examples/unified/rl/verl/low_vram/elastic_job_16g.yaml
kubectl -n dlrover logs -f job/verl-grpo-16g
```

If your cluster does not expose logs through a Job object, check the worker pod:

```bash
kubectl -n dlrover get pods -l elasticjob-name=verl-grpo-16g
kubectl -n dlrover logs -f <worker-pod-name>
```

## Run Without Kubernetes

Inside an environment that already has `dlrover`, `verl`, `torch`, `vllm`,
`pandas`, and `pyarrow` installed:

```bash
python examples/unified/rl/verl/low_vram/prepare_tiny_gsm8k.py \
  --output-dir /tmp/gsm8k
DATA_DIR=/tmp/gsm8k bash examples/unified/rl/verl/low_vram/run_grpo_16g.sh
```

## Low-VRAM Knobs

If the pod OOMs on a 16GB GPU, try these in order:

```bash
ROLLOUT_GPU_MEMORY_UTILIZATION=0.22 \
TOTAL_TRAINING_STEPS=3 \
bash examples/unified/rl/verl/low_vram/run_grpo_16g.sh \
  data.max_response_length=64 \
  actor_rollout_ref.rollout.max_num_batched_tokens=256
```

The most important settings are:

- `algorithm.adv_estimator=grpo`: avoids the critic model.
- `actor_rollout_ref.rollout.n=2`: minimum useful group size for GRPO.
- `ppo_micro_batch_size_per_gpu=1`: keeps actor update memory low.
- `param_offload=True` and `optimizer_offload=True`: moves FSDP state to CPU.
- `gpu_memory_utilization=0.28`: leaves room for actor training on one GPU.
- `max_prompt_length=256` and `max_response_length=128`: limits KV cache size.

For a longer run, increase `TOTAL_TRAINING_STEPS` after the smoke test passes.
