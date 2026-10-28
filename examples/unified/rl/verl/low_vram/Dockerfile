FROM verlai/verl:vllm011.latest

WORKDIR /workspace/dlrover

COPY . /workspace/dlrover

RUN pip install --no-cache-dir -e .

ENV PYTHONPATH=/workspace/dlrover
