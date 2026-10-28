# DLRover Workspace

This example starts a DLRover managed multi-node workspace instead of starting
training immediately.

## Start Workspace

```bash
kubectl apply -f rbac.yaml
kubectl apply -f jupyter_elastic_job.yaml
export NODE_IP=$(kubectl get nodes -o jsonpath='{.items[0].status.addresses[?(@.type=="InternalIP")].address}')
export JUPYTER_NODE_PORT=$(kubectl get svc -n dlrover torch-workspace-jupyter -o jsonpath='{.spec.ports[0].nodePort}')
```

Open `http://${NODE_IP}:${JUPYTER_NODE_PORT}/?token=dlrover`.

After opening Jupyter Lab, see
[`JUPYTER_LAB_GUIDE.md`](./JUPYTER_LAB_GUIDE.md) or
[`JUPYTER_LAB_GUIDE_CN.md`](./JUPYTER_LAB_GUIDE_CN.md) for a complete command
guide.

The Jupyter Service uses `NodePort` by default. Set
`dlrover.io/jupyter-service-type` to `ClusterIP` or `LoadBalancer` in the
ElasticJob annotations if your cluster exposes notebooks through an internal
gateway or cloud load balancer.

For `NodePort`, set `dlrover.io/jupyter-node-port` to request a fixed nodePort.
The value must be within the cluster `service-node-port-range`; otherwise leave
it empty and let Kubernetes allocate the port.

The image used by worker pods must contain `jupyter lab` and DLRover. If your
training image does not include Jupyter, build a workspace image from the same
base image and install JupyterLab.

## Check Workers

Run in a Jupyter terminal or notebook cell:

```bash
dlrover-workspace status
```

## Launch Training

Run the same training command on all worker pods:

```bash
dlrover-workspace launch --framework-cmd \
  "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

Enable DLRover automatic networking explicitly when you want the workspace
workers to rendezvous through DLRover:

```bash
dlrover-workspace launch --network-mode all --framework-cmd \
  "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

To network only selected worker pods, pass `--network-mode ranks` together
with `--ranks`:

```bash
dlrover-workspace launch --network-mode ranks --ranks 0,1 --framework-cmd \
  "python examples/pytorch/mnist/cnn_train.py --num_epochs 5"
```

The command is launched in the background by default. Logs are stored in
`/tmp/dlrover-workspace/train.rank_<rank>.log` inside each worker pod.

```bash
dlrover-workspace logs --tail 100
dlrover-workspace stop
```
