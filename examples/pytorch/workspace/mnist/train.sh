
# 全组网

dlrover-workspace launch \
  --network-mode all \
  --dlrover-run-args "--nnodes=1 --nproc_per_node=1 --max_restarts=3" \
  --framework-cmd "/nas/code/dev/my-repository/test/dlrover-test/examples/pytorch/mnist/cnn_train.py --num_epochs 5 \
      --training_data /nas/train-data/dataset/mnist_png/training/ \
      --validation_data /nas/train-data/dataset/mnist_png/testing/"

# 部分组网

# dlrover-workspace launch \
#   --network-mode ranks \
#   --ranks 0,1 \
#   --dlrover-run-args "--nnodes=2:\$NODE_NUM --nproc_per_node=1 --max_restarts=3" \
#   --framework-cmd "/nas/code/dev/my-repository/test/dlrover-test/examples/pytorch/mnist/cnn_train.py --num_epochs 5 \
#       --training_data /nas/train-data/dataset/mnist_png/training/ \
#       --validation_data /nas/train-data/dataset/mnist_png/testing/"