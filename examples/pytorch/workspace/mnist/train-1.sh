dlrover-workspace launch --cli-log on --network-mode ranks --ranks 0,1 --framework-cmd \
  "/nas/code/dev/my-repository/test/dlrover-test/examples/pytorch/mnist/cnn_train.py --num_epochs 5 \
      --training_data /nas/train-data/dataset/mnist_png/training/ \
      --validation_data /nas/train-data/dataset/mnist_png/testing/"