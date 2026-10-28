# DLRover Workspace 训练进程生命周期设计

本文档内容已整合到统一设计文档：

- [DLRover Workspace 与 Jupyter 集成设计](./dlrover-workspace-jupyter-cn.md)

统一文档现在同时覆盖：

- Workspace/Jupyter 集成背景与目标。
- 环境生命周期与训练生命周期分层。
- `dlrover-workspace launch`、`stop`、`status`、`logs` 的进程管理逻辑。
- DLRover rendezvous、运行时资源监控和 GPU 垂直弹性。
- 训练框架支持情况、当前使用限制、失败处理和生产建议。

后续请维护统一设计文档，避免生命周期设计与 Workspace/Jupyter 设计分叉。
