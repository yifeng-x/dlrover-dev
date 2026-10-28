from kubernetes import client, config
from kubernetes.client.rest import ApiException
from dlrover.brain.python.common.log import default_logger as logger


class ResourceMonitor(object):
    def __init__(self, namespace, gpu_resource_name):
        self.namespace = namespace
        self.gpu_resource_name = gpu_resource_name
        # 加载 K8s 配置
        # 如果在集群内运行，使用 config.load_incluster_config()
        # 如果在本地开发，使用 config.load_kube_config()
        try:
            config.load_incluster_config()
        except config.ConfigException:
            config.load_kube_config() # 运行在本地 kubeconfig 模式
            

        self.v1 = client.CoreV1Api()

    def get_cluster_idle_gpus(self) -> int:
        """
            获取集群中每个节点的空闲 GPU 数量。
            逻辑：空闲数 = 节点总容量 (Capacity) - 已分配量 (Allocated)
            
            注意：这里的“已分配”指的是 K8s 调度器已经承诺给 Pod 的资源，
            不一定代表 GPU 正在满负载运行，但代表该卡已被预留。
        """
        try:
            nodes = self.v1.list_node()
            
            total_idle = 0
            for node in nodes.items:
                node_name = node.metadata.name

                # 获取节点最大可分配GPU总量 (Allocatable)，通常等于物理插卡数
                max_gpus = 0
                if node.status.allocatable and self.gpu_resource_name in node.status.allocatable:
                    max_gpus = int(node.status.allocatable[self.gpu_resource_name])

                # 过滤出调度到该节点且正在运行的 Pod
                # field_selector 用于筛选特定节点上的 Pod
                field_selector = f"spec.nodeName={node_name}"
                pods = self.v1.list_pod_for_all_namespaces(field_selector=field_selector)
                
                # 获取当前已分配的 GPU 数量
                current_allocated_gpus = 0
                for pod in pods.items:
                    # 忽略非运行状态的 Pod (如 Succeeded, Failed)，只计算 Running 或 Pending 占用的资源
                    if pod.status.phase not in ['Running', 'Pending']:
                        continue
                    
                    for container in pod.spec.containers:
                        if container.resources and container.resources.requests:
                            request_gpus = container.resources.requests.get(self.gpu_resource_name)
                            if request_gpus:
                                # 处理可能是字符串 "1" 或整数的情况
                                current_allocated_gpus += int(request_gpus)
                
                idle_gpu_count = max_gpus - current_allocated_gpus
                
                # 确保不会出现负数（防止数据不一致）
                idle_count = max(0, idle_gpu_count)

                total_idle += idle_count

                logger.info(
                    f"total_gpuas:{max_gpus}," 
                    f"current_allocated_gpus:{current_allocated_gpus}," 
                    f"idle_gpus:{idle_count}"
                )

            logger.info(f"cluster total idle gpus:{total_idle}")
            
            return total_idle

        except ApiException as e:
            logger.error(f"Call k8s API exception：{e}")
            return 0
        except Exception as e:
            logger.error(f"unknown exception：{e}")
            return 0