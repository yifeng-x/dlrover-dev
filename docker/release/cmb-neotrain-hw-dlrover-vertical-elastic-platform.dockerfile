
FROM registry.cic.cmbchina.cn/cic-arm/cmb-neotrain-dlrover-arm64:v0.7.0-hw-v3-20260204 as base

USER root
ENV TZ=Asia/Shanghai
ENV http_proxy=http://55.32.168.30:3128
ENV https_proxy=http://55.32.168.30:3128

WORKDIR /neotrain

COPY ./dist/dlrover-0.7.0.dev0-py3-none-any.whl .
# COPY ./ms-swift-elastic/LD72.05_cmb_swift/dist/ms_swift-4.0.0.dev0-py3-none-any.whl .
RUN pip uninstall -y dlrover
RUN pip install --force-reinstall --no-cache-dir ./dlrover-0.7.0.dev0-py3-none-any.whl -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn

# RUN pip install -U ./ms_swift-4.0.0.dev0-py3-none-any.whl -U -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn
RUN rm -f ./*.whl

ENV http_proxy=
ENV https_proxy=

USER ai-user

ENTRYPOINT ["/bin/bash"]