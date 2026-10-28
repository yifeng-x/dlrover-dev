FROM registry.cic.cmbchina.cn/cic/cmb-neotrain:v3.10.3-hw_train_platform_01_06 as base

USER root
ENV TZ=Asia/Shanghai
# RUN export http_proxy=http://55.32.168.30:3128 && export https_proxy=http://55.32.168.30:3128
ENV http_proxy=http://55.32.168.30:3128
ENV https_proxy=http://55.32.168.30:3128

WORKDIR /neotrain

COPY ./dlrover-0.7.0.dev0-py3-none-any.whl .

# RUN pip install grpcio grpcio-tools protobuf deprecated pyparsing psutil kubernetes \
#     -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn
RUN pip install grpcio-tools deprecated kubernetes \
    -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn

RUN pip install ./dlrover-0.7.0.dev0-py3-none-any.whl -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn
RUN rm -f ./*.whl

# RUN pip install tornado \
#         numpy \
#         gradio \
#         datasets \
#         accelerate \
#         transformers \
#         tokenizers \
#         deepspeed \
#         -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn

RUN pip install deepspeed -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn

# RUN unset http_proxy && unset https_proxy
ENV http_proxy=
ENV https_proxy=

USER ai-user
ENTRYPOINT ["/bin/bash", "-c"]