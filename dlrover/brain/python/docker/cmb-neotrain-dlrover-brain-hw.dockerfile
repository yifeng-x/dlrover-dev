FROM dlrover-ci:master-0.7.0 as base

ENV TZ=Asia/Shanghai
ENV http_proxy=http://55.32.168.30:3128
ENV https_proxy=http://55.32.168.30:3128

WORKDIR /neotrain

COPY ./dist/dlrover-0.7.0.dev0-py3-none-any.whl .

RUN pip install grpcio-tools deprecated kubernetes fastapi uvicorn aiohttp pydantic perfetto tqdm setuptools filelock \
    -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn

RUN pip install --force-reinstall --no-cache-dir ./dlrover-0.7.0.dev0-py3-none-any.whl -i https://repo.huaweicloud.com/repository/pypi/simple --trusted-host central.jaf.cmbchina.cn
RUN rm -f ./*.whl

ENV http_proxy=
ENV https_proxy=

ENTRYPOINT ["/bin/bash"]