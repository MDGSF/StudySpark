FROM spark:3.5.2-scala2.12-java17-python3-ubuntu

USER root

ENV DEBIAN_FRONTEND=noninteractive

RUN set -x \
  && apt-get update -y \
  && apt-get install -y --reinstall ca-certificates

COPY ./sources.list /etc/apt/

ENV TZ=Asia/Shanghai

RUN set -ex; \
  apt-get update -y; \
  apt install -y gpg-agent wget curl; \
  ln -snf /usr/share/zoneinfo/$TZ /etc/localtime; \
  echo $TZ > /etc/timezone; \
  rm -rf /var/lib/apt/lists/*

RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark==3.5.2
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple delta-spark==3.2.0
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple numpy
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pandas
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple matplotlib
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple nuscenes-devkit

# USER spark
