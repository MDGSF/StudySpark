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
  apt install -y gpg-agent wget; \
  ln -snf /usr/share/zoneinfo/$TZ /etc/localtime; \
  echo $TZ > /etc/timezone; \
  rm -rf /var/lib/apt/lists/*

RUN set -ex; \
  apt-get update -y; \
  apt install -y intel-oneapi-mkl; \
  apt install -y intel-oneapi-mkl-devel; \
  rm -rf /var/lib/apt/lists/*

RUN ln -sf /opt/intel/mkl/lib/intel64/libmkl_rt.so /usr/local/lib/libblas.so.3
RUN ln -sf /opt/intel/mkl/lib/intel64/libmkl_rt.so /usr/local/lib/liblapack.so.3

# RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple numpy
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pandas
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple matplotlib

USER spark
