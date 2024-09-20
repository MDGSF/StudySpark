FROM spark:3.5.2-scala2.12-java17-python3-ubuntu

USER root

# RUN set -ex; \
#   apt-get update; \
#   apt-get install -y python3 python3-pip; \
#   rm -rf /var/lib/apt/lists/*

# RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pyspark
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple numpy
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple pandas
RUN pip install -i https://pypi.tuna.tsinghua.edu.cn/simple matplotlib

USER spark
