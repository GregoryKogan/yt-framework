FROM mirror.gcr.io/ubuntu:noble

USER root

RUN apt-get update \
  && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y software-properties-common \
  && apt-get clean \
  \
  && add-apt-repository ppa:deadsnakes/ppa

RUN apt-get update \
  \
  && DEBIAN_FRONTEND=noninteractive TZ=Etc/UTC apt-get install -y \
  containerd \
  curl wget \
  git gcc g++\
  less vim nano jq \
  gdb iperf tcpdump google-perftools \
  lsof htop iotop strace telnet ethtool iproute2 iputils-ping dnsutils netcat-openbsd procps \
  tini \
  zstd unzip \
  lsb-release \
  libidn11-dev \
  openjdk-11-jdk \
  python3.11 python3.11-full python3.11-distutils python3.11-dev \
  \
  && apt-get clean \
  \
  && update-alternatives --install /usr/bin/python3 python3 /usr/bin/python3.11 1 \
  && update-alternatives --install /usr/bin/python python /usr/bin/python3.11 1 \
  \
  && curl https://bootstrap.pypa.io/get-pip.py -o get-pip.py \
  && python3.11 get-pip.py \
  && python3.11 -m pip install --upgrade pip setuptools wheel \
  && rm get-pip.py && apt-get remove -y python3-pyparsing && apt-get autoremove -y

RUN apt-get remove -y python3-pyparsing 
RUN pip install \
  ytsaurus-client \
  ytsaurus-yson \
  ytsaurus-spyt==2.5.1 
RUN pip install \
  ytsaurus-client \
  ytsaurus-yson \ 
  ytsaurus-spyt==2.5.1 \
  annotated-types==0.7.0 \
  boto3==1.35.65 \
  botocore==1.35.65 \
  Brotli==1.1.0 \
  click==8.1.7 \
  FastWARC==0.14.9 \
  jmespath==1.0.1 \
  numpy==1.26.4 \
  pandas==2.2.3 \
  py4j==0.10.9.7 \
  pyarrow==18.0.0 \
  pydantic==2.9.2 \
  pydantic_core==2.23.4 \
  pyparsing==3.2.0 \
  pyspark==3.4.1 \
  python-dateutil==2.9.0.post0 \
  pytz==2024.2 \
  s3transfer==0.10.3 \
  six==1.16.0 \
  sparkdantic==1.5.0 \
  tqdm==4.67.0 \
  typing_extensions==4.12.2 \
  tzdata==2024.2 \
  urllib3==2.2.3 \
  venv-pack==0.2.0 \
  warcio==1.7.5 \
  charset-normalizer==3.4.0 \
  python-magic==0.4.27 \
  pillow==11.0.0 \
  ImageHash==4.3.1 \
  exif==1.6.1 \
  opencv-python-headless==4.10.0.84 \
  openpyxl==3.1.5 \
  wrapt_timeout_decorator==1.5.1 \
  psycopg2-binary==2.9.9 \
  brotlipy==0.7.0 \
  zstandard==0.19.0 \
  tensorflow>=2.2.0 \
  tensorflow-hub \
  tf-keras \
  opennsfw2==0.14.0 \
  mosaicml-streaming

RUN pip install fsspec==2026.1.0 omegaconf==2.3.0 ultralytics
