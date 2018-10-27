FROM mrkcse/docker-python-librdkafka

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY setup.py /usr/src/app
COPY ftrigger /usr/src/app/ftrigger
ARG SETUP_COMMAND=install

RUN   apk update \                                                                                                                                                                                                                        
  &&   apk add ca-certificates wget \                                                                                                                                                                                                      
  &&   update-ca-certificates \
  && pip install --upgrade pip \
  && pip3 install --upgrade --user pip

RUN wget https://files.pythonhosted.org/packages/dd/0a/c8bbf50a0b8e1d623521565e4c3211aebf7c9a7cf7eeb35b5360132a0ccc/confluent_kafka-0.11.6-cp36-cp36m-manylinux1_x86_64.whl

RUN apk add --no-cache --virtual .build-deps \
        autoconf \
        automake \
        gcc \
        git \
        libtool \
        make \
        musl-dev && \
    python setup.py ${SETUP_COMMAND} && \
    pip install kafka-python && \
    python -c "import pip._internal; print(pip._internal.pep425tags.get_supported())" && \
    pip3 install confluent_kafka-0.11.6-cp36-cp36m-manylinux1_x86_64.whl && \
    pip install multiprocessing-logging && \
    apk del .build-deps
   

LABEL maintainer="King Chung Huang <kchuang@ucalgary.ca>" \
      org.label-schema.schema-version="1.0" \
      org.label-schema.name="Function Triggers" \
      org.label-schema.vcs-url="https://github.com/muhammadrezaulkarim/ftrigger"
