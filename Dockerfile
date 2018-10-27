FROM mrkcse/docker-python-librdkafka

RUN mkdir -p /usr/src/app
WORKDIR /usr/src/app

COPY setup.py /usr/src/app
COPY ftrigger /usr/src/app/ftrigger
ARG SETUP_COMMAND=install
RUN wget https://files.pythonhosted.org/packages/df/93/c18e2633d770952e628e8a81dfa0ee5bbb23c342b37ad5eb3fdbb9f05d15/confluent_kafka-0.11.6-cp27-cp27m-manylinux1_x86_64.whl
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
    pip install confluent-kafka==0.11.6 && \
    pip install multiprocessing-logging && \
    apk del .build-deps
   

LABEL maintainer="King Chung Huang <kchuang@ucalgary.ca>" \
      org.label-schema.schema-version="1.0" \
      org.label-schema.name="Function Triggers" \
      org.label-schema.vcs-url="https://github.com/muhammadrezaulkarim/ftrigger"
