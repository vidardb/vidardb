# Mirror from https://hub.docker.com/_/postgres
FROM vidardb/postgres:12.4

ARG apt_opts
ARG env_exts

# Add bootstrap entrypoint and util tools
COPY docker-entrypoint.sh install-madlib.sh /usr/local/bin/
RUN chmod 777 /usr/local/bin/docker-entrypoint.sh /usr/local/bin/install-madlib.sh

# Install VidarDB Engine
ENV VIDARDB_VERSION=master
RUN set -xe && export $env_exts && apt-get $apt_opts update && apt-get $apt_opts install -y --no-install-recommends gcc-8 g++-8 make wget cmake libsnappy1v5 libsnappy-dev && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8 && \
    wget --no-check-certificate -O vidardb.tar.gz https://github.com/vidardb/vidardb-engine/archive/${VIDARDB_VERSION}.tar.gz && \
    tar zxvf vidardb.tar.gz && cd vidardb-engine-${VIDARDB_VERSION} && CMAKE_FLAGS="-DWITH_SNAPPY=ON" make install && rm -rf /usr/local/lib/libvidardb.a && cd .. && \
    ldconfig && apt-get purge -y --auto-remove make wget cmake && rm -rf /var/lib/apt/lists/* /tmp/* vidardb.tar.gz vidardb-engine-${VIDARDB_VERSION}

# Install PostgresForeignDataWrapper
ENV POSTGRES_VERSION=12
ENV FDW_VERSION=master
RUN set -xe && export $env_exts && apt-get $apt_opts update && \
    apt-get $apt_opts install -y --no-install-recommends make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    wget --no-check-certificate -O pgrocks-fdw.tar.gz https://github.com/vidardb/pgrocks-fdw/archive/${FDW_VERSION}.tar.gz && \
    tar zxvf pgrocks-fdw.tar.gz && cd pgrocks-fdw-${FDW_VERSION} && make VIDARDB=true && make install && cd .. && \
    apt-get purge -y --auto-remove make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    rm -rf /var/lib/apt/lists/* /tmp/* pgrocks-fdw.tar.gz pgrocks-fdw-${FDW_VERSION}

# Install AI Functionality
ENV MADLIB_VERSION=1.17.0
RUN set -xe && export $env_exts && apt-get $apt_opts update && \
    apt-get $apt_opts install -y --no-install-recommends wget ca-certificates postgresql-plpython3-${POSTGRES_VERSION} python3 m4 && \
    wget --no-check-certificate -O apache-madlib-${MADLIB_VERSION}-bin-Linux.deb https://dist.apache.org/repos/dist/release/madlib/${MADLIB_VERSION}/apache-madlib-${MADLIB_VERSION}-bin-Linux.deb && \
    apt-get $apt_opts install -y ./apache-madlib-${MADLIB_VERSION}-bin-Linux.deb && ln -s /usr/bin/python3 /usr/bin/python && \
    apt-get purge -y --auto-remove wget ca-certificates && \
    rm -rf /var/lib/apt/lists/* /tmp/* apache-madlib-${MADLIB_VERSION}-bin-Linux.deb
