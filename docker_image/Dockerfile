# Mirror from https://hub.docker.com/_/postgres
FROM vidardb/postgres:12.4

ARG apt_opts
ARG env_exts

# Add bootstrap entrypoint and util tools
COPY docker-entrypoint.sh install-madlib.sh /usr/local/bin/
RUN chmod 777 /usr/local/bin/docker-entrypoint.sh /usr/local/bin/install-madlib.sh

# Install VidarDB Engine
# https://launchpad.net/ubuntu/+source/snappy/1.1.7-1/+build/13663202/+files/libsnappy1v5_1.1.7-1_amd64.deb
# http://launchpadlibrarian.net/343996427/libsnappy-dev_1.1.7-1_amd64.deb
ENV VIDARDB_VERSION=master
RUN set -xe && export $env_exts && apt-get $apt_opts update && apt-get $apt_opts install -y --no-install-recommends gcc-8 g++-8 make wget cmake && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-8 800 --slave /usr/bin/g++ g++ /usr/bin/g++-8 && \
    wget --no-check-certificate -O libsnappy1v5_1.1.7-1_amd64.deb https://launchpad.net/ubuntu/+source/snappy/1.1.7-1/+build/13663202/+files/libsnappy1v5_1.1.7-1_amd64.deb && \
    wget --no-check-certificate -O libsnappy-dev_1.1.7-1_amd64.deb http://launchpadlibrarian.net/343996427/libsnappy-dev_1.1.7-1_amd64.deb && \
    wget --no-check-certificate -O vidardb.tar.gz https://github.com/vidardb/vidardb-engine/archive/${VIDARDB_VERSION}.tar.gz && \
    apt-get install -y ./libsnappy1v5_1.1.7-1_amd64.deb ./libsnappy-dev_1.1.7-1_amd64.deb && \
    tar zxvf vidardb.tar.gz && cd vidardb-engine-${VIDARDB_VERSION} && CMAKE_FLAGS="-DWITH_SNAPPY=ON" make install && rm -rf /usr/lib64/libvidardb.a && cd .. && \
    echo "/usr/lib64" >> /etc/ld.so.conf.d/libc.conf && ldconfig && apt-get purge -y --auto-remove make wget cmake && \
    rm -rf /var/lib/apt/lists/* /tmp/* vidardb.tar.gz vidardb-engine-${VIDARDB_VERSION} libsnappy1v5_1.1.7-1_amd64.deb libsnappy-dev_1.1.7-1_amd64.deb

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
    apt-get $apt_opts install -y --no-install-recommends wget ca-certificates postgresql-plpython-${POSTGRES_VERSION} python m4 && \
    wget --no-check-certificate -O apache-madlib-${MADLIB_VERSION}-bin-Linux.deb https://dist.apache.org/repos/dist/release/madlib/${MADLIB_VERSION}/apache-madlib-${MADLIB_VERSION}-bin-Linux.deb && \
    dpkg -i apache-madlib-${MADLIB_VERSION}-bin-Linux.deb && \
    apt-get purge -y --auto-remove wget ca-certificates && \
    rm -rf /var/lib/apt/lists/* /tmp/* apache-madlib-${MADLIB_VERSION}-bin-Linux.deb
