# Mirror from https://hub.docker.com/_/postgres
FROM vidardb/postgres:11.7

# Install VidarDB
ENV VIDARDB_VERSION=master
RUN set -xe && apt-get update && apt-get install -y --no-install-recommends gcc-6 g++-6 make wget && \
    update-alternatives --install /usr/bin/gcc gcc /usr/bin/gcc-6 800 --slave /usr/bin/g++ g++ /usr/bin/g++-6 && \
    wget --no-check-certificate -O vidardb.tar.gz https://github.com/vidardb/vidardb/archive/${VIDARDB_VERSION}.tar.gz && \
    tar zxvf vidardb.tar.gz && cd vidardb-${VIDARDB_VERSION} && \
    DEBUG_LEVEL=0 PORTABLE=1 make shared_lib install-shared && cd .. && \
    apt-get purge -y --auto-remove make wget && \
    rm -rf /var/lib/apt/lists/* /tmp/* vidardb.tar.gz vidardb-${VIDARDB_VERSION}

# Install PostgresForeignDataWrapper
ENV POSTGRES_VERSION=11
ENV FDW_VERSION=master
RUN set -xe && apt-get update && \
    apt-get install -y --no-install-recommends make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    wget --no-check-certificate -O PostgresForeignDataWrapper.tar.gz https://github.com/vidardb/PostgresForeignDataWrapper/archive/${FDW_VERSION}.tar.gz && \
    tar zxvf PostgresForeignDataWrapper.tar.gz && cd PostgresForeignDataWrapper-${FDW_VERSION} && make VIDARDB=true && make install && cd .. && \
    apt-get purge -y --auto-remove make wget ca-certificates postgresql-server-dev-${POSTGRES_VERSION} && \
    rm -rf /var/lib/apt/lists/* /tmp/* PostgresForeignDataWrapper.tar.gz PostgresForeignDataWrapper-${FDW_VERSION}

# Install AI Functionality
ENV MADLIB_VERSION=1.17.0
RUN set -xe && apt-get update && \
    apt-get install -y --no-install-recommends wget ca-certificates postgresql-plpython-${POSTGRES_VERSION} python m4 && \
    wget --no-check-certificate -O apache-madlib-${MADLIB_VERSION}-bin-Linux.deb https://dist.apache.org/repos/dist/release/madlib/${MADLIB_VERSION}/apache-madlib-${MADLIB_VERSION}-bin-Linux.deb && \
    dpkg -i apache-madlib-${MADLIB_VERSION}-bin-Linux.deb && \
    apt-get purge -y --auto-remove wget ca-certificates && \
    rm -rf /var/lib/apt/lists/* /tmp/* apache-madlib-${MADLIB_VERSION}-bin-Linux.deb

# Add bootstrap entrypoint and util tools
COPY docker-entrypoint.sh install-madlib.sh health-check.sh /usr/local/bin/

# Add health check
HEALTHCHECK --start-period=5s CMD health-check.sh
