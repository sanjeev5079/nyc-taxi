FROM mauriciojost/scala-sbt-ci:openjdk8-scala2.12.8-sbt1.2.8-0.2.0
COPY pip.ini  /etc/pip.conf
RUN sed -i 's/deb\.debian\.org/nexus\.se\.telenor\.net\/repository/g'  /etc/apt/sources.list && \
	sed -i 's/security\.debian\.org/nexus\.se\.telenor\.net\/repository/g'  /etc/apt/sources.list
RUN apt-get update --fix-missing && apt-get install -y  --no-install-recommends \
    git \
    python3\
    python3-setuptools\
    python3.5-dev\
    gcc \
    telnet \
    && easy_install3  pip  \
    && rm -rf /var/lib/apt/lists/*

RUN adduser jenkins --system --disabled-login --uid 1002 && \
    chown -R 1002:1002 /home/jenkins

USER jenkins