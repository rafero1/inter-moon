FROM python:3.6.9

# Install dependencies
RUN apt-get update && apt-get install -y \
    make \
    automake \
    gcc \
    g++ \
    build-essential \
    subversion \
    python3-dev \
    libffi-dev \
    musl-dev \
    libpq-dev \
    bash \
    curl \
    git \
    jq \
    openssh-client \
    rsync \
    tar \
    unzip \
    wget \
    zip

# Copy files
COPY moon /var/www/moon

WORKDIR /var/www/moon

# update pip
RUN pip3 install --upgrade pip setuptools wheel
RUN pip3 install --upgrade pip

# Install python dependencies
RUN pip3 install -r requirements.txt

RUN chmod +x entrypoint.sh

# Run service on container startup
ENTRYPOINT ["/var/www/moon/entrypoint.sh"]
