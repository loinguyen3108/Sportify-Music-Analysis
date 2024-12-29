FROM python:3.12

RUN apt-get update && apt-get install -y \
    cmake \
    curl \
    g++ \
    gcc \
    git \
    gnupg2 \
    net-tools \
    sysstat \
    htop \
    libgl1-mesa-glx \
    libglib2.0-0 \
    libnss3 \
    libgconf-2-4 \
    libfontconfig1 \
    nano \
    && rm -rf /var/lib/apt/lists/*

RUN CHROMEDRIVER_VERSION=`curl -sS chromedriver.storage.googleapis.com/LATEST_RELEASE` \
    && mkdir -p /opt/chromedriver-$CHROMEDRIVER_VERSION  \
    && curl -sS -o /tmp/chromedriver_linux64.zip http://chromedriver.storage.googleapis.com/$CHROMEDRIVER_VERSION/chromedriver_linux64.zip  \
    && unzip -qq /tmp/chromedriver_linux64.zip -d /opt/chromedriver-$CHROMEDRIVER_VERSION  \
    && rm /tmp/chromedriver_linux64.zip  \
    && chmod +x /opt/chromedriver-$CHROMEDRIVER_VERSION/chromedriver  \
    && ln -fs /opt/chromedriver-$CHROMEDRIVER_VERSION/chromedriver /usr/local/bin/chromedriver

# Add Google Chrome repository to the system
RUN curl -sS -o - https://dl-ssl.google.com/linux/linux_signing_key.pub | apt-key add - \
    && echo "deb http://dl.google.com/linux/chrome/deb/ stable main" >> /etc/apt/sources.list.d/google-chrome.list \
    && apt-get -y update \
    && apt-get install -y google-chrome-stable

RUN useradd --create-home spotify
USER spotify
RUN mkdir -p /home/spotify/spotify_web
WORKDIR /home/spotify/spotify_web

COPY --chown=spotify:spotify . /home/spotify/spotify_web
ENV PATH="/home/spotify/.local/bin:${PATH}"
RUN pip install --upgrade --no-cache-dir pip \
    && pip install --no-cache-dir -r /home/spotify/spotify_web/requirements.txt
