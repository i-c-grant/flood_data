# Start from the official Apache Airflow image
FROM apache/airflow:2.7.3

USER root

# Install system dependencies
RUN apt-get update && apt-get install -y \
    build-essential \
    curl \
    wget \
    git \
    && rm -rf /var/lib/apt/lists/*

# Install micromamba
RUN curl -Ls https://micro.mamba.pm/api/micromamba/linux-64/latest | tar -xvj bin/micromamba
RUN mv bin/micromamba /usr/local/bin/ && rmdir bin

# Set up micromamba environment
ENV MAMBA_ROOT_PREFIX=/opt/conda
ENV PATH="/opt/conda/bin:$PATH"

# Copy environment file
COPY environment.yml /tmp/environment.yml

# Create and configure the conda environment
RUN micromamba create -y -f /tmp/environment.yml && \
    micromamba shell init -s bash $MAMBA_ROOT_PREFIX

# Switch back to airflow user
USER airflow

# Add micromamba shell initalization to bashrc
RUN echo 'eval "$(micromamba shell hook --shell bash)"' >> ~/.bashrc

# The default CMD from the Airflow image will still work
