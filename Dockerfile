FROM python:3.11-slim

# Installer outils + yq + unzip + jq + htop
RUN apt-get update && apt-get install -y bash curl jq unzip htop && \
    curl -L https://github.com/mikefarah/yq/releases/latest/download/yq_linux_amd64 -o /usr/bin/yq && \
    chmod +x /usr/bin/yq && rm -rf /var/lib/apt/lists/*


RUN apt-get update && apt-get install -y jq

 WORKDIR /app

# Dépendances Python
COPY requirements.txt .
RUN pip install --upgrade pip && pip install -r requirements.txt

# Copier les fichiers nécessaires
COPY orchestration/ ./orchestration/
COPY monitoring/ ./monitoring/
COPY processing/ ./processing/
COPY config/ ./config/
COPY transformations/ ./transformations/
# 🔐 Rendre exécutables les scripts nécessaires
RUN chmod +x ./orchestration/*.sh \
    && chmod +x ./monitoring/*.py \
    && chmod +x ./processing/*.py \
    && chmod +x ./transformations/*.py 
# On reste root mais le conteneur s'exécutera avec le bon UID via docker-compose
ARG USER_ID=1000
ARG GROUP_ID=1000

RUN addgroup --gid $GROUP_ID appgroup && \
    adduser --disabled-password --gecos '' --uid $USER_ID --gid $GROUP_ID appuser && \
    chown -R $USER_ID:$GROUP_ID /app

USER appuser

CMD ["bash", "/app/orchestration/pipeline_master.sh"]
