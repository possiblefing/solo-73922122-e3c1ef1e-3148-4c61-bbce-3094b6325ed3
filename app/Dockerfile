FROM postgres:16

ENV DEBIAN_FRONTEND=noninteractive
RUN apt-get update \
 && apt-get install -y --no-install-recommends python3 python3-pip \
 && rm -rf /var/lib/apt/lists/*

WORKDIR /opt/app
COPY requirements.txt .
RUN pip3 install --no-cache-dir --break-system-packages -r requirements.txt

COPY main.py .
COPY restore_command.sh /opt/app/restore_command.sh
COPY static/ /opt/app/static/
RUN chmod 755 /opt/app/restore_command.sh

EXPOSE 8080
CMD ["python3", "-m", "uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8080"]
