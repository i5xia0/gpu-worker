FROM python:3.12-slim

WORKDIR /workspace

COPY worker/requirements.txt /tmp/worker-requirements.txt

RUN pip install --no-cache-dir -r /tmp/worker-requirements.txt

CMD ["python", "/workspace/worker/app.py"]
