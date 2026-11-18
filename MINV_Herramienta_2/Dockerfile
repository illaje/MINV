FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY core.py app.py messaging.py ./
CMD ["python", "-u", "app.py"]
