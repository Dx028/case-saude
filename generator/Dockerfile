# Gerador de eventos hospitalares simulados
FROM python:3.12-slim

RUN useradd --create-home --uid 10001 gerador
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY gerador.py .

USER gerador
EXPOSE 8000
CMD ["python", "-u", "gerador.py"]
