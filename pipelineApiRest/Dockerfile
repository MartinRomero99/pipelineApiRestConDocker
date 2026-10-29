# Imagen base con Python
FROM python:3.12-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /pipelineApiRest

# Instalar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código del proyecto
COPY main.py config.py ./

# Ejecutar el pipeline
CMD ["python", "main.py"]