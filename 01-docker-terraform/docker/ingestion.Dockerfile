
FROM python:3.12.8

WORKDIR /app

# Install any needed packages specified in requirements.txt
RUN pip install --no-cache-dir pandas psycopg2 sqlalchemy

# Copy the rest of the application code
COPY ./data /data
COPY ./load_nyc_data.py .

# Keep container running
CMD ["tail", "-f", "/dev/null"]
