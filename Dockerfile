# Image
FROM python:3.10
# Working directory
WORKDIR /app
# Copy the present working directory contents into the container at /app
COPY . /app
# Install the dependencies
RUN pip install -r requirements.txt
# To execute the command
CMD ["python", "app.py"]

