# Using python 3.7 full image as the base image
FROM python:3.7.7-slim
# Defining working directory and copy the requirements file. We will run the commands inside this new directory
WORKDIR /usr/src/myapp
# Copy requirements.txt  to the working directory
COPY requirements.txt .
# Install required python packages
RUN pip install --no-cache-dir -r requirements.txt
# Copy all files in training-db local host directory to /usr/src/myapp in Docker container
COPY . .
# Expose the port that our app runs in
EXPOSE 5003
# Enviroment Variables
ENV TRAININGDB_API=http://trainingdb_service:5001/training-db/PCAData
ENV MODEL_REPO=model_repo_de2021_steven
ENV PROJECT_ID=jads-de-2021
# Run our App
CMD ["python3","app.py"]

# We can also use an ENTRYPOINT and a CMD
# Run our App
#ENTRYPOINT ["python3"]
#CMD ["app.py"]