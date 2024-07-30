FROM bitnami/spark:latest

# Installing Python packages
RUN apt-get update && apt-get install -y python3-pip && \
    pip3 install pandas scikit-learn 
