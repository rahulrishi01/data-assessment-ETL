FROM python:3.6
COPY . /app
WORKDIR /app
EXPOSE 3306
RUN pip install -r requirements.txt
CMD ["python", "-B", "application.py"]
