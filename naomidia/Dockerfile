FROM python:3.11-buster

WORKDIR /home/brenno/git/etl_redes/etl-redes/naomidia/Dockerfile

COPY requirements.txt requirements.txt
RUN pip3 install -r requirements.txt

COPY . .

CMD ["python3", "-m", "naomidia_insta"]