FROM nexus-repo.dmp.vimpelcom.ru/dmp_core/python-base:v3.10-2

COPY requirements.txt requirements.txt

RUN pip install -r requirements.txt

COPY tags-to-tables.py tags-to-tables.py