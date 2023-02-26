Nếu chạy lần đầu, gán lại biến môi trường cho airflow init trong file .env:
_AIRFLOW_DB_UPGRADE=True
_AIRFLOW_WWW_USER_CREATE=True

Nếu chạy lần thứ 2, set lại 2 biến trên bằng False

Command:
docker-compose up
# Cài đặt thư viện cho airflow để chạy code
# Tìm container id của airflow scheduler bằng docker ps
docker exec -it container_id_of_scheduler /bin/bash
sudo su - airflow
pip install -r /opt/airflow/dags/requirements.txt 
exit
exit
# Reload lại list dags
docker exec -it --user airflow airflow-scheduler bash -c "airflow dags list"

# Open in localhost:8080
# User and pass là airflow

