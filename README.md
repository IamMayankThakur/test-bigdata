# test-bigdata
Test framework for assignments in the Big Data course (UE17CS313).

This project was used to evaluate hadoop, spark and spark streaming assignments for over 300 students at [PES University, Bangalore, India](https://pes.edu).

## Setup

* Clone repo
* Create venv `python3 -m venv /path/to/new/virtual/environment`
* Activate it using `source /path/to/venv/bin/activate`
* Install required modules using `pip install -r requirements.txt`
* Install redis using `sudo apt-get install redis-server`

## Running the project for development

* `source /path/to/venv/bin/activate`
* `cd adminmgr`
* `python manage.py makemigrations`
* `python manage.py migrate`
* `python manage.py runserver`
* Ensure redis is running. `redis-cli ping`
* In another terminal, run a celery worker using `celery -A adminmgr worker -l info --concurrency=1`
* Run your local hadoop/spark workers.
* Create superuser using `python manage.py createsuperuser`
* See the customized admin interface at `localhost:8000/admin`
* In `settings.py` ensure that `STATIC_URL` is set to `staticfiles` or any other existing directory to load the static files.
* Set a valid password and email id, or load it from env variables in `adminmgr/notifymgr/mail.py` to send automated emails.

## Running the project in production

* Install `redis`, `nginx`, `supervisord`, `postgresql`.
* `nginx` is used as a web server.
* `nginx` is running on port 80.
* `nginx` will `proxy_pass` on port 8000
* `gunicorn` is a production grade app server.
* `gunicorn` will be running on port 8000.
* `nginx` will also be serving all the static files.
* Can use the existing `nginx.conf` as a template
* Setup `supervisord` (or `systemd`) for running celery and `gunicorn` as a daemon.
* Recommended to dockerize the application.


### Feel free to Create an issue or a PR in case of a bug.
### Contact the developers if you need to use this for evaluation of assignments.

### Developers

* [Nishant Ravi Shankar ](https://github.com/mellon-collie)
* [Mayank Thakur (Me)](https://github.com/IamMayankThakur)

* ### Thanks to Dr K V Subramaniam for the guidance.
