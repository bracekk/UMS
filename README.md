# UMS — Unity Manufacturing Solutions

Flask + Jinja + SQLite manufacturing operations app for small production teams.

## What is included in this build

- workstation `cost_per_hour`
- product cost split into materials / jobs / total
- order cost split into materials / jobs / total
- batch cost split into materials / jobs / total
- batch planned workstation load preview before launch
- product groups with filtering in product and order planning flows
- cleaner deployment files for Gunicorn / Render
- safer project packaging without committed secrets or live backups

## Local run

```bash
python -m venv .venv
source .venv/bin/activate   # Windows: .venv\Scripts\activate
pip install -r requirements.txt
cp .env.example .env
python app.py
```

The first startup creates `database.db` automatically.

## Production start

Gunicorn entrypoint:

```bash
gunicorn wsgi:app
```

## Important notes

- Set a real `SECRET_KEY` in `.env`
- Set `RESEND_API_KEY` only if you want password reset email sending
- The packaged project intentionally does not ship with a live `.env`, `.git`, backups, or an old production database
