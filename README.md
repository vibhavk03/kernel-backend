# Kernel Backend (FastAPI)

This is a simple FastAPI backend with a clean folder structure.

## Structure

```
app/
├── controllers/
│   └── item_controller.py
├── routers/
│   └── item_router.py
├── schemas/
│   └── item.py
└── main.py

requirements.txt
``` 

## Running

Install dependencies:

```bash
pip install -r requirements.txt
```

Start server:

```bash
uvicorn app.main:app --reload
```

### Endpoints

- `GET /` - root welcome message
- `GET /items/` - dummy list of items
- `POST /items/` - create item (JSON body)

