# pgmigrate-lab task

PostgreSQL schema + data-access layer, managed with [dbmate](https://github.com/amacneil/dbmate).

## Layout

```
db/
  migrations/      # dbmate migration files (YYYYMMDDHHMMSS_<name>.sql)
  schema.sql       # current checked-in schema dump
  data_models/     # SQLModel classes (auto-generated from schema.sql)
  schemas/         # Pydantic CRUD schemas (auto-generated)
  dal/             # data-access layer
  scripts/         # code generators
  session/         # async session factory
  utils/           # small DB helpers
  tests/           # DAL unit tests (run with pytest, sqlite-in-memory)
tests/             # hidden schema/behavior assertions (added during eval)
```

## Running things locally

```
dbmate up              # apply pending migrations
pytest db/tests        # run DAL tests
pytest tests           # run hidden assertions (if present)
```

`DATABASE_URL` is pre-set in the shell environment.
