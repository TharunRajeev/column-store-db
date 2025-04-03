## Project Structure

```bash
.
├── Makefile
├── README.md
├── db
│   ├── impl
│   │   ├── core
│   │   │   ├── catalog_manager.c
│   │   │   ├── client_context.c
│   │   │   └── db_manager.c
│   │   ├── network
│   │   │   ├── client.c
│   │   │   └── server.c
│   │   ├── query
│   │   │   ├── handler.c
│   │   │   ├── executor
│   │   │   │   ├── create.c
│   │   │   │   ├── fetch.c
│   │   │   │   ├── insert.c
│   │   │   │   ├── math.c
│   │   │   │   └── select.c
│   │   │   └── parser
│   │   │       └── parse.c
│   │   └── utils
│   │       └── utils.c
│   └── include
│       ├── core
│       │   ├── catalog_manager.h
│       │   ├── client_context.h
│       │   └── db.h
│       ├── query
│       │   ├── handler.h
│       │   ├── executor
│       │   │   ├── operators.h
│       │   │   └── query_exec.h
│       │   └── parser
│       │       └── parse.h
│       └── utils
│           ├── common.h
│           └── utils.h
├── lib
    ├── impl
    │   ├── btree.c
    │   ├── hashmap.c
    │   ├── mempool.c
    │   ├── threadpool.c
    │   └── vector.c
    └── include
        ├── btree.h
        ├── hashmap.h
        ├── mempool.h
        ├── threadpool.h
        └── vector.h
```
