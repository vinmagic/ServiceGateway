# ServiceGateway

Server/
├── config/
│   └── config.mjs                 # load + validate config
├── db/
│   ├── mongo.mjs                  # Mongo client/bootstrap
│   ├── collections.mjs            # ensureTsCollection, results collections
│   └── gridfs.mjs                 # GridFS upload/download helpers
├── observability/
│   ├── logger.mjs                 # pino + tee + mongo log writer
│   └── metrics-plugin.mjs         # onResponse hook (writes metrics)
├── streams/
│   ├── manager.mjs                # load + compile rules, watch, findStream
│   └── host.mjs                   # normalizeHost()
├── security/
│   └── apikey-plugin.mjs          # onRequest API key check
├── http/
│   ├── body.mjs                   # text/binary detection, read/limit, redaction
│   ├── results.mjs                # waitForResultIn(), result ingest/upsert
│   ├── routes-health.mjs          # /__health, /__version
│   └── routes-universal.mjs       # the catch-all route as a Fastify plugin
├── server.mjs                     # creates Fastify, registers plugins
└── index.mjs                      # starts server, graceful shutdown