# Golang Driver

Databend Golang driver access databend distributions or databend cloud through [REST API]
{https://databend.rs/doc/integrations/api/rest}.

## Connecting

The following DSN formats are supported
```text
databend://root:root@databend:8000/default?sslmode=disable&debug=1
databend://root:root@databend:8000/default?presigned_url_disabled=true&wait_time_secs=30
```

## Parameter References

| Parameter              | Description                                                                                                                | Default | example                                                                |
|------------------------|----------------------------------------------------------------------------------------------------------------------------|---------|------------------------------------------------------------------------|
| user                   | Databend user name                                                                                                         | none    | databend://{user}:root@databend:8000/                                  |
| password               | Databend user password                                                                                                     | none    | databend://root:{password}@databend:8000/                              |
| sslmode                | Enable SSL                                                                                                                 | disable | databend://root:root@databend:8000/default?sslmode=enable              |
| presigned_url_disabled | whether use presigned url to upload data, generally if you use local disk as your storage layer, it should be set as true  | false   | databend://root:root@databend:8000/default?presigned_url_disabled=true |
| wait_time_secs         | Restful query api blocking time, if the query is not finished, the api will block for wait_time_secs seconds               | 10      | databend://root:root@databend:8000/hello_databend?wait_time_secs=10    |
| max_rows_in_buffer     | the maximum rows in server session buffer                                                                                  | 5000000 | databend://root:root@databend:8000/default?max_rows_in_buffer=50000    |
| max_rows_per_page      | the maximum rows per page in response data body                                                                            | 100000  | databend://root:root@databend:8000/default?max_rows_per_page=100000    |

