# mini redis

This is an experimental project for me to learn database basic knowledge. It is not production ready project.Please DO NOT USE IT in production environment.

it implement part of redis protocol, so you can use redis-cli to visit the db. See more in usage section.

It's welcome to contribute if you want to enrich yourself.



## Usage

```
➜  src ./redis-cli set key value
OK
➜  src ./redis-cli set key value2
OK
➜  src ./redis-cli get key
value2
➜  src ./redis-cli del key
(integer) 1
➜  src ./redis-cli del key
(integer) 0
➜  src ./redis-cli get key
(nil)
```



