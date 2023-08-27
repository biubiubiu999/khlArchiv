# 简介

此工具是将Tdengine数据库中的数据根据配置文件转存到文件中

## 使用说明

```shell
npm i

node index.js config
```
可以开多个进程，每个进程不同的config文件
## 配置介绍
```
{
    "startTime": "2023-07-03",
    "endTime": "2023-07-05",
    "dbInfo": {
        "host": "172.30.60.105",
        "port": 6061,
        "user": "root",
        "password": "kingview",
        "auth": "c48947bc7dbd55e974fc6dc1bbb9ade6",
        "database": "history"
    },
    "intervalData": false,
    "intervalTable": [
        5,
        10,
        20,
        30,
        60,
        90,
        120
    ],
    "createTableFlag":true
}
```