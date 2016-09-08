# redis-copy
redis复制工具

目前仅支持string、set、hash、list 四中类型的复制，并自动识别！

编译时需要第三方的库：

go get github.com/garyburd/redigo/redis

go get gopkg.in/alecthomas/kingpin.v2

命令行使用格式说明。

redis-copy -H 192.168.1.111 -P 6379 -A 123456 -D 4 -h 127.0.0.1 -p 6379 -a 123456 -d 4 -m *




--help

-H, --srcaddr=""      Use -H <源 IP地址>

-P, --srcport="6379"  Use -P <源 端口>

-A, --srcauth=""      Use -A <源 认证密码>

-D, --srcdbs=0        Use -D <源 数据库>


-h, --dstaddr=""      Use -h <目标 IP地址>

-p, --dstport="6379"  Use -p <目标 端口>

-a, --dstauth=""      Use -a <目标 认证密码>

-d, --dstdbs=0        Use -d <目标 数据库>

-m, --mcp=""          Use -m <要复制键值如：test*>

