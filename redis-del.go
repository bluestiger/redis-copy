package main

import (
	//"flag"
	"fmt"
	"log"
	"os"
	//"reflect"
	//"strings"
	"strings"
	"time"

	"github.com/garyburd/redigo/redis"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	pool           *redis.Pool
	addr           = kingpin.Flag("addr", "Use -H <IP地址>").Default("").Short('H').String()
	port           = kingpin.Flag("port", "Use -p <端口>").Default("6379").Short('p').String()
	password       = kingpin.Flag("auth", "Use -a <认证密码>").Default("").Short('a').String()
	databases      = kingpin.Flag("dbs", "Use -d <数据库>").Default("0").Short('d').Int()
	multipledelete = kingpin.Flag("mdel", "Use -m <要删除键值如：test*>").Default("").Short('m').String()
)

//redis 连接池
func newPoll(server string, password string, databases int) *redis.Pool {
	return &redis.Pool{
		MaxIdle:     3,
		IdleTimeout: 240 * time.Second,
		Dial: func() (redis.Conn, error) {
			c, err := redis.Dial("tcp", server)
			if err != nil {
				return nil, err
			}
			if _, err := c.Do("AUTH", password); err != nil {
				c.Close()
				return nil, err
			}
			if _, err := c.Do("select", databases); err != nil {
				c.Close()
				return nil, err
			}
			return c, err
		},
		TestOnBorrow: func(c redis.Conn, t time.Time) error {
			if time.Since(t) < time.Minute {
				return nil
			}
			_, err := c.Do("PING")
			return err
		},
	}
}

func main() {

	//redis-del.exe --addr 120.55.187.37 --port 6379 --auth Voodoo123456 --dbs 0 --mdel test*;desk*
	//redis-del.exe -H 120.55.187.37 -p 6379 -a Voodoo123456 -d 0 -m test*;desk*

	//解析参数
	//flag.Parse()
	kingpin.Parse()

	//默认参数至少3个
	if len(os.Args) < 3 {
		fmt.Println("请用查看帮助命令:“--help”")
		return
	}

	//多参数输入时，分割字符串
	//fmt.Println(reflect.TypeOf(*multipledelete))
	multiplekey := strings.Split(*multipledelete, ";")

	//禁止单独使用“*”
	for _, mkey := range multiplekey {
		if mkey == "*" {
			fmt.Println("禁止单独使用“*”")
			os.Exit(1)
		}
	}

	//从连接池中拿取空闲连接，连接到redis。
	server := *addr + ":" + *port
	pool = newPoll(server, *password, *databases)
	conn := pool.Get()

	//fmt.Println(reflect.TypeOf(multiplekey))
	for _, mdel := range multiplekey {
		//fmt.Println(mdel)

		keys, err := redis.Strings(conn.Do("KEYS", mdel))
		if err != nil {
			fmt.Println(err)
		}
		if strings.Join(keys, "") == "" {
			fmt.Println("不存在该键值，键值不能为空！")
		}
		defer pool.Close()

		//fmt.Println(reflect.TypeOf(keys))
		for _, key := range keys {
			//fmt.Println(reflect.TypeOf(key))
			//fmt.Println(key)
			_, err := conn.Do("DEL", key)
			if err != nil {
				log.Println(err)
			}
			log.Println(key, "删除成功")
			defer pool.Close()
		}
	}

}
