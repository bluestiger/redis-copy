package main

import (
	//"flag"
	//"reflect"
	//"strconv"
	"fmt"
	"log"
	"math"
	"os"
	"runtime"
	"strings"
	"sync"
	"time"

	//"github.com/garyburd/redigo/redis"
	"github.com/gomodule/redigo/redis"
	"gopkg.in/alecthomas/kingpin.v2"
)

var (
	sourceAddr = kingpin.Flag("srcaddr", "Use -H <源 IP地址>").Default("").Short('H').String()
	sourcePort = kingpin.Flag("srcport", "Use -P <源 端口>").Default("6379").Short('P').String()
	sourceAuth = kingpin.Flag("srcauth", "Use -A <源 认证密码>").Default("").Short('A').String()
	sourceDbs  = kingpin.Flag("srcdbs", "Use -D <源 数据库>").Default("0").Short('D').Int()

	targetAddr = kingpin.Flag("dstaddr", "Use -h <目标 IP地址>").Default("").Short('h').String()
	targetPort = kingpin.Flag("dstport", "Use -p <目标 端口>").Default("6379").Short('p').String()
	targetAuth = kingpin.Flag("dstauth", "Use -a <目标 认证密码>").Default("").Short('a').String()
	targetDbs  = kingpin.Flag("dstdbs", "Use -d <目标 数据库>").Default("0").Short('d').Int()

	sourcePool, targetPool *redis.Pool
	contents               string
	multipleCopy           = kingpin.Flag("mcopy", "Use -m <要复制键值如：test_one*>").Default("").Short('m').String()
	outputlog              = kingpin.Flag("output", "Use -out <输出日志文件>").Default("./output.log").Short('o').String()
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

	//解析参数
	kingpin.Parse()

	//规定参数数量不不少于7个
	if len(os.Args) < 7 {
		fmt.Println("请用查看帮助命令:“--help”")
		return
	}

	//分割字符串
	multipleKeys := strings.Split(*multipleCopy, ";")
	//fmt.Println(reflect.TypeOf(multipleKeys), multipleKeys)

	//禁止“-m”后面带空参数
	for _, mKeys := range multipleKeys {
		if mKeys == "" {
			fmt.Println("禁止传空值！")
			os.Exit(1)
		}
	}

	//从连接池中拿取空闲连接，连接到redis源。
	sourceServer := *sourceAddr + ":" + *sourcePort
	sourcePool = newPoll(sourceServer, *sourceAuth, *sourceDbs)
	sourceConn := sourcePool.Get()
	defer sourcePool.Close()

	//并发参数设定
	ncpu := runtime.NumCPU()
	runtime.GOMAXPROCS(ncpu)
	wg := sync.WaitGroup{}

	//记录开始时间。
	start := time.Now()

	for _, mKeys := range multipleKeys {
		//fmt.Println(reflect.TypeOf(mKeys), mKeys)
		keys, err := redis.Strings(sourceConn.Do("KEYS", mKeys))
		if err != nil {
			fmt.Println(err)
		}
		defer sourcePool.Close()

		//当队列的数量小于CPU核数时强制设置CPU核数为并发数。
		lenKeys := len(keys)
		if lenKeys < ncpu {
			ncpu = lenKeys
		}
		//根据CPU核数进行分组，向上取整 ceil (天花板)
		j := int(math.Ceil(float64(lenKeys) / float64(ncpu)))

		//设置cpu数为队列数，并发执行任务。
		wg.Add(ncpu)
		for i := 0; i < ncpu; i++ {
			go copyKeys(&wg, i, j, lenKeys, keys)
		}
		wg.Wait()
		fmt.Printf("\n")
		fmt.Println("复制数量:", lenKeys)
	}

	//记录结束时间。
	end := time.Now()

	//输出执行时间，单位为毫秒。
	result := end.Sub(start).Nanoseconds() / 1000000

	//输出执行结果。
	fmt.Println("总共用时：", result, "ms")

}

//复制主体，自动判断key值类型；传入并发参数，key值总数量，keys字符串组。
func copyKeys(wg *sync.WaitGroup, i int, j int, lenKeys int, keys []string) {

	//从连接池中拿取空闲连接，连接到redis源。
	sourceServer := *sourceAddr + ":" + *sourcePort
	sourcePool = newPoll(sourceServer, *sourceAuth, *sourceDbs)
	sourceConn := sourcePool.Get()
	defer sourcePool.Close()

	//从连接池中拿取空闲连接，连接到redis目标。
	targetServer := *targetAddr + ":" + *targetPort
	targetPool = newPoll(targetServer, *targetAuth, *targetDbs)
	targetConn := targetPool.Get()
	defer targetPool.Close()

	//fmt.Println(reflect.TypeOf(keys), keys)
	for n := i * j; n < (i+1)*j; n++ {
		if n >= lenKeys {
			break
		}
		key := keys[n]
		//fmt.Println(reflect.TypeOf(key), key)
		keyType, err := sourceConn.Do("TYPE", key)
		if err != nil {
			log.Println(err)
		}
		defer sourcePool.Close()

		//复制前先删除目标对应的key值。
		_, _ = targetConn.Do("DEL", key)
		defer targetPool.Close()

		//fmt.Println(reflect.TypeOf(keyType), keyType)
		switch keyType {

		case "string":
			keyStringGet, err := redis.String(sourceConn.Do("GET", key))
			if err != nil {
				log.Println(err)
			}
			defer sourcePool.Close()
			//fmt.Println(reflect.TypeOf(keyStringGet), keyStringGet)
			keyStringYes, err := redis.String(targetConn.Do("SET", key, keyStringGet))
			if err != nil {
				log.Println(err)
			}
			//fmt.Println(reflect.TypeOf(keyStringYes), keyStringYes)
			contents := "SET" + " " + key + " " + keyStringGet
			if keyStringYes == "OK" {
				keyStringYesBool := true
				outPut(keyStringYesBool, contents)
			} else {
				keyStringYesBool := false
				outPut(keyStringYesBool, contents)
			}
			defer targetPool.Close()

		case "set":
			keySetSort, err := redis.Strings(sourceConn.Do("SORT", key, "alpha"))
			if err != nil {
				log.Println(err)
			}
			defer sourcePool.Close()
			//fmt.Println(reflect.TypeOf(keySetSort), keySetSort)
			for _, keySetSadd := range keySetSort {
				keySetYes, err := redis.Bool(targetConn.Do("SADD", key, keySetSadd))
				if err != nil {
					log.Println(err)
				}
				contents := "SADD" + " " + key + " " + keySetSadd
				outPut(keySetYes, contents)
				defer targetPool.Close()
			}

		case "hash":
			keyHashHkeys, err := redis.Strings(sourceConn.Do("HKEYS", key))
			if err != nil {
				log.Println(err)
			}
			defer sourcePool.Close()
			//fmt.Println(reflect.TypeOf(keyHashHkeys), keyHashHkeys)
			for _, keyHashHkey := range keyHashHkeys {
				keyHashHget, err := redis.String(sourceConn.Do("Hget", key, keyHashHkey))
				if err != nil {
					log.Println(err)
				}
				defer sourcePool.Close()
				//fmt.Println(reflect.TypeOf(keyHashHget), keyHashHget)
				keyHashYes, err := redis.Bool(targetConn.Do("HSET", key, keyHashHkey, keyHashHget))
				if err != nil {
					log.Println(err)
				}
				contents := "HSET" + " " + key + " " + keyHashHkey + " " + keyHashHget
				outPut(keyHashYes, contents)
				defer targetPool.Close()
			}

		case "list":
			keyListLrange, err := redis.Strings(sourceConn.Do("LRANGE", key, "0", "-1"))
			if err != nil {
				log.Println(err)
			}
			defer sourcePool.Close()
			//fmt.Println(reflect.TypeOf(keyListLrange), keyListLrange)
			for _, keyListRpush := range keyListLrange {
				keyListYes, err := redis.Bool(targetConn.Do("RPUSH", key, keyListRpush))
				if err != nil {
					log.Println(err)
				}
				//fmt.Println(reflect.TypeOf(keyListRpush), keyListRpush)
				contents := "RPUSH" + " " + key + " " + keyListRpush
				outPut(keyListYes, contents)
				defer targetPool.Close()
			}

		default:
			fmt.Println("目前暂不支持", keyType, "该类型的复制")

		}
		defer sourcePool.Close()
		defer targetPool.Close()
	}
	wg.Done()
}

//输出Info信息。
func outPut(keyTypeYes bool, contents string) {
	if keyTypeYes == true {
		contentsInfo := time.Now().Format("2006-01-02 15:04:05") + " " + contents + " " + "复制完成！"
		//outPutLog(contentsInfo)
		fmt.Println(contentsInfo)
		return
	} else {
		contentsError := time.Now().Format("2006-01-02 15:04:05") + " " + contents + " " + "复制失败！"
		//outPutLog(contentsError)
		fmt.Println(contentsError)
		return
	}
}

//写入到Log文件。
func outPutLog(contents string) {
	fileName := *outputlog
	dstFile, err := os.OpenFile(fileName, os.O_CREATE|os.O_APPEND|os.O_WRONLY, 0660)
	if err != nil {
		log.Fatalln(err)
	}
	defer dstFile.Close()
	save := contents
	dstFile.WriteString(save + "\n")
}
