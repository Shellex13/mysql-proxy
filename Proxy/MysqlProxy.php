<?php

namespace Proxy;

//require '/data/www/public/sdk/autoload.php';

class MysqlProxy {

    /**
     * @var \swoole_server
     */
    private $serv = null;

    /*
     * 上报sql数据到集中redis里面
     */
    private $redis = null;
    private $redisHost = NULL;
    private $redisPort = NULL;
    private $redisAuth = NULL;
    private $RECORD_QUERY = false;
    //阈值
    private $slow_limit = 100;
    private $big_limit = 204800;
    /*
     * PROXY的ip 用于proxy集群上报加到key里面
     */
    private $localip = null;

    /**
     * @var \swoole_table task 和worker之间共享数据用
     */
    private $table = null;

    /*
     * 刚连上
     */

    const CONNECT_START = 0;
    /*
     * 发送了auth挑战数据
     */
    const CONNECT_SEND_AUTH = 1;
    /*
     * 发送ok后握手成功
     */
    const CONNECT_SEND_ESTA = 2;
    const COM_QUERY = 3;
    const COM_INIT_DB = 2;
    const COM_QUIT = 1;
    const COM_PREPARE = 22;

    private $targetConfig = array();

    /**
     * @var \MysqlProtocol
     */
    private $protocol;

    /**
     * @var MySQL[]
     */
    private $pool;

    /**
     * @var array
     */
    private $clients = [];

    private function createTable() {
        $this->table = new \swoole_table(1024);
        $arr = [];
        foreach ($this->targetConfig as $dbname => $config) {
            $conf = $config['master'];
            $dataSource = $conf['host'] . ":" . $conf['port'] . ":" . $dbname;
            $this->table->column($dataSource, \swoole_table::TYPE_INT, 4);
            $this->table->column("request_num_" . $dbname, \swoole_table::TYPE_INT, 4);
            $arr[$dataSource] = 0;
            if (isset($config['slave'])) {
                foreach ($config['slave'] as $sconfig) {
                    $dataSource = $sconfig['host'] . ":" . $sconfig['port'] . ":" . $dbname;
                    $this->table->column($dataSource, \swoole_table::TYPE_INT, 4);
                    $arr[$dataSource] = 0;
                }
            }
        }
        $this->table->column("client_count", \swoole_table::TYPE_INT, 4);
        $this->table->column("request_num", \swoole_table::TYPE_INT, 4);
        $this->table->create();
    }

    public function init() {
        $this->getConfigConst();
        $this->getConfigNode();
        $this->serv = new \swoole_server('0.0.0.0', PORT, SWOOLE_BASE, SWOOLE_SOCK_TCP);
        $this->serv->set([
            'worker_num' => WORKER_NUM,
            'task_worker_num' => TASK_WORKER_NUM,
            'open_length_check' => 1,
            'open_tcp_nodelay' => true,
            'log_file' => SWOOLE_LOG,
            'daemonize' => DAEMON,
            'package_length_func' => 'mysql_proxy_get_length'
                ]
        );
        $this->createTable();
        $this->protocol = new \MysqlProtocol();
        $this->pool = array(); //mysql的池子
        $this->clients = array(); //连到proxy的客户端
        $this->serv->on('receive', array($this, 'OnReceive'));
        $this->serv->on('connect', array($this, 'OnConnect'));
        $this->serv->on('close', array($this, 'OnClose'));
        $this->serv->on('workerStart', array($this, 'OnWorkerStart'));
        $this->serv->on('task', array($this, 'OnTask'));
//        $this->serv->on('start', array($this, 'OnStart'));
        $this->serv->on('finish', function () {
            
        });
    }

    private function getConfigConst() {
        $array = \Yosymfony\Toml\Toml::Parse(__DIR__ . '/../config.toml');
        $common = $array['common'];

        define("REDIS_SLOW", $common['redis_slow']); //todo remove this config
        define("REDIS_BIG", $common['redis_big']); //todo remove this config
        define("REDIS_WRONG_SQL", "sqlwrong");

        define("MYSQL_CONN_REDIS_KEY", $common['mysql_conn_redis_key']);

        define("WORKER_NUM", $common['worker_num']);
        define("TASK_WORKER_NUM", $common['task_worker_num']);
        define("SWOOLE_LOG", $common['swoole_log']);
        define("DAEMON", $common['daemon']);
        define("PORT", $common['port']);
    }

    private function getConfigNode() {
//        $env = get_cfg_var('env.name') ? get_cfg_var('env.name') : 'product';
//        $jsonConfig = \CloudConfig::get("platform/proxy_shequ", "test");
//        $config = json_decode($jsonConfig, true);
        $array = \Yosymfony\Toml\Toml::Parse(__DIR__ . '/../config.toml');

        foreach ($array as $key => $value) {
            if ($key == "common") {

                $ex = explode(":", $value['redis_host']);
                $this->redisHost = $ex[0];
                $this->redisPort = $ex[1];

                if (isset($value['redis_auth'])) {
                    $this->redisAuth = $value['redis_auth'];
                }

                if (isset($value['slow_limit'])) {
                    $this->slow_limit = (int) $value['slow_limit'];
                }

                if (isset($value['big_limit'])) {
                    $this->big_limit = (int) $value['big_limit'];
                }

                $this->RECORD_QUERY = $value['record_query'];
            } else {//nodes
//                $node = $key;
                foreach ($value['db'] as $db) {
                    $this->getEntry($db, $value);
                }
            }
        }
        if (isset($array['common']['include_path'])) {
            $this->getConfigChildNode($array['common']['include_path']);
        }
    }

    private function getConfigChildNode($path) {
        $iterator = new \FilesystemIterator($path);
        foreach ($iterator as $fileinfo) {
            if ($fileinfo->isFile()) {
                if ($fileinfo->getExtension() === "toml") {
                    $node = \Yosymfony\Toml\Toml::Parse($fileinfo->getPathname());
                    foreach ($node as $nodeName => $value) {
                        foreach ($value['db'] as $db) {
                            $this->getEntry($db, $value);
                        }
                    }
                }
            }
        }
    }

    /*
     * 获取每个db维度的配置
     */

    private function getEntry($db, $value) {
        $dbEx = explode(";", $db);
        // parse `db = ["name=dummy_db1;realname=db1;charset=gbk","name=db2;max_conn=100","name=db3"] `
        $dbArr = $ret = [];
        foreach ($dbEx as $v) {
            $exDb = explode("=", $v);
            switch ($exDb[0]) {
                case "name":
                    $dbArr['database'] = $exDb[1];
                    break;
                case "charset":
                    $dbArr['charset'] = $exDb[1];
                    break;
                case "max_conn":
                    $dbArr['maxconn'] = $exDb[1];
                    break;
                case "realname"://传给db的真实名字
                    $dbArr['real_database'] = $exDb[1];
                    break;
                default:
                    throw new \Exception("the config format error $v");
            }
        }

        //charset  max_conn如果没设置走默认的
        if (!isset($dbArr['charset'])) {
            $dbArr['charset'] = $value['default_charset'];
        }
        if (!isset($dbArr['maxconn'])) {
            $dbArr['maxconn'] = $value['default_max_conn'];
        }

        //check all
        if (!isset($dbArr['database'])) {
            throw new \Exception("the db name can not be null");
        }

        //没设置 name=realname
        if (!isset($dbArr['real_database'])) {
            $dbArr['real_database'] = $dbArr['database'];
        }

        if (!isset($dbArr['maxconn'])) {
            throw new \Exception("the maxconn can not be null");
        } else {
            //计算每个worker分多少连接
            $realMax = (int) ceil($dbArr['maxconn'] / WORKER_NUM);
        }
        if (!isset($dbArr['charset'])) {
            throw new \Exception("the charset can not be null");
        }
        if (!isset($value['username'])) {
            throw new \Exception("the username can not be null");
        }

        //init master
        if (!isset($value['master_host'])) {
            throw new \Exception("the master_host can not be null");
        } else {
            $hostEx = explode(":", $value['master_host']);
            $ret['master'] = array(
                'host' => $hostEx[0],
                'port' => $hostEx[1],
                'user' => $value['username'],
                'password' => $this->getPassword($value),
                'database' => $dbArr['real_database'],
                'charset' => $dbArr['charset'],
                'maxconn' => $realMax
            );
            //init allow ip
            if (isset($value['client_ip'])) {
                $ret['allow_ip'] = $value['client_ip'];
            }
        }
        //init slave
        if (isset($value['slave_host'])) {
            $ret['slave_weight_array'] = [];
            $index = 0;
            foreach ($value['slave_host'] as $slave) {
                $sEX = explode(";", $slave);
                $sHost = $sEX[0];
                $hostEx = explode(":", $sHost);
                $ret['slave'][] = array(
                    'host' => $hostEx[0],
                    'port' => $hostEx[1],
                    'user' => $value['username'],
                    'password' => $this->getPassword($value),
                    'database' => $dbArr['real_database'],
                    'charset' => $dbArr['charset'],
                    'maxconn' => $realMax
                );
                //init slave_weight_array
                //weight=1
                $weightEx = explode("=", $sEX[1]);
                $weight = (int) $weightEx[1];
                $ret['slave_weight_array'] = array_pad($ret['slave_weight_array'], count($ret['slave_weight_array']) + $weight, $index);
                $index++;
            }
        }

        $this->targetConfig[$dbArr['database']] = $ret;
    }

    private function getPassword($value) {
        if (isset($value['encrypt_password'])) {
            return base64_decode($value['encrypt_password']);
        }
        if (!isset($value['password'])) {
            throw new \Exception("invalid password");
        } else {
            return $value['password'];
        }
    }

    public function start() {
        $this->serv->start();
    }

    private function checkClientIp($dbName, $fd) {
        if (isset($this->targetConfig[$dbName]['allow_ip'])) {
            $ipArr = $this->targetConfig[$dbName]['allow_ip'];
            $clientIp = $this->clients[$fd]['client_ip'];
            foreach ($ipArr as $pattern) {
                if ($this->checkTwoIp($clientIp, $pattern)) {
                    return true;
                }
            }
            //not find
            return false;
        }
        //not set return true
        return true;
    }

    private function checkTwoIp($ip, $pattern) {
        $ipArr = explode(".", $ip);
        $patternArr = explode(".", $pattern);
        for ($i = 0; $i < 4; $i++) {
            if ($patternArr[$i] == "%") {
                continue;
            }
            if ($ipArr[$i] != $patternArr[$i]) {
                return false;
            }
        }
        return true;
    }

    public function OnReceive(\swoole_server $serv, $fd, $from_id, $data) {
        $this->table->incr("table_key", "request_num");
        if ($this->clients[$fd]['status'] == self::CONNECT_SEND_AUTH) {
            $dbName = $this->protocol->getDbName($data);
            $this->table->incr("table_key", "request_num_" . $dbName);
            if (!isset($this->targetConfig[$dbName])) {
                \Logger::log("db $dbName can not find");
                $binaryData = $this->protocol->packErrorData(10000, "db '$dbName' can not find in mysql proxy config");
                return $this->serv->send($fd, $binaryData);
            }

            if (!$this->checkClientIp($dbName, $fd)) {
                \Logger::log("the client is forbidden");
                $binaryData = $this->protocol->packErrorData(10000, "your ip cannot access to the db ($dbName)");
                return $this->serv->send($fd, $binaryData);
            }

            $this->protocol->sendConnectOk($serv, $fd);
            $this->clients[$fd]['status'] = self::CONNECT_SEND_ESTA;
            $this->clients[$fd]['dbName'] = $dbName;
            //   $this->clients[$fd]['clientsAuthData'] = $data;
            return;
        }
        if ($this->clients[$fd]['status'] == self::CONNECT_SEND_ESTA) {
            $dbName = $this->clients[$fd]['dbName'];
            $this->table->incr("table_key", "request_num_" . $dbName);
            $ret = $this->protocol->getSql($data);
            $cmd = $ret['cmd'];
            $sql = $ret['sql'];

            if ($cmd !== self::COM_QUERY) {
                if ($cmd === self::COM_PREPARE) {
                    $binary = $this->protocol->packErrorData(MySQL::ERROR_PREPARE, "proxy do not support remote prepare , (PDO example:set PDO::ATTR_EMULATE_PREPARES=true)");
                    return $this->serv->send($fd, $binary);
                }
                if ($cmd === self::COM_QUIT) {//直接关闭和client链接
                    $serv->close($fd, true);
                    return;
                }
                $binary = $this->protocol->packOkData(0, 0);
                $this->serv->send($fd, $binary);
                return;
            }

            $pre = substr($sql, 0, 10);
            if (stristr($pre, "SET ")) {
                $binary = $this->protocol->packOkData(0, 0);
                $this->serv->send($fd, $binary);
                if (!stristr($pre, "SET NAMES") && !stristr($pre, "SET autocommit")) {//不支持的sql 打日志 抓出可能的动态改变session的错误sql
                    $this->logWrongSql($sql, $fd, $dbName);
                }
                return;
            } elseif (stristr($pre, "select")) {
                if (isset($this->targetConfig[$dbName]['slave'])) {
                    shuffle($this->targetConfig[$dbName]['slave_weight_array']);
                    $index = $this->targetConfig[$dbName]['slave_weight_array'][0];
                    $config = $this->targetConfig[$dbName]['slave'][$index];
                } else {//未配置从 直接走主
                    $config = $this->targetConfig[$dbName]['master'];
                }
            } else {//其他走主库 包含/*master*/
                $config = $this->targetConfig[$dbName]['master'];
            }

            $dataSource = $config['host'] . ":" . $config['port'] . ":" . $dbName;
            if (!isset($this->pool[$dataSource])) {
                $pool = new MySQL($config, $this->table, array($this, 'OnResult'));
                $this->pool[$dataSource] = $pool;
            }
            $this->clients[$fd]['start'] = microtime(true) * 1000;
            $this->clients[$fd]['sql'] = $sql;
            $this->clients[$fd]['datasource'] = $dataSource;
            $this->pool[$dataSource]->query($data, $fd);
        }
    }

    private function logWrongSql($sql, $fd, $dbName) {
        $client_ip = $this->clients[$fd]['client_ip'];
        \Logger::log("the wrong sql '{$sql}' ip '{$client_ip}' db '{$dbName}'");
//        $this->serv->task($logData);
    }

    public function OnResult($binaryData, $fd) {
        if ($fd == 0) {//ping的返回结果 todo 从库自动恢复 目前只维护连接
            echo "got\n";
        }
        if (isset($this->clients[$fd])) {//有可能已经关闭了
            if (!$this->serv->send($fd, $binaryData)) {
                $binary = $this->protocol->packErrorData(MySQL::ERROR_QUERY, "send to client failed,data size " . strlen($binaryData));
                $this->serv->send($fd, $binary);
            }
            if ($this->RECORD_QUERY) {
                $end = microtime(true) * 1000;

//                //todo remove
//                $random = rand(0, 20);
//                if ($random == 9) {
//                    $use = $end - $this->clients[$fd]['start'];
//                    \Logger::log("log every time '{$sql}'");
//                }

                $logData = array(
                    'start' => $this->clients[$fd]['start'],
                    'size' => strlen($binaryData),
                    'time' => $end - $this->clients[$fd]['start'],
                    'sql' => $this->clients[$fd]['sql'],
                    'datasource' => $this->clients[$fd]['datasource'],
                    'client_ip' => $this->clients[$fd]['client_ip'],
                );
                if ($logData['time'] > $this->slow_limit
                        or $logData['size'] > $this->big_limit) {
                    $this->serv->task($logData);
                }
            }
        }
    }

    public function OnConnect(\swoole_server $serv, $fd) {
//        \Logger::log("client connect $fd");
        $this->clients[$fd]['status'] = self::CONNECT_START;
        $this->protocol->sendConnectAuth($serv, $fd);
        $this->clients[$fd]['status'] = self::CONNECT_SEND_AUTH;
        $info = $serv->getClientInfo($fd);
        if ($info) {
            $this->clients[$fd]['client_ip'] = $info['remote_ip'];
        } else {
            $this->clients[$fd]['client_ip'] = 0;
        }
        $this->table->incr("table_key", "client_count");
    }

    public function OnClose(\swoole_server $serv, $fd) {
//        \Logger::log("client close $fd");
        //todo del from client
        $this->table->decr("table_key", "client_count");
        //remove from task queue,if possible
        if (isset($this->clients[$fd]['datasource'])) {
            $this->pool[$this->clients[$fd]['datasource']]->removeTask($fd);
        }
        unset($this->clients[$fd]);
    }

//    public function OnStart($serv)
//    {
//        
//    }

    public function OnWorkerStart(\swoole_server $serv, $worker_id) {
        $this->getConfigNode();
        if ($worker_id >= $serv->setting['worker_num']) {
            $serv->tick(3000, array($this, "OnTaskTimer"));
//            $serv->tick(5000, array($this, "OnPing"));
            swoole_set_process_name("mysql proxy task");
            $result = swoole_get_local_ip();
            $first_ip = array_pop($result);
            $this->localip = $first_ip;
        } else {
            swoole_set_process_name("mysql proxy worker");
//            $serv->tick(3000, array($this, "OnWorkerTimer")); //自动剔除/恢复 故障从库 && 维持连接
        }
    }

    private function sendPing($config) {
        $dataSource = $config['host'] . ":" . $config['port'] . ":" . $config['database'];
        if (isset($this->pool[$dataSource])) {
            $data = $this->protocol->packPingData();
            $this->pool[$dataSource]->pinging = true;
            $this->pool[$dataSource]->query($data, 0); //当前服务的客户端fd为0  证明是proxy主动发出的ping
        }
    }

    public function OnWorkerTimer($serv) {
        foreach ($this->targetConfig as $configEntry) {
            if (isset($configEntry['master'])) {
                $config = $configEntry['master'];
                $this->sendPing($config);
            }
            if (isset($configEntry['slave'])) {
                foreach ($configEntry['slave'] as $config) {
                    $this->sendPing($config);
                }
            }
        }
    }

//____________________________________________________task worker__________________________________________________
    /**
     * 连接redis 增加redis密码认证
     */
    private function connectRedis() {
        if (empty($this->redis)) {
            $client = new \redis;
            if ($client->pconnect($this->redisHost, $this->redisPort)) {
                //判断是否设置密码
                if (!empty($this->redisAuth)) {
                    $client->auth($this->redisAuth);
                }
                $this->redis = $client;
            } else {
                return false;
            }
        }
        return true;
    }

    //task callback 上报连接数 && 清理redis
    public function OnTaskTimer($serv) {
        $count = $this->table->get("table_key");
        if (!$this->connectRedis()) {
            return;
        }
        /*
         * count layout
         *                                                hash
         * 
         *  datasource1      datasource2    datasource3   client_count(客户端链接)
         *       ↓                           ↓                     ↓                    ↓
         *       1                           1                     0                   10
         * 
         */
        $ser = \swoole_serialize::pack($count);
        $this->redis->hSet(MYSQL_CONN_REDIS_KEY, $this->localip, $ser);
        $this->redis->expire(MYSQL_CONN_REDIS_KEY, 60);

        $date = date("Y-m-d");
        $total = $this->redis->zCard(REDIS_SLOW . $date);
        if ($total > 100) {
            $this->redis->zRemRangeByRank(REDIS_SLOW . $date, 0, -100); //删除排名100后的所有成员
            $this->redis->zRemRangeByRank(REDIS_BIG . $date, 0, -100);
        }

        $request_num = (int) $this->table->get("table_key", "request_num");
        $this->table->set("table_key", array("request_num" => 0));
        $this->redis->set("proxy_qps", $request_num / 3); //总的qps

        foreach ($this->targetConfig as $dbname => $config) {
            $request_num = (int) $this->table->get("table_key", "request_num_" . $dbname);
            $this->table->set("table_key", array("request_num_" . $dbname => 0));
            $this->redis->set("proxy_qps_" . $dbname, $request_num / 3); //总的qps
        }
    }

    public function OnTask($serv, $task_id, $from_id, $data) {
        if (!$this->connectRedis()) {
            return;
        }
        $date = date("Y-m-d");
        $expireFlag = false;
        if (!$this->redis->exists(REDIS_SLOW . $date)) {
            $expireFlag = true;
        }
        $ser = \swoole_serialize::pack($data);
        if ($data['size'] > $this->big_limit) {
            $this->redis->zadd(REDIS_BIG . $date, $data['size'], $ser);
        }

        if ($data['time'] > $this->slow_limit) {
            $this->redis->zadd(REDIS_SLOW . $date, $data['time'], $ser);
        }


        if ($expireFlag) {
            $this->redis->expireAt(REDIS_BIG . $date, strtotime(date("Y-m-d 23:59:59"))); //凌晨过期
            $this->redis->expireAt(REDIS_SLOW . $date, strtotime(date("Y-m-d 23:59:59"))); //凌晨过期
            // $this->redis->expireAt('sqllist' . $date, strtotime(date("Y-m-d 23:59:59")) - time()); //凌晨过期
        }
    }

}
