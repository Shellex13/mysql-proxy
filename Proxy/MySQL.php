<?php

namespace Proxy;

class MySQL {

    const DEFAULT_PORT = 3306;
    const ERROR_CONN = 10001;
    const ERROR_AUTH = 10002;
    const ERROR_QUERY = 10003;
    const ERROR_PREPARE = 10004;

    private $protocol = null;
    public $onResult = null;
    /*
     * 链接池最大连接数
     */
    private $poolSize = 0;
    /*
     * 已经建立的连接数
     */
    private $usedSize = 0;
    /*
     * 空闲链接
     */
    public $idlePool = array();
    /*
     * 排队的请求
     */
    public $taskQueue = array();
    /*
     * 是否正在ping 方式主从相同数据源同时ping
     */
    public $pinging = false;

    /**
     * @var \swoole_table 用于存储连接数汇总信息
     */
    public $table = null;

    /**
     * @var string
     */
    public $datasource = null;


    /*
     * 客户端fd到$db的映射
     */
    private $fd2db = array();

    const RESP_OK = 0;
    const RESP_ERROR = -1;
    const RESP_EOF = -2;

    function __construct($config, $table, callable $onResult) {
        if (empty($config['host'])) {
            throw new \Exception("require mysql host option.");
        }
        if (empty($config['port'])) {
            $config['port'] = self::DEFAULT_PORT;
        }
        $this->protocol = new \MysqlProtocol();
        $this->onResult = $onResult;
        $this->config = $config;
        $this->table = $table;
        $this->poolSize = $config['maxconn'];
        $this->datasource = $config['host'] . ":" . $config['port'] . ":" . $config['database'];
        $this->protocol = new \MysqlProtocol();
    }

    public function onClose($db) {//mysql主动断开了和proxy的链接
        \Logger::log("close with mysql");
        $this->remove($db); //如果此链接在idle里面就剔除
        if ($db->clientFd > 0) {//如果此链接已经分配给了客户端,则向客户端发送错误信息(重启mysql才会发生这种情况，session timeout的时候除非分配连接和gone away同时发生)
            $binaryData = $this->protocol->packErrorData(self::ERROR_CONN, "close with mysql");
            return call_user_func($this->onResult, $binaryData, $db->clientFd);
        }
    }

    public function onReceive(\swoole_client $db, $data = "") {
        if ($db->status == "CONNECT") {
            $binary = $this->protocol->responseAuth($data, $this->config['database'], $this->config['user'], $this->config['password'], $this->config['charset']);
            if (is_array($binary)) {//error??
                $binaryData = $this->protocol->packErrorData(self::ERROR_CONN, $binary['error_msg']);
                //随后mysql会主动断开连接 回调onclose
                \Logger::log("连接mysql 失败 {$binary['error_msg']}");
                call_user_func($this->onResult, $binaryData, $db->clientFd);
                return;
            }
            $db->status = "AUTH";
            $db->send($binary);
            return;
        } else if ($db->status == "AUTH") {
            $ret = $this->protocol->getConnResult($data);
            if ($ret == 1) {
                $db->status = "EST";
                \Logger::log("连接mysql 成功 $ret {$this->datasource}");
                $this->join($db);
                return;
            } else {
                //随后mysql会主动断开连接 回调onclose
                \Logger::log("连接mysql 失败 $ret {$this->datasource}");
                $binaryData = $this->protocol->packErrorData(self::ERROR_AUTH, "auth error when connect");
                call_user_func($this->onResult, $binaryData, $db->clientFd);
            }
        } else {
            $ret = $this->protocol->getResp($data); //todo change name
            switch ($ret['cmd']) {
                case self::RESP_EOF:
                    if (( ++$db->eofCnt) == 2) {//第二次的eof才是[row] eof
                        $db->buffer .= $data;
                        call_user_func($this->onResult, $db->buffer, $db->clientFd);
                        $this->release($db);
                    } else {//pack the [Field] eof data
                        $db->buffer .= $data;
                    }
                    break;
                case self::RESP_OK:
                    //某些sql 'select app as c,count(id) as count,sum(is_used) as used from `msg_captcha_log1`  where add_time >= "1509465600" and add_time <= "1518191999" and ty
//pe="1"  group by app  order by `msg_captcha_log1`.id desc' 会在第二次eof前返回一个ok包 why?
                    if ($db->eofCnt == 1) {
                        $db->buffer .= $data;
                    } else {
                        call_user_func($this->onResult, $data, $db->clientFd);
                        if ($ret['in_tran'] === 0) {
                            $this->release($db);
                        } else {
                            $db->in_tran = 1;
                        }
                    }
                    break;
                case self::RESP_ERROR:
                    call_user_func($this->onResult, $data, $db->clientFd);
                    $this->release($db);
                    break;

                default://result
                    $db->buffer .= $data; //pack result
                    break;
            }
        }
    }

    public function onError($db) {
        if ($db->status === "CONNECT") {
            $this->usedSize--;
            $this->table->decr("table_key", $this->datasource);
        }
        \Logger::log("something error {$db->errCode} db:{$this->datasource}");
        $binaryData = $this->protocol->packErrorData(self::ERROR_QUERY, "something error {$db->errCode}");
        return call_user_func($this->onResult, $binaryData, $db->clientFd);
    }

    protected function connect($fd) {
        $db = new \swoole_client(SWOOLE_SOCK_TCP, SWOOLE_SOCK_ASYNC);
        $db->set([
            'open_length_check' => 1,
            'open_tcp_nodelay' => true,
            'package_length_func' => 'mysql_proxy_get_length'
                ]
        );
        $db->on('close', array($this, 'onClose'));
        $db->on('receive', array($this, 'onReceive'));
        $db->on('error', array($this, 'onError'));
        $db->on("connect", function($cli) {
            \Logger::log("connect to mysql");
        });
        $db->status = "CONNECT";
        $db->clientFd = $fd; //提前设置，为了出错时候可以发送给客户端
        $db->buffer = '';
        $db->eofCnt = 0;
        $db->in_tran = 0;

        //先加
        $this->usedSize++;
        $this->table->incr("table_key", $this->datasource);

        $db->connect($this->config['host'], $this->config['port'], 10);
    }

    public function query($data, $fd) {
        if (isset($this->fd2db[$fd])) {//已经分配了连接
            MysqlProxy::$clients[$fd]['start'] = microtime(true) * 1000;
            $this->fd2db[$fd]->send($data);
            return;
        }
        if (count($this->idlePool) > 0) {
            //从空闲队列中取出可用的资源
            $db = array_shift($this->idlePool);
            $this->fd2db[$fd] = $db;
            $db->clientFd = $fd; //当前连接服务于那个客户端fd
            $db->buffer = '';
            $db->eofCnt = 0;
            MysqlProxy::$clients[$fd]['start'] = microtime(true) * 1000;
            $db->send($data); //发送数据到mysql
            return;
        } else if ($this->usedSize < $this->poolSize) {
            array_push($this->taskQueue, array('fd' => $fd, 'data' => $data));
            $this->connect($fd);
        } else {
            array_push($this->taskQueue, array('fd' => $fd, 'data' => $data));
            \Logger::log("out of pool size ,source:{$this->datasource}  query {$data}");
        }
    }

    /**
     * 加入到连接池中
     * @param $db
     */
    private function join($db) {
        //保存到空闲连接池中
        array_push($this->idlePool, $db);
        $this->doTask();
    }

    protected function doTask() {
        while (count($this->taskQueue) > 0 && count($this->idlePool) > 0) {
            //从空闲队列中取出可用的资源
            $db = array_shift($this->idlePool);
            //从队列取出排队的
            $task = array_shift($this->taskQueue);
            $db->clientFd = $task['fd'];
            $this->fd2db[$task['fd']] = $db;
            $db->buffer = '';
            $db->eofCnt = 0;
            MysqlProxy::$clients[$task['fd']]['start'] = microtime(true) * 1000;
            $db->send($task['data']);
        }
    }

    /**
     * 释放资源
     * @param $db
     */
    public function release($db) {
        unset($this->fd2db[$db->clientFd]);
        $db->clientFd = 0;
        $db->buffer = '';
        $db->eofCnt = 0;
        $db->in_tran = 0;
        array_push($this->idlePool, $db);
        $this->doTask();
    }

    /**
     * 移除资源
     * @param $db
     * @return bool
     */
    function remove($db) {
        foreach ($this->idlePool as $k => $res) {
            if ($res === $db) {
                unset($this->idlePool[$k]);
                $this->usedSize--;
                $this->table->decr("table_key", $this->datasource);
                return true;
            }
        }
        return false;
    }

    /**
     * 移除排队和解除事务
     * @param $fd
     * @return bool
     */
    function removeTask($fd) {
        if (isset($this->fd2db[$fd])) {
            $db = $this->fd2db[$fd];
            if ($db->in_tran) {//在事务里面直接断开了和proxy的链接，相应的proxy也和mysql断开链接重新连
                \Logger::log("client close when in transaction");
                unset($this->fd2db[$fd]);
                $this->usedSize--;
                $this->table->decr("table_key", $this->datasource);
                $db->close();
            }
            unset($this->fd2db[$fd]);
        }
        foreach ($this->taskQueue as $k => $arr) {
            if ($arr['fd'] === $fd) {
                unset($this->taskQueue[$k]);
                return true;
            }
        }
        return false;
    }

}
