<?php

namespace src\drive;


class PDO
{
    private static $instance;

    private static $host;
    private static $port;
    private static $database;
    private static $charset;
    private static $user;
    private static $password;

    public static function init($host, $port, $database, $charset, $user, $password)
    {
        self::$host = $host;
        self::$port = $port;
        self::$database = $database;
        self::$charset = $charset;
        self::$user = $user;
        self::$password = $password;
    }

    public static function getInstance(): \PDO
    {
        if (self::$instance) {
            return self::$instance;
        } else {
            $dsn = 'mysql:host=' . self::$host . ';port=' . self::$port
                . ';dbname=' . self::$database . ';charset=' . self::$charset;
            self::$instance = new \PDO($dsn, self::$user, self::$password, [\PDO::ATTR_PERSISTENT => true]);
            return self::$instance;
        }
    }
}
