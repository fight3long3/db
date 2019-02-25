<?php

namespace fight;


use fight\drive\PDO;

class Db
{
    private static $connect;
    private static $instance;

    private static $table;
    private $field;
    private $data;
    private $where;
    private $group;
    private $having;
    private $order = '';
    private $offset;
    private $length;
    private $key;
    private $expire;
    private $sql;
    private $params;
    private $alias;
    private $join;

    public static function init($host, $port, $database, $charset, $user, $password)
    {
        PDO::init($host, $port, $database, $charset, $user, $password);
    }

    public static function getConnect()
    {
        if (self::$connect) {
            return self::$connect;
        } else {
            self::$connect = PDO::getInstance();
            return self::$connect;
        }
    }

    /**
     * @param string $table
     * @return Db
     */
    public static function table(string $table): Db
    {
        self::$table = self::parseTable($table);
        if (is_null(self::$instance)) {
            return self::$instance = new self();
        } else {
            return self::$instance;
        }
    }

    public function field($field): Db
    {
        $this->field = $field;
        return $this;
    }

    public function data(array $data): Db
    {
        $this->data = $data;
        return $this;
    }

    public function where($where): Db
    {
        $this->where = $where;
        return $this;
    }

    public function limit(int $offset, int $length = null): Db
    {
        $this->offset = $offset;
        $this->length = $length;
        return $this;
    }

    public function group(string $group): Db
    {
        $this->group = ' ' . $group;
        return $this;
    }

    public function having(string $having): Db
    {
        $this->having = ' ' . $having;
        return $this;
    }

    public function order($field, string $order = null): Db
    {
        if (is_array($field)) {
            foreach ($field as $key => $value) {
                if (is_int($key)) {
                    $this->order .= ', ' . $value;
                } else {
                    $this->order .= ', ' . $key . ' ' . $value;
                }
            }
            $this->order = ltrim($this->order, ', ');
        } else {
            if (is_null($order)) {
                $this->order = $field;
            } else {
                $this->order = $field . ' ' . $order;
            }
        }
        return $this;
    }

    /**
     * TODO
     */
    public function cache($key = null, int $expire = 0): Db
    {
        $this->key = $key;
        $this->expire = $expire;
        return $this;
    }

    public function value(string $field)
    {
        $field = $this->parseColumn($field);
        $this->sql = 'select ' . $field . ' from ' . self::$table;
        $this->params = [];
        $this->getJoin();
        if ($this->where) {
            if (is_array($this->where)) {
                $this->sql .= ' where ' . $this->getWhereString($this->where);
            } else {
                $this->sql .= ' where ' . $this->where;
            }
            $this->where = null;
        }

        $this->sql .= ' limit 1';

        if (empty($this->params)) {
            if ($sth = Db::getConnect()->query($this->sql)) {
                return $sth->fetchColumn();
            }
        } else {
            if ($sth = Db::getConnect()->prepare($this->sql)) {
                if ($sth->execute($this->params)) {
                    return $sth->fetchColumn();
                }
            }
        }

        return false;
    }

    public function column(string $field): array
    {
        $field = $this->parseColumn($field);
        $this->sql = 'select ' . $field . ' from ' . self::$table;
        $this->params = [];
        $this->getJoin();
        if ($this->where) {
            if (is_array($this->where)) {
                $this->sql .= ' where ' . $this->getWhereString($this->where);
            } else {
                $this->sql .= ' where ' . $this->where;
            }
            $this->where = null;
        }
        if ($this->group) {
            $this->sql .= ' group by ' . $this->group;
            $this->group = null;
        }
        if ($this->having) {
            $this->sql .= ' having ' . $this->having;
            $this->having = null;
        }
        if ($this->order) {
            $this->sql .= ' order by ' . $this->order;
            $this->order = null;
        }
        if (!is_null($this->offset)) {
            $this->sql .= ' limit ' . $this->offset;
            if (!is_null($this->length)) {
                $this->sql .= ', ' . $this->length;
            }
            $this->offset = null;
            $this->length = null;
        }
        if (isset($this->params)) {
            if ($sth = Db::getConnect()->prepare($this->sql)) {
                if ($sth->execute($this->params)) {
                    return $sth->fetchAll(\PDO::FETCH_COLUMN);
                }
            }

        } else {
            if ($sth = Db::getConnect()->query($this->sql)) {
                return $sth->fetchAll(\PDO::FETCH_COLUMN);
            }
        }
        return [];
    }

    public function aggregate($aggregate, $field)
    {
        $field = $field === '*' ? '*' : $this->parseColumn($field);
        $this->sql = 'select ' . $aggregate . '(' . $field . ') from ' . self::$table;
        $this->params = [];
        $this->getJoin();
        if ($this->where) {
            if (is_array($this->where)) {
                $this->sql .= ' where ' . $this->getWhereString($this->where);
            } else {
                $this->sql .= ' where ' . $this->where;
            }
            $this->where = null;
        }
        if ($this->group) {
            $this->sql .= ' group by ' . $this->group;
            $this->group = null;
        }
        if ($this->having) {
            $this->sql .= ' having ' . $this->having;
            $this->having = null;
        }
        if (empty($this->params)) {
            if ($sth = Db::getConnect()->query($this->sql)) {
                return $sth->fetchColumn();
            }
        } else {
            if ($sth = Db::getConnect()->prepare($this->sql)) {
                if ($sth->execute($this->params)) {
                    return $sth->fetchColumn();
                }
            }
        }
        return 0;
    }

    public function count(string $field = '*'): int
    {
        return $this->aggregate('count', $field);
    }

    public function sum(string $field): int
    {
        return $this->aggregate('sum', $field);
    }

    public function min(string $field): int
    {
        return $this->aggregate('min', $field);
    }

    public function max(string $field): int
    {
        return $this->aggregate('max', $field);
    }

    public function avg(string $field): int
    {
        return $this->aggregate('avg', $field);
    }

    public function find()
    {
        if (is_string($this->field)) {
            $field = $this->field;
        } elseif (is_array($this->field)) {
            $field = '';
            foreach ($this->field as $key => $value) {
                if (is_int($key)) {
                    $field .= $this->parseColumn($value) . ',';
                } else {
                    $field .= $this->parseColumn($key) . ' ' . $value . ',';
                }
            }
            $field = rtrim($field, ',');
        } else {
            $field = '*';
        }
        $this->sql = 'select ' . $field . ' from ' . self::$table;

        $this->params = [];

        $this->getJoin();

        if ($this->where) {
            if (is_array($this->where)) {
                $this->sql .= ' where ' . $this->getWhereString($this->where);
            } else {
                $this->sql .= ' where ' . $this->where;
            }
            $this->where = null;
        }

        $this->sql .= ' limit 1';

        if (empty($this->params)) {
            if ($sth = Db::getConnect()->query($this->sql)) {
                return $sth->fetch(\PDO::FETCH_ASSOC);
            }
        } else {
            if ($sth = Db::getConnect()->prepare($this->sql)) {
                if ($sth->execute($this->params)) {
                    return $sth->fetch(\PDO::FETCH_ASSOC);
                }
            }
        }

        return false;
    }

    public function select(): array
    {
        if (is_string($this->field)) {
            $field = $this->field;
        } elseif (is_array($this->field)) {
            $field = '';
            foreach ($this->field as $key => $value) {
                if (is_int($key)) {
                    $field .= $this->parseColumn($value) . ',';
                } else {
                    $field .= $this->parseColumn($key) . ' ' . $value . ',';
                }
            }
            $field = rtrim($field, ',');
        } else {
            $field = '*';
        }
        $this->sql = 'select ' . $field . ' from ' . self::$table;

        $this->params = [];

        $this->getJoin();

        if (is_array($this->where)) {
            $this->sql .= ' where ' . $this->getWhereString($this->where);
            $this->where = null;
        } elseif (is_string($this->where)) {
            $this->sql .= ' where ' . $this->where;
            $this->where = null;
        }

        if ($this->group) {
            $this->sql .= ' group by ' . $this->group;
            $this->group = null;
        }

        if ($this->having) {
            $this->sql .= ' having ' . $this->having;
            $this->having = null;
        }

        if ($this->order) {
            $this->sql .= ' order by ' . $this->order;
            $this->order = null;
        }

        if (!is_null($this->offset)) {
            $this->sql .= ' limit ' . $this->offset;
            if (!is_null($this->length)) {
                $this->sql .= ', ' . $this->length;
            }
            $this->offset = null;
            $this->length = null;
        }

        if (empty($this->params)) {
            if ($sth = Db::getConnect()->query($this->sql)) {
                return $sth->fetchAll(\PDO::FETCH_ASSOC);
            }
        } else {
            if ($sth = Db::getConnect()->prepare($this->sql)) {
                if ($sth->execute($this->params)) {
                    return $sth->fetchAll(\PDO::FETCH_ASSOC);
                }
            }
        }

        return [];
    }

    public function insert(array $data = null): bool
    {
        $data = $data ?: $this->data;
        if (empty($data)) {
            return false;
        } else {
            $this->sql = 'insert into ' . self::$table;
            $columns = ' (';
            $values = ') values (';
            $this->params = [];
            foreach ($data as $key => $value) {
                $columns .= $this->parseColumn($key) . ', ';
                $values .= '?, ';
                $this->params[] = $value;
            }
            $this->sql .= rtrim($columns, ', ') . rtrim($values, ', ') . ')';
            if ($sth = self::getConnect()->prepare($this->sql)) {
                return $sth->execute($this->params);
            } else {
                return false;
            }
        }
    }

    public function insertGetId(array $data = null): int
    {
        if ($this->insert($data)) {
            return $this->lastInsertId();
        } else {
            return false;
        }
    }

    public function insertAll(array $dataSet = null)
    {
        $dataSet = $dataSet ?: $this->data;
        if ($dataSet) {
            $this->sql = 'insert into ' . self::$table;
            $columns = ' (';
            $values = ') values (';
            $this->params = [];
            foreach ($dataSet as $key => $value) {
                if (0 === $key) {
                    foreach ($value as $k => $v) {
                        $columns .= $this->parseColumn($k) . ', ';
                        $values .= '?, ';
                        $this->params[] = $v;
                    }
                    $values = rtrim($values, ', ') . '), (';
                } else {
                    foreach ($value as $k => $v) {
                        $values .= '?, ';
                        $this->params[] = $v;
                    }
                    $values = rtrim($values, ', ') . '), (';
                }
            }
            $this->sql .= rtrim($columns, ', ') . rtrim($values, ', (');
            if ($sth = self::getConnect()->prepare($this->sql)) {
                return $sth->execute($this->params);
            }
        }
        return false;
    }

    public function update(array $data = null)
    {
        $data = $data ?: $this->data;
        if ($data) {
            $this->sql = 'update ' . self::$table . ' set ';
            $this->params = [];
            foreach ($data as $key => $value) {
                $this->sql .= $this->parseColumn($key) . '=?, ';
                $this->params[] = $value;
            }
            $this->sql = rtrim($this->sql, ', ');
            if ($this->where) {
                if (is_array($this->where)) {
                    $this->sql .= ' where ' . $this->getWhereString($this->where);
                } else {
                    $this->sql .= ' where ' . $this->where;
                }
                $this->where = null;
            }
            if ($sth = self::getConnect()->prepare($this->sql)) {
                if ($sth->execute($this->params)) {
                    return $sth->rowCount();
                }
            }
        }
        return 0;
    }

    public function delete($idOrIds = null): int
    {
        $this->sql = /** @lang text */
            'delete from ' . self::$table;
        if ($idOrIds) {
            if (is_array($idOrIds)) {
                $this->sql .= ' where `id` in (' . str_repeat('?, ', count($idOrIds) - 1) . '?)';
                $this->params = $idOrIds;
            } else {
                $this->sql .= ' where `id` = ?';
                $this->params[] = $idOrIds;
            }
        } elseif ($this->where) {
            if (is_array($this->where)) {
                $this->params = [];
                $this->sql .= ' where ' . $this->getWhereString($this->where);
            } else {
                $this->sql .= ' where ' . $this->where;
            }
            $this->where = null;
        }
        if (isset($this->params)) {
            if ($sth = self::getConnect()->prepare($this->sql)) {
                if ($sth->execute($this->params)) {
                    return $sth->rowCount();
                }
            }
        } else {
            return Db::getConnect()->exec($this->sql);
        }
        return 0;
    }

    public function alias($alias)
    {
        if (is_array($alias)) {
            foreach ($alias as $key => $value) {
                if (false !== strpos($key, '`')) {
                    $this->alias[$key] = $value;
                } else {
                    $this->alias['`' . $key . '`'] = $value;
                }
            }
        } else {
            $this->alias = $alias;
        }
        return $this;
    }

    public function join($join, string $condition = '', string $type = 'inner', array $where = [])
    {
        if (is_array($join)) {
            if (false !== strpos(key($join), '`')) {
                $join = key($join) . ' ' . current($join);
            } else {
                $join = '`' . key($join) . '` ' . current($join);
            }
        } else {
            $join = self::parseTable($join);
        }
        $this->join[] = [
            $join,
            $condition,
            $type,
            $where
        ];
        return $this;
    }

    public function leftJoin($join, $condition = null, array $where = [])
    {
        $this->join($join, $condition, 'left', $where);
        return $this;
    }

    public function rightJoin($join, $condition = null, array $where = [])
    {
        $this->join($join, $condition, 'right', $where);
        return $this;
    }

    public function fullJoin($join, $condition = null, array $where = [])
    {
        $this->join($join, $condition, 'full', $where);
        return $this;
    }

    public function query(string $sql)
    {
        return self::getConnect()->query($sql);
    }

    public function execute(string $sql)
    {
        return self::getConnect()->exec($sql);
    }

    public function paginate(int $size = 10, int $page = null)
    {
        $this->length = $size;
        $page = is_null($page) && isset($_GET['page']) && ctype_digit($_GET['page']) && $_GET['page'] > 0
            ? $_GET['page'] : 1;
        $this->offset = ($page - 1) * $size;
        $data = [];
        $total = $this->count();
        if ($total > $this->offset) {
            $data = $this->select();
        }
        return [
            'total' => $total,
            'page' => $page,
            'size' => $size,
            'data' => $data
        ];
    }

    public static function startTrans()
    {
        return self::getConnect()->beginTransaction();
    }

    public static function commit()
    {
        return self::getConnect()->commit();
    }

    public static function rollback()
    {
        return self::getConnect()->rollback();
    }

    public function lastInsertId()
    {
        return self::getConnect()->lastInsertId();
    }

    public function getLastSql()
    {
        return $this->sql;
    }

    public function getLastParams()
    {
        return $this->params;
    }

    private function getWhereString(array $where)
    {
        $whereString = '';
        $logicalOperator = $this->getLogicalOperator($where);
        foreach ($where as $key => $value) {
            if (is_array($value)) {
                if (is_string($key)) {
                    if (is_array($value[0]) || 'or' === $value[0] || 'and' === $value[0]) {
                        $logicalOperatorChild = $this->getLogicalOperator($value);
                        foreach ($value as $item) {
                            if (is_array($item[1])) {
                                $whereString .= $this->parseColumn($key) . ' ' . $item[0] . ' ('
                                    . str_repeat('?, ', count($item[1]) - 1)
                                    . '?) ' . $logicalOperatorChild . ' ';
                                $this->params = array_merge($this->params, $item[1]);
                            } else {
                                $whereString .= $this->parseColumn($key) . ' ' . $item[0]
                                    . ' ? ' . $logicalOperatorChild . ' ';
                                $this->params[] = $item[1];
                            }
                        }
                        $whereString = rtrim($whereString, $logicalOperatorChild . ' ')
                            . ' ' . $logicalOperator . ' ';
                    } else {
                        if (is_array($value[1])) {
                            $whereString .= $this->parseColumn($key) . ' ' . $value[0] . ' ('
                                . str_repeat('?, ', count($value[1]) - 1)
                                . '?) ' . $logicalOperator . ' ';
                            $this->params = array_merge($this->params, $value[1]);
                        } else {
                            $whereString .= $this->parseColumn($key) . ' ' . $value[0] . ' ? ' . $logicalOperator . ' ';
                            $this->params[] = $value[1];
                        }
                    }
                } else {
                    $whereString .= '(' . $this->getWhereString($value) . ') ' . $logicalOperator . ' ';
                }
            } else {
                if (is_int($key)) {
                    $whereString .= $value . ' ' . $logicalOperator . ' ';
                } else {
                    $whereString .= $this->parseColumn($key) . ' = ? ' . $logicalOperator . ' ';
                    $this->params[] = $value;
                }
            }
        }
        return rtrim($whereString, $logicalOperator . ' ');
    }

    private function getJoin()
    {
        if (is_string($this->alias)) {
            $this->sql .= ' ' . $this->alias;
        } elseif (is_array($this->alias) && isset($this->alias[self::$table])) {
            $this->sql .= ' ' . $this->alias[self::$table];
        }

        if ($this->join) {
            foreach ($this->join as $item) {
                $this->sql .= ' ' . $item[2] . ' join ' . $item[0];
                if (isset($this->alias[$item[0]])) {
                    $this->sql .= ' ' . $this->alias[$item[0]];
                }
                if ($item[1]) {
                    $this->sql .= ' on ' . $item[1];
                }
                if ($item[3]) {
                    if ('or' === $item[3][0]) {
                        $this->sql .= ' or ';
                    } else {
                        $this->sql .= ' and ';
                    }
                    $this->sql .= $this->getWhereString($item[3]);
                }
            }
            $this->alias = null;
            $this->join = null;
        }
    }

    private static function parseTable($table)
    {
        if (false !== strpos($table, '`')) {
            return $table;
        }
        $table = trim($table);
        if (false !== strpos($table, ' ')) {
            [$table, $alias] = explode(' ', $table, 2);
            return '`' . $table . '` ' . $alias;
        } else {
            return '`' . $table . '`';
        }
    }

    private function parseColumn($column)
    {
        if (false !== strpos($column, '`')) {
            return $column;
        }
        $column = trim($column);
        if (false !== strpos($column, '(')) {
            [$aggregate, $column] = explode('(', $column);
            return $aggregate . '(' . $this->parseColumn(rtrim($column, ')')) . ')';
        } elseif (false !== strpos($column, '.')) {
            [$table, $column] = explode('.', $column, 2);
            if (isset($this->alias[$table])) {
                return '`' . $table . '`.`' . $column . '`';
            }
            return $table . '.`' . $column . '`';
        } else {
            return '`' . $column . '`';
        }
    }

    private function getLogicalOperator(array &$where): string
    {
        if (isset($where[0]) and 'or' === $where[0]) {
            unset($where[0]);
            return 'or';
        } elseif (isset($where[0]) and 'and' === $where[0]) {
            unset($where[0]);
        }
        return 'and';
    }

    private function getWhereKey(array $where)
    {
        $tmp = '';
        foreach ($where as $key => $value) {
            if (is_int($key)) {
                if (is_array($value)) {
                    $tmp .= $this->getWhereKey($value);
                } else {
                    $tmp .= $value;
                }
            } else {
                if (is_array($value)) {
                    if (is_array($value[0]) || 'or' === $value[0] || 'and' === $value[0]) {
                        foreach ($value as $item) {
                            $tmp .= $item[0];
                            if (is_array($item[1])) {
                                $this->params = array_merge($this->params, $item[1]);
                            } else {
                                $this->params[] = $item[1];
                            }
                        }
                    } else {
                        $tmp .= $key . $value[0];
                        if (is_array($value[1])) {
                            $this->params = array_merge($this->params, $value[1]);
                        } else {
                            $this->params[] = $value[1];
                        }
                    }
                } else {
                    $tmp .= $key;
                    $this->params[] = $value;
                }
            }
        }
        return $tmp;
    }

}
