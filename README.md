# db
一个模仿thinkphp写的数据库操作工具

```php
use src\Db;

Db::init('localhost', '3306', 'novel', 'utf8', 'root', '1');

$list = Db::table('novel_type')
    ->where(['id' => 1])
    ->value('name');
var_dump($list);

$list = Db::table('novel_type')
    ->where(['id' => ['<=', 2]])
    ->column('name');
var_dump($list);

$list = Db::table('novel_type')
    ->field(['id', 'name'])
    ->where(['id' => 1])
    ->find();
var_dump($list);
$list = Db::table('novel_type')
    ->field(['id', 'name'])
    ->where(['id' => ['<=', 2]])
    ->select();
var_dump($list);

```


