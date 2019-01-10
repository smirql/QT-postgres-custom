# QT-postgres-custom
Changed version of psql plugin (driver) to QtSQL. Based on Qt 5.11.1 version.

**Not fully tested, may contains errors**

Added support :
* JSON
* Arrays of types

## Installation

``` bash
sudo apt install libpq-dev pkg-config

cd $QT_SRC/qtbase/src/plugins/sqldrivers/
git clone git@github.com:smirql/QT-postgres-custom.git smpsql
cd smpsql
$QMAKE_PATH/qmake
make
make install # sudo if use system qmake
```

## Usage

``` c++
QSqlDatabase dbase = QSqlDatabase::addDatabase("QSMPSQL", db_name);
```
## Type matching

#### Read from PostgreSQL

| PostgreSQL |    | Qt |
|------------|:--:|----|
| varchar[] | -> | QStringList |
| text[] | -> | QStringList |
| json (any type) | -> | QJsonDocument |
| other_type[] | -> | QVariantList |

for more information see **qsql_smpsql.cpp** **qDecodePSQLType(...)**

#### Send to PostgreSQL

| Qt |    | PostgreSQL |
|----|:--:|------------|
| QStringList | -> | ARRAY[]::varchar[] |
| QVariantList | -> | ARRAY[] |
| QJsonDocument<br/>QJsonValue<br/>QJsonObject<br/>QJsonArray| -> | '....'::json |

for more information see **qsql_smpsql.cpp** **formatVariant(...)**


## Note

this code will return error from PostgreSQL

``` c++
QSqlQuery query;

QVariantList data;
query.prepare("SELECT :data");
query.bindValue(":data", data);
 
qDebug() << query.exec();
```
will return **false** : QVariantList -> ARRAY[] Base type of array is undefined

``` c++
QSqlQuery query;

QVariantList data;
data << 1;
data << QString("string");
query.prepare("SELECT :data");
query.bindValue(":data", data);
 
qDebug() << query.exec();
```
will return **false** : QVariantList -> ARRAY[1, 'string'] Array must contains only single type

``` c++
QSqlQuery query;

QStringList data;
query.prepare("SELECT :data");
query.bindValue(":data", data);
 
qDebug() << query.exec();
```
will return **true** : QStringList -> ARRAY[]::varchar[]

