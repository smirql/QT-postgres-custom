TARGET = qsqlsmpsql

HEADERS += $$PWD/qsql_smpsql_p.h
SOURCES += $$PWD/qsql_smpsql.cpp $$PWD/main.cpp

CONFIG += link_pkgconfig

PKGCONFIG += libpq

OTHER_FILES += smpsql.json

PLUGIN_CLASS_NAME = QSMPSQLDriverPlugin

QT  = core core-private sql-private
PLUGIN_TYPE = sqldrivers
load(qt_plugin)

DEFINES += QT_NO_CAST_TO_ASCII QT_NO_CAST_FROM_ASCII
