TARGET = qsqlsmpsql

HEADERS += $$PWD/qsql_smpsql_p.h
SOURCES += $$PWD/qsql_smpsql.cpp $$PWD/main.cpp

CONFIG += link_pkgconfig

PKGCONFIG += libpq

OTHER_FILES += smpsql.json

PLUGIN_CLASS_NAME = QSMPSQLDriverPlugin
include(../qsqldriverbase.pri)
