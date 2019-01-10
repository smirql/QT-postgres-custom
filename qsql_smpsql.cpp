/****************************************************************************
**
** Copyright (C) 2016 The Qt Company Ltd.
** Contact: https://www.qt.io/licensing/
**
** This file is part of the QtSql module of the Qt Toolkit.
**
** $QT_BEGIN_LICENSE:LGPL$
** Commercial License Usage
** Licensees holding valid commercial Qt licenses may use this file in
** accordance with the commercial license agreement provided with the
** Software or, alternatively, in accordance with the terms contained in
** a written agreement between you and The Qt Company. For licensing terms
** and conditions see https://www.qt.io/terms-conditions. For further
** information use the contact form at https://www.qt.io/contact-us.
**
** GNU Lesser General Public License Usage
** Alternatively, this file may be used under the terms of the GNU Lesser
** General Public License version 3 as published by the Free Software
** Foundation and appearing in the file LICENSE.LGPL3 included in the
** packaging of this file. Please review the following information to
** ensure the GNU Lesser General Public License version 3 requirements
** will be met: https://www.gnu.org/licenses/lgpl-3.0.html.
**
** GNU General Public License Usage
** Alternatively, this file may be used under the terms of the GNU
** General Public License version 2.0 or (at your option) the GNU General
** Public license version 3 or any later version approved by the KDE Free
** Qt Foundation. The licenses are as published by the Free Software
** Foundation and appearing in the file LICENSE.GPL2 and LICENSE.GPL3
** included in the packaging of this file. Please review the following
** information to ensure the GNU General Public License requirements will
** be met: https://www.gnu.org/licenses/gpl-2.0.html and
** https://www.gnu.org/licenses/gpl-3.0.html.
**
** $QT_END_LICENSE$
**
****************************************************************************/

#include "qsql_smpsql_p.h"

#include <qcoreapplication.h>
#include <qvariant.h>
#include <qdatetime.h>
#include <qregexp.h>
#include <qsqlerror.h>
#include <qsqlfield.h>
#include <qsqlindex.h>
#include <qsqlrecord.h>
#include <qsqlquery.h>
#include <qsocketnotifier.h>
#include <qstringlist.h>
#include <qlocale.h>
#include <QtSql/private/qsqlresult_p.h>
#include <QtSql/private/qsqldriver_p.h>
#include <qjsondocument.h>

#include <QString>

#include <netinet/in.h>

#include <libpq-fe.h>
#include <pg_config.h>

#include <cmath>

#include <QJsonDocument>
#include <QJsonArray>
#include <QJsonValue>
#include <QJsonObject>

// workaround for postgres defining their OIDs in a private header file
#define QBOOLOID 16
#define QINT8OID 20
#define QINT2OID 21
#define QINT4OID 23
#define QNUMERICOID 1700
#define QFLOAT4OID 700
#define QFLOAT8OID 701
#define QABSTIMEOID 702
#define QRELTIMEOID 703
#define QDATEOID 1082
#define QTIMEOID 1083
#define QTIMETZOID 1266
#define QTIMESTAMPOID 1114
#define QTIMESTAMPTZOID 1184
#define QOIDOID 2278
#define QBYTEAOID 17
#define QREGPROCOID 24
#define QXIDOID 28
#define QCIDOID 29

#define QBITOID 1560
#define QVARBITOID 1562

// add json

#define SMJSONOID 114

// add arrays

#define SMBOOL_ARRAYOID 1000
#define SMINT8_ARRAYOID 1016
#define SMINT2_ARRAYOID 1005
#define SMINT4_ARRAYOID 1007
#define SMNUMERIC_ARRAYOID 1231
#define SMFLOAT4_ARRAYOID 1021
#define SMFLOAT8_ARRAYOID 1022
#define SMABSTIME_ARRAYOID 1023
#define SMRELTIME_ARRAYOID 1024
#define SMDATE_ARRAYOID 1182
#define SMTIME_ARRAYOID 1183
#define SMTIMETZ_ARRAYOID 1270
#define SMTIMESTAMP_ARRAYOID 1115
#define SMTIMESTAMPTZ_ARRAYOID 1185
#define SMBYTEA_ARRAYOID 1001
#define SMREGPROC_ARRAYOID 1008
#define SMXID_ARRAYOID 1011
#define SMCID_ARRAYOID 1012
#define SMJSON_ARRAYOID 199

#define SMVARCHAR_ARRAYOID 1015
#define SMTEXT_ARRAYOID 1009

#define VARHDRSZ 4

/* This is a compile time switch - if PQfreemem is declared, the compiler will use that one,
   otherwise it'll run in this template */
template <typename T>
inline void PQfreemem(T *t, int = 0) { free(t); }

Q_DECLARE_OPAQUE_POINTER(PGconn*)
Q_DECLARE_METATYPE(PGconn*)

Q_DECLARE_OPAQUE_POINTER(PGresult*)
Q_DECLARE_METATYPE(PGresult*)

QT_BEGIN_NAMESPACE

inline void qPQfreemem(void *buffer)
{
    PQfreemem(buffer);
}

/* Missing declaration of PGRES_SINGLE_TUPLE for PSQL below 9.2 */
#if !defined PG_VERSION_NUM || PG_VERSION_NUM-0 < 90200
static const int PGRES_SINGLE_TUPLE = 9;
#endif

typedef int StatementId;
static const StatementId InvalidStatementId = 0;

namespace  {
template <typename T>
const T& refV(const QVariant & variant) noexcept(false) {
    static T def;
    if (variant.userType() == qMetaTypeId<T>())
            return *reinterpret_cast<const T*>(variant.constData());
    return def;
}
}

class QSMPSQLResultPrivate;

class QSMPSQLResult final : public QSqlResult
{
    Q_DECLARE_PRIVATE(QSMPSQLResult)

public:
    QSMPSQLResult(const QSMPSQLDriver *db);
    ~QSMPSQLResult();

    QVariant handle() const override;
    void virtual_hook(int id, void *data) override;

    struct DataType {
        bool isArray = {false};
        int ptype = {0};
        QVariant::Type variant = {QVariant::Invalid};
        int metatype = {QMetaType::UnknownType};
    };

protected:
    void cleanup();
    bool fetch(int i) override;
    bool fetchFirst() override;
    bool fetchLast() override;
    bool fetchNext() override;
    bool nextResult() override;
    QVariant data(int i) override;
    bool isNull(int field) override;
    bool reset (const QString &query) override;
    int size() override;
    int numRowsAffected() override;
    QSqlRecord record() const override;
    QVariant lastInsertId() const override;
    bool prepare(const QString &query) override;
    bool exec() override;

private:
    QVariant processData(const DataType & info,
                         const char * val,
//                         const int start,
                         const int length);

    int findNextArrayPos(
            const char * begin,
            const int length);

    QPair<char *, int> clearEscapesSingleQouted(
            const char * begin,
            const int length);
    QPair<char *, int> clearEscapesDoubleQouted(
            const char * begin,
            const int length);
};

class QSMPSQLDriverPrivate final : public QSqlDriverPrivate
{
    Q_DECLARE_PUBLIC(QSMPSQLDriver)
public:
    QSMPSQLDriverPrivate() : QSqlDriverPrivate(),
        connection(0),
        isUtf8(false),
        pro(QSMPSQLDriver::Version6),
        sn(0),
        pendingNotifyCheck(false),
        hasBackslashEscape(false),
        stmtCount(0),
        currentStmtId(InvalidStatementId)
    { dbmsType = QSqlDriver::PostgreSQL; }

    PGconn *connection;
    bool isUtf8;
    QSMPSQLDriver::Protocol pro;
    QSocketNotifier *sn;
    QStringList seid;
    mutable bool pendingNotifyCheck;
    bool hasBackslashEscape;
    int stmtCount;
    StatementId currentStmtId;

    void appendTables(QStringList &tl, QSqlQuery &t, QChar type);
    PGresult *exec(const char *stmt);
    PGresult *exec(const QString &stmt);
    StatementId sendQuery(const QString &stmt);
    bool setSingleRowMode() const;
    PGresult *getResult(StatementId stmtId) const;
    void finishQuery(StatementId stmtId);
    void discardResults() const;
    StatementId generateStatementId();
    void checkPendingNotifications() const;
    QSMPSQLDriver::Protocol getPSQLVersion();
    bool setEncodingUtf8();
    void setDatestyle();
    void setByteaOutput();
    void detectBackslashEscape();
    mutable QHash<int, QString> oidToTable;
};

void QSMPSQLDriverPrivate::appendTables(QStringList &tl, QSqlQuery &t, QChar type)
{
    QString query = QString::fromLatin1("select pg_class.relname, pg_namespace.nspname from pg_class "
                                        "left join pg_namespace on (pg_class.relnamespace = pg_namespace.oid) "
                                        "where (pg_class.relkind = '%1') and (pg_class.relname !~ '^Inv') "
                                        "and (pg_class.relname !~ '^pg_') "
                                        "and (pg_namespace.nspname != 'information_schema')").arg(type);
    t.exec(query);
    while (t.next()) {
        QString schema = t.value(1).toString();
        if (schema.isEmpty() || schema == QLatin1String("public"))
            tl.append(t.value(0).toString());
        else
            tl.append(t.value(0).toString().prepend(QLatin1Char('.')).prepend(schema));
    }
}

PGresult *QSMPSQLDriverPrivate::exec(const char *stmt)
{
    // PQexec() silently discards any prior query results that the application didn't eat.
    PGresult *result = PQexec(connection, stmt);
    currentStmtId = result ? generateStatementId() : InvalidStatementId;
    checkPendingNotifications();
    return result;
}

PGresult *QSMPSQLDriverPrivate::exec(const QString &stmt)
{
    return exec((isUtf8 ? stmt.toUtf8() : stmt.toLocal8Bit()).constData());
}

StatementId QSMPSQLDriverPrivate::sendQuery(const QString &stmt)
{
    // Discard any prior query results that the application didn't eat.
    // This is required for PQsendQuery()
    discardResults();
    const int result = PQsendQuery(connection,
                                   (isUtf8 ? stmt.toUtf8() : stmt.toLocal8Bit()).constData());
    currentStmtId = result ? generateStatementId() : InvalidStatementId;
    return currentStmtId;
}

bool QSMPSQLDriverPrivate::setSingleRowMode() const
{
    // Activates single-row mode for last sent query, see:
    // https://www.postgresql.org/docs/9.2/static/libpq-single-row-mode.html
    // This method should be called immediately after the sendQuery() call.
#if defined PG_VERSION_NUM && PG_VERSION_NUM-0 >= 90200
    return PQsetSingleRowMode(connection) > 0;
#else
    return false;
#endif
}

PGresult *QSMPSQLDriverPrivate::getResult(StatementId stmtId) const
{
    // Make sure the results of stmtId weren't discaded. This might
    // happen for forward-only queries if somebody executed another
    // SQL query on the same db connection.
    if (stmtId != currentStmtId) {
        // If you change the following warning, remember to update it
        // on sql-driver.html page too.
        qWarning("QSMPSQLDriver::getResult: Query results lost - "
                 "probably discarded on executing another SQL query.");
        return nullptr;
    }
    PGresult *result = PQgetResult(connection);
    checkPendingNotifications();
    return result;
}

void QSMPSQLDriverPrivate::finishQuery(StatementId stmtId)
{
    if (stmtId != InvalidStatementId && stmtId == currentStmtId) {
        discardResults();
        currentStmtId = InvalidStatementId;
    }
}

void QSMPSQLDriverPrivate::discardResults() const
{
    while (PGresult *result = PQgetResult(connection))
        PQclear(result);
}

StatementId QSMPSQLDriverPrivate::generateStatementId()
{
    int stmtId = ++stmtCount;
    if (stmtId <= 0)
        stmtId = stmtCount = 1;
    return stmtId;
}

void QSMPSQLDriverPrivate::checkPendingNotifications() const
{
    Q_Q(const QSMPSQLDriver);
    if (seid.size() && !pendingNotifyCheck) {
        pendingNotifyCheck = true;
        QMetaObject::invokeMethod(const_cast<QSMPSQLDriver*>(q), "_q_handleNotification", Qt::QueuedConnection, Q_ARG(int,0));
    }
}

class QSMPSQLResultPrivate : public QSqlResultPrivate
{
    Q_DECLARE_PUBLIC(QSMPSQLResult)
public:
    Q_DECLARE_SQLDRIVER_PRIVATE(QSMPSQLDriver)
    QSMPSQLResultPrivate(QSMPSQLResult *q, const QSMPSQLDriver *drv)
      : QSqlResultPrivate(q, drv),
        result(0),
        currentSize(-1),
        canFetchMoreRows(false),
        stmtId(InvalidStatementId),
        preparedQueriesEnabled(false)
    { }

    QString fieldSerial(int i) const override { return QLatin1Char('$') + QString::number(i + 1); }
    void deallocatePreparedStmt();

    PGresult *result;
    QList<PGresult*> nextResultSets;
    int currentSize;
    bool canFetchMoreRows;
    StatementId stmtId;
    bool preparedQueriesEnabled;
    QString preparedStmtId;

    bool processResults();
};

static QSqlError qMakeError(const QString& err, QSqlError::ErrorType type,
                            const QSMPSQLDriverPrivate *p, PGresult* result = 0)
{
    const char *s = PQerrorMessage(p->connection);
    QString msg = p->isUtf8 ? QString::fromUtf8(s) : QString::fromLocal8Bit(s);
    QString errorCode;
    if (result) {
      errorCode = QString::fromLatin1(PQresultErrorField(result, PG_DIAG_SQLSTATE));
      msg += QString::fromLatin1("(%1)").arg(errorCode);
    }
    return QSqlError(QLatin1String("QSMPSQL: ") + err, msg, type, errorCode);
}

bool QSMPSQLResultPrivate::processResults()
{
    Q_Q(QSMPSQLResult);
    if (!result) {
        q->setSelect(false);
        q->setActive(false);
        currentSize = -1;
        canFetchMoreRows = false;
        if (stmtId != drv_d_func()->currentStmtId) {
            q->setLastError(qMakeError(QCoreApplication::translate("QSMPSQLResult",
                            "Query results lost - probably discarded on executing "
                            "another SQL query."), QSqlError::StatementError, drv_d_func(), result));
        }
        return false;
    }
    int status = PQresultStatus(result);
    switch (status) {
    case PGRES_TUPLES_OK:
        q->setSelect(true);
        q->setActive(true);
        currentSize = q->isForwardOnly() ? -1 : PQntuples(result);
        canFetchMoreRows = false;
        return true;
    case PGRES_SINGLE_TUPLE:
        q->setSelect(true);
        q->setActive(true);
        currentSize = -1;
        canFetchMoreRows = true;
        return true;
    case PGRES_COMMAND_OK:
        q->setSelect(false);
        q->setActive(true);
        currentSize = -1;
        canFetchMoreRows = false;
        return true;
    default:
        break;
    }
    q->setSelect(false);
    q->setActive(false);
    currentSize = -1;
    canFetchMoreRows = false;
    q->setLastError(qMakeError(QCoreApplication::translate("QSMPSQLResult",
                    "Unable to create query"), QSqlError::StatementError, drv_d_func(), result));
    return false;
}

static QSMPSQLResult::DataType qDecodePSQLType(int t)
{
    QSMPSQLResult::DataType type;
    type.ptype = t;
    type.isArray = false;

    switch (t) {
    case QBOOLOID:
        type.variant = QVariant::Bool;
        break;
    case QINT8OID:
        type.variant = QVariant::LongLong;
        break;
    case QINT2OID:
    case QINT4OID:
    case QOIDOID:
    case QREGPROCOID:
    case QXIDOID:
    case QCIDOID:
        type.variant = QVariant::Int;
        break;
    case QNUMERICOID:
    case QFLOAT4OID:
    case QFLOAT8OID:
        type.variant = QVariant::Double;
        break;
    case QABSTIMEOID:
    case QRELTIMEOID:
    case QDATEOID:
        type.variant = QVariant::Date;
        break;
    case QTIMEOID:
    case QTIMETZOID:
        type.variant = QVariant::Time;
        break;
    case QTIMESTAMPOID:
    case QTIMESTAMPTZOID:
        type.variant = QVariant::DateTime;
        break;
    case QBYTEAOID:
        type.variant = QVariant::ByteArray;
        break;
    case SMJSONOID:
        type.variant = QVariant::UserType;
        type.metatype = QMetaType::QJsonDocument;
        break;
    case SMBOOL_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::Bool;
        break;
    case SMINT8_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::LongLong;
        break;
    case SMINT2_ARRAYOID:
    case SMINT4_ARRAYOID:
    case SMREGPROC_ARRAYOID:
    case SMXID_ARRAYOID:
    case SMCID_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::Int;
        break;
    case SMNUMERIC_ARRAYOID:
    case SMFLOAT4_ARRAYOID:
    case SMFLOAT8_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::Double;
        break;
    case SMABSTIME_ARRAYOID:
    case SMRELTIME_ARRAYOID:
    case SMDATE_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::Date;
        break;
    case SMTIME_ARRAYOID:
    case SMTIMETZ_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::Time;
        break;
    case SMTIMESTAMP_ARRAYOID:
    case SMTIMESTAMPTZ_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::DateTime;
        break;
    case SMBYTEA_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::ByteArray;
        break;
    case SMJSON_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::UserType;
        type.metatype = QMetaType::QJsonDocument;
        break;
    case SMVARCHAR_ARRAYOID:
    case SMTEXT_ARRAYOID:
        type.isArray = true;
        type.variant = QVariant::String;
        break;
    default:
        type.variant = QVariant::String;
        break;
    }
    return type;
}

void QSMPSQLResultPrivate::deallocatePreparedStmt()
{
    const QString stmt = QLatin1String("DEALLOCATE ") + preparedStmtId;
    PGresult *result = drv_d_func()->exec(stmt);

    if (PQresultStatus(result) != PGRES_COMMAND_OK)
        qWarning("Unable to free statement: %s", PQerrorMessage(drv_d_func()->connection));
    PQclear(result);
    preparedStmtId.clear();
}

QSMPSQLResult::QSMPSQLResult(const QSMPSQLDriver* db)
    : QSqlResult(*new QSMPSQLResultPrivate(this, db))
{
    Q_D(QSMPSQLResult);
    d->preparedQueriesEnabled = db->hasFeature(QSqlDriver::PreparedQueries);
}

QSMPSQLResult::~QSMPSQLResult()
{
    Q_D(QSMPSQLResult);
    cleanup();

    if (d->preparedQueriesEnabled && !d->preparedStmtId.isNull())
        d->deallocatePreparedStmt();
}

QVariant QSMPSQLResult::handle() const
{
    Q_D(const QSMPSQLResult);
    return QVariant::fromValue(d->result);
}

void QSMPSQLResult::cleanup()
{
    Q_D(QSMPSQLResult);
    if (d->result)
        PQclear(d->result);
    d->result = nullptr;
    while (!d->nextResultSets.isEmpty())
        PQclear(d->nextResultSets.takeFirst());
    if (d->stmtId != InvalidStatementId) {
        if (d->drv_d_func())
            d->drv_d_func()->finishQuery(d->stmtId);
    }
    d->stmtId = InvalidStatementId;
    setAt(QSql::BeforeFirstRow);
    d->currentSize = -1;
    d->canFetchMoreRows = false;
    setActive(false);
}

bool QSMPSQLResult::fetch(int i)
{
    Q_D(const QSMPSQLResult);
    if (!isActive())
        return false;
    if (i < 0)
        return false;
    if (at() == i)
        return true;

    if (isForwardOnly()) {
        if (i < at())
            return false;
        bool ok = true;
        while (ok && i > at())
            ok = fetchNext();
        return ok;
    }

    if (i >= d->currentSize)
        return false;
    setAt(i);
    return true;
}

bool QSMPSQLResult::fetchFirst()
{
    Q_D(const QSMPSQLResult);
    if (!isActive())
        return false;
    if (at() == 0)
        return true;

    if (isForwardOnly()) {
        if (at() == QSql::BeforeFirstRow) {
            // First result has been already fetched by exec() or
            // nextResult(), just check it has at least one row.
            if (d->result && PQntuples(d->result) > 0) {
                setAt(0);
                return true;
            }
        }
        return false;
    }

    return fetch(0);
}

bool QSMPSQLResult::fetchLast()
{
    Q_D(const QSMPSQLResult);
    if (!isActive())
        return false;

    if (isForwardOnly()) {
        // Cannot seek to last row in forwardOnly mode, so we have to use brute force
        int i = at();
        if (i == QSql::AfterLastRow)
            return false;
        if (i == QSql::BeforeFirstRow)
            i = 0;
        while (fetchNext())
            ++i;
        setAt(i);
        return true;
    }

    return fetch(d->currentSize - 1);
}

bool QSMPSQLResult::fetchNext()
{
    Q_D(QSMPSQLResult);
    if (!isActive())
        return false;

    const int currentRow = at();  // Small optimalization
    if (currentRow == QSql::BeforeFirstRow)
        return fetchFirst();
    if (currentRow == QSql::AfterLastRow)
        return false;

    if (isForwardOnly()) {
        if (!d->canFetchMoreRows)
            return false;
        PQclear(d->result);
        d->result = d->drv_d_func()->getResult(d->stmtId);
        if (!d->result) {
            setLastError(qMakeError(QCoreApplication::translate("QSMPSQLResult",
                                    "Unable to get result"), QSqlError::StatementError, d->drv_d_func(), d->result));
            d->canFetchMoreRows = false;
            return false;
        }
        int status = PQresultStatus(d->result);
        switch (status) {
        case PGRES_SINGLE_TUPLE:
            // Fetched next row of current result set
            Q_ASSERT(PQntuples(d->result) == 1);
            Q_ASSERT(d->canFetchMoreRows);
            setAt(currentRow + 1);
            return true;
        case PGRES_TUPLES_OK:
            // In single-row mode PGRES_TUPLES_OK means end of current result set
            Q_ASSERT(PQntuples(d->result) == 0);
            d->canFetchMoreRows = false;
            return false;
        default:
            setLastError(qMakeError(QCoreApplication::translate("QSMPSQLResult",
                                    "Unable to get result"), QSqlError::StatementError, d->drv_d_func(), d->result));
            d->canFetchMoreRows = false;
            return false;
        }
    }

    if (currentRow + 1 >= d->currentSize)
        return false;
    setAt(currentRow + 1);
    return true;
}

bool QSMPSQLResult::nextResult()
{
    Q_D(QSMPSQLResult);
    if (!isActive())
        return false;

    setAt(QSql::BeforeFirstRow);

    if (isForwardOnly()) {
        if (d->canFetchMoreRows) {
            // Skip all rows from current result set
            while (d->result && PQresultStatus(d->result) == PGRES_SINGLE_TUPLE) {
                PQclear(d->result);
                d->result = d->drv_d_func()->getResult(d->stmtId);
            }
            d->canFetchMoreRows = false;
            // Check for unexpected errors
            if (d->result && PQresultStatus(d->result) == PGRES_FATAL_ERROR)
                return d->processResults();
        }
        // Fetch first result from next result set
        if (d->result)
            PQclear(d->result);
        d->result = d->drv_d_func()->getResult(d->stmtId);
        return d->processResults();
    }

    if (d->result)
        PQclear(d->result);
    d->result = d->nextResultSets.isEmpty() ? nullptr : d->nextResultSets.takeFirst();
    return d->processResults();
}

QVariant QSMPSQLResult::data(int i)
{
    Q_D(const QSMPSQLResult);
    if (i >= PQnfields(d->result)) {
        qWarning("QSMPSQLResult::data: column %d out of range", i);
        return QVariant();
    }
    const int currentRow = isForwardOnly() ? 0 : at();
    int ptype = PQftype(d->result, i);
    DataType type = qDecodePSQLType(ptype);
    if (PQgetisnull(d->result, currentRow, i))
        return QVariant((type.isArray) ? QVariant::List : type.variant);
    const char *val = PQgetvalue(d->result, currentRow, i);
    const int length = PQgetlength(d->result, currentRow, i);
    if (length > 0 && *val == '{'
            && !type.isArray
            && type.variant == QVariant::String)
        type.isArray = true;

    return processData(type,
                       val,
                       length);
}

bool QSMPSQLResult::isNull(int field)
{
    Q_D(const QSMPSQLResult);
    const int currentRow = isForwardOnly() ? 0 : at();
    return PQgetisnull(d->result, currentRow, field);
}

bool QSMPSQLResult::reset (const QString& query)
{
    Q_D(QSMPSQLResult);
    cleanup();
    if (!driver())
        return false;
    if (!driver()->isOpen() || driver()->isOpenError())
        return false;

    d->stmtId = d->drv_d_func()->sendQuery(query);
    if (d->stmtId == InvalidStatementId) {
        setLastError(qMakeError(QCoreApplication::translate("QSMPSQLResult",
                                "Unable to send query"), QSqlError::StatementError, d->drv_d_func()));
        return false;
    }

    if (isForwardOnly())
        setForwardOnly(d->drv_d_func()->setSingleRowMode());

    d->result = d->drv_d_func()->getResult(d->stmtId);
    if (!isForwardOnly()) {
        // Fetch all result sets right away
        while (PGresult *nextResultSet = d->drv_d_func()->getResult(d->stmtId))
            d->nextResultSets.append(nextResultSet);
    }
    return d->processResults();
}

int QSMPSQLResult::size()
{
    Q_D(const QSMPSQLResult);
    return d->currentSize;
}

int QSMPSQLResult::numRowsAffected()
{
    Q_D(const QSMPSQLResult);
    return QString::fromLatin1(PQcmdTuples(d->result)).toInt();
}

QVariant QSMPSQLResult::lastInsertId() const
{
    Q_D(const QSMPSQLResult);
    if (d->drv_d_func()->pro >= QSMPSQLDriver::Version8_1) {
        QSqlQuery qry(driver()->createResult());
        // Most recent sequence value obtained from nextval
        if (qry.exec(QLatin1String("SELECT lastval();")) && qry.next())
            return qry.value(0);
    } else if (isActive()) {
        Oid id = PQoidValue(d->result);
        if (id != InvalidOid)
            return QVariant(id);
    }
    return QVariant();
}

QSqlRecord QSMPSQLResult::record() const
{
    Q_D(const QSMPSQLResult);
    QSqlRecord info;
    if (!isActive() || !isSelect())
        return info;

    int count = PQnfields(d->result);
    for (int i = 0; i < count; ++i) {
        QSqlField f;
        if (d->drv_d_func()->isUtf8)
            f.setName(QString::fromUtf8(PQfname(d->result, i)));
        else
            f.setName(QString::fromLocal8Bit(PQfname(d->result, i)));
        const int tableOid = PQftable(d->result, i);
        // WARNING: We cannot execute any other SQL queries on
        // the same db connection while forward-only mode is active
        // (this would discard all results of forward-only query).
        // So we just skip this...
        if (tableOid != InvalidOid && !isForwardOnly()) {
            auto &tableName = d->drv_d_func()->oidToTable[tableOid];
            if (tableName.isEmpty()) {
                QSqlQuery qry(driver()->createResult());
                if (qry.exec(QStringLiteral("SELECT relname FROM pg_class WHERE pg_class.oid = %1")
                            .arg(tableOid)) && qry.next()) {
                    tableName = qry.value(0).toString();
                }
            }
            f.setTableName(tableName);
        }
        int ptype = PQftype(d->result, i);

        {
            auto type = qDecodePSQLType(ptype);
            QVariant::Type v_t = (type.isArray) ? QVariant::List : type.variant;
            f.setType(std::move(v_t));
        }
        int len = PQfsize(d->result, i);
        int precision = PQfmod(d->result, i);

        switch (ptype) {
        case QTIMESTAMPOID:
        case QTIMESTAMPTZOID:
            precision = 3;
            break;

        case QNUMERICOID:
            if (precision != -1) {
                len = (precision >> 16);
                precision = ((precision - VARHDRSZ) & 0xffff);
            }
            break;
        case QBITOID:
        case QVARBITOID:
            len = precision;
            precision = -1;
            break;
        default:
            if (len == -1 && precision >= VARHDRSZ) {
                len = precision - VARHDRSZ;
                precision = -1;
            }
        }

        f.setLength(len);
        f.setPrecision(precision);
        f.setSqlType(ptype);
        info.append(f);
    }
    return info;
}

void QSMPSQLResult::virtual_hook(int id, void *data)
{
    Q_ASSERT(data);

    QSqlResult::virtual_hook(id, data);
}

static QString qCreateParamString(const QVector<QVariant> &boundValues, const QSqlDriver *driver)
{
    if (boundValues.isEmpty())
        return QString();

    QString params;
    QSqlField f;
    for (int i = 0; i < boundValues.count(); ++i) {
        const QVariant &val = boundValues.at(i);

        f.setType(val.type());
        if (val.isNull())
            f.clear();
        else
            f.setValue(val);
        if(!params.isNull())
            params.append(QLatin1String(", "));
        params.append(driver->formatValue(f));
    }
    return params;
}

QString qMakePreparedStmtId()
{
    static QBasicAtomicInt qPreparedStmtCount = Q_BASIC_ATOMIC_INITIALIZER(0);
    QString id = QLatin1String("qsmpsqlpstmt_") + QString::number(qPreparedStmtCount.fetchAndAddRelaxed(1) + 1, 16);
    return id;
}

bool QSMPSQLResult::prepare(const QString &query)
{
    Q_D(QSMPSQLResult);
    if (!d->preparedQueriesEnabled)
        return QSqlResult::prepare(query);

    cleanup();

    if (!d->preparedStmtId.isEmpty())
        d->deallocatePreparedStmt();

    const QString stmtId = qMakePreparedStmtId();
    const QString stmt = QString::fromLatin1("PREPARE %1 AS ").arg(stmtId).append(d->positionalToNamedBinding(query));

    PGresult *result = d->drv_d_func()->exec(stmt);

    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        setLastError(qMakeError(QCoreApplication::translate("QSMPSQLResult",
                                "Unable to prepare statement"), QSqlError::StatementError, d->drv_d_func(), result));
        PQclear(result);
        d->preparedStmtId.clear();
        return false;
    }

    PQclear(result);
    d->preparedStmtId = stmtId;
    return true;
}

bool QSMPSQLResult::exec()
{
    Q_D(QSMPSQLResult);
    if (!d->preparedQueriesEnabled)
        return QSqlResult::exec();

    cleanup();

    QString stmt;
    const QString params = qCreateParamString(boundValues(), driver());
    if (params.isEmpty())
        stmt = QString::fromLatin1("EXECUTE %1").arg(d->preparedStmtId);
    else
        stmt = QString::fromLatin1("EXECUTE %1 (%2)").arg(d->preparedStmtId, params);

    d->stmtId = d->drv_d_func()->sendQuery(stmt);
    if (d->stmtId == InvalidStatementId) {
        setLastError(qMakeError(QCoreApplication::translate("QSMPSQLResult",
                                "Unable to send query"), QSqlError::StatementError, d->drv_d_func()));
        return false;
    }

    if (isForwardOnly())
        setForwardOnly(d->drv_d_func()->setSingleRowMode());

    d->result = d->drv_d_func()->getResult(d->stmtId);
    if (!isForwardOnly()) {
        // Fetch all result sets right away
        while (PGresult *nextResultSet = d->drv_d_func()->getResult(d->stmtId))
            d->nextResultSets.append(nextResultSet);
    }
    return d->processResults();
}

///////////////////////////////////////////////////////////////////

bool QSMPSQLDriverPrivate::setEncodingUtf8()
{
    PGresult* result = exec("SET CLIENT_ENCODING TO 'UNICODE'");
    int status = PQresultStatus(result);
    PQclear(result);
    return status == PGRES_COMMAND_OK;
}

void QSMPSQLDriverPrivate::setDatestyle()
{
    PGresult* result = exec("SET DATESTYLE TO 'ISO'");
    int status =  PQresultStatus(result);
    if (status != PGRES_COMMAND_OK)
        qWarning("%s", PQerrorMessage(connection));
    PQclear(result);
}

void QSMPSQLDriverPrivate::setByteaOutput()
{
    if (pro >= QSMPSQLDriver::Version9) {
        // Server version before QSMPSQLDriver::Version9 only supports escape mode for bytea type,
        // but bytea format is set to hex by default in PSQL 9 and above. So need to force the
        // server to use the old escape mode when connects to the new server.
        PGresult *result = exec("SET bytea_output TO escape");
        int status = PQresultStatus(result);
        if (status != PGRES_COMMAND_OK)
            qWarning("%s", PQerrorMessage(connection));
        PQclear(result);
    }
}

void QSMPSQLDriverPrivate::detectBackslashEscape()
{
    // standard_conforming_strings option introduced in 8.2
    // http://www.postgresql.org/docs/8.2/static/runtime-config-compatible.html
    if (pro < QSMPSQLDriver::Version8_2) {
        hasBackslashEscape = true;
    } else {
        hasBackslashEscape = false;
        PGresult* result = exec(QLatin1String("SELECT '\\\\' x"));
        int status = PQresultStatus(result);
        if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK)
            if (QString::fromLatin1(PQgetvalue(result, 0, 0)) == QLatin1String("\\"))
                hasBackslashEscape = true;
        PQclear(result);
    }
}

static QSMPSQLDriver::Protocol qMakePSQLVersion(int vMaj, int vMin)
{
    switch (vMaj) {
    case 6:
        return QSMPSQLDriver::Version6;
    case 7:
    {
        switch (vMin) {
        case 1:
            return QSMPSQLDriver::Version7_1;
        case 3:
            return QSMPSQLDriver::Version7_3;
        case 4:
            return QSMPSQLDriver::Version7_4;
        default:
            return QSMPSQLDriver::Version7;
        }
        break;
    }
    case 8:
    {
        switch (vMin) {
        case 1:
            return QSMPSQLDriver::Version8_1;
        case 2:
            return QSMPSQLDriver::Version8_2;
        case 3:
            return QSMPSQLDriver::Version8_3;
        case 4:
            return QSMPSQLDriver::Version8_4;
        default:
            return QSMPSQLDriver::Version8;
        }
        break;
    }
    case 9:
    {
        switch (vMin) {
        case 1:
            return QSMPSQLDriver::Version9_1;
        case 2:
            return QSMPSQLDriver::Version9_2;
        case 3:
            return QSMPSQLDriver::Version9_3;
        case 4:
            return QSMPSQLDriver::Version9_4;
        case 5:
            return QSMPSQLDriver::Version9_5;
        case 6:
            return QSMPSQLDriver::Version9_6;
        default:
            return QSMPSQLDriver::Version9;
        }
        break;
    }
    case 10:
        return QSMPSQLDriver::Version10;
    default:
        if (vMaj > 10)
            return QSMPSQLDriver::UnknownLaterVersion;
        break;
    }
    return QSMPSQLDriver::VersionUnknown;
}

static QSMPSQLDriver::Protocol qFindPSQLVersion(const QString &versionString)
{
    const QRegExp rx(QStringLiteral("(\\d+)(?:\\.(\\d+))?"));
    if (rx.indexIn(versionString) != -1) {
        // Beginning with PostgreSQL version 10, a major release is indicated by
        // increasing the first part of the version, e.g. 10 to 11.
        // Before version 10, a major release was indicated by increasing either
        // the first or second part of the version number, e.g. 9.5 to 9.6.
        int vMaj = rx.cap(1).toInt();
        int vMin;
        if (vMaj >= 10) {
            vMin = 0;
        } else {
            if (rx.cap(2).isEmpty())
                return QSMPSQLDriver::VersionUnknown;
            vMin = rx.cap(2).toInt();
        }
        return qMakePSQLVersion(vMaj, vMin);
    }

    return QSMPSQLDriver::VersionUnknown;
}

QSMPSQLDriver::Protocol QSMPSQLDriverPrivate::getPSQLVersion()
{
    QSMPSQLDriver::Protocol serverVersion = QSMPSQLDriver::Version6;
    PGresult* result = exec("select version()");
    int status = PQresultStatus(result);
    if (status == PGRES_COMMAND_OK || status == PGRES_TUPLES_OK) {
        serverVersion = qFindPSQLVersion(
            QString::fromLatin1(PQgetvalue(result, 0, 0)));
    }
    PQclear(result);

    QSMPSQLDriver::Protocol clientVersion =
#if defined(PG_MAJORVERSION)
        qFindPSQLVersion(QLatin1String(PG_MAJORVERSION));
#elif defined(PG_VERSION)
        qFindPSQLVersion(QLatin1String(PG_VERSION));
#else
        QSMPSQLDriver::VersionUnknown;
#endif

    if (serverVersion == QSMPSQLDriver::VersionUnknown) {
        serverVersion = clientVersion;
        if (serverVersion != QSMPSQLDriver::VersionUnknown)
            qWarning("The server version of this PostgreSQL is unknown, falling back to the client version.");
    }

    // Keep the old behavior unchanged
    if (serverVersion == QSMPSQLDriver::VersionUnknown)
        serverVersion = QSMPSQLDriver::Version6;

    if (serverVersion < QSMPSQLDriver::Version7_3) {
        qWarning("This version of PostgreSQL is not supported and may not work.");
    }

    return serverVersion;
}

QSMPSQLDriver::QSMPSQLDriver(QObject *parent)
    : QSqlDriver(*new QSMPSQLDriverPrivate, parent)
{
}

QSMPSQLDriver::QSMPSQLDriver(PGconn *conn, QObject *parent)
    : QSqlDriver(*new QSMPSQLDriverPrivate, parent)
{
    Q_D(QSMPSQLDriver);
    d->connection = conn;
    if (conn) {
        d->pro = d->getPSQLVersion();
        d->detectBackslashEscape();
        setOpen(true);
        setOpenError(false);
    }
}

QSMPSQLDriver::~QSMPSQLDriver()
{
    Q_D(QSMPSQLDriver);
    if (d->connection)
        PQfinish(d->connection);
}

QVariant QSMPSQLDriver::handle() const
{
    Q_D(const QSMPSQLDriver);
    return QVariant::fromValue(d->connection);
}

bool QSMPSQLDriver::hasFeature(DriverFeature f) const
{
    Q_D(const QSMPSQLDriver);
    switch (f) {
    case Transactions:
    case QuerySize:
    case LastInsertId:
    case LowPrecisionNumbers:
    case EventNotifications:
    case MultipleResultSets:
    case BLOB:
        return true;
    case PreparedQueries:
    case PositionalPlaceholders:
        return d->pro >= QSMPSQLDriver::Version8_2;
    case BatchOperations:
    case NamedPlaceholders:
    case SimpleLocking:
    case FinishQuery:
    case CancelQuery:
        return false;
    case Unicode:
        return d->isUtf8;
    }
    return false;
}

/*
   Quote a string for inclusion into the connection string
   \ -> \\
   ' -> \'
   surround string by single quotes
 */
static QString qQuote(QString s)
{
    s.replace(QLatin1Char('\\'), QLatin1String("\\\\"));
    s.replace(QLatin1Char('\''), QLatin1String("\\'"));
    s.append(QLatin1Char('\'')).prepend(QLatin1Char('\''));
    return s;
}

bool QSMPSQLDriver::open(const QString & db,
                        const QString & user,
                        const QString & password,
                        const QString & host,
                        int port,
                        const QString& connOpts)
{
    Q_D(QSMPSQLDriver);
    if (isOpen())
        close();
    QString connectString;
    if (!host.isEmpty())
        connectString.append(QLatin1String("host=")).append(qQuote(host));
    if (!db.isEmpty())
        connectString.append(QLatin1String(" dbname=")).append(qQuote(db));
    if (!user.isEmpty())
        connectString.append(QLatin1String(" user=")).append(qQuote(user));
    if (!password.isEmpty())
        connectString.append(QLatin1String(" password=")).append(qQuote(password));
    if (port != -1)
        connectString.append(QLatin1String(" port=")).append(qQuote(QString::number(port)));

    // add any connect options - the server will handle error detection
    if (!connOpts.isEmpty()) {
        QString opt = connOpts;
        opt.replace(QLatin1Char(';'), QLatin1Char(' '), Qt::CaseInsensitive);
        connectString.append(QLatin1Char(' ')).append(opt);
    }

    d->connection = PQconnectdb(std::move(connectString).toLocal8Bit().constData());
    if (PQstatus(d->connection) == CONNECTION_BAD) {
        setLastError(qMakeError(tr("Unable to connect"), QSqlError::ConnectionError, d));
        setOpenError(true);
        PQfinish(d->connection);
        d->connection = 0;
        return false;
    }

    d->pro = d->getPSQLVersion();
    d->detectBackslashEscape();
    d->isUtf8 = d->setEncodingUtf8();
    d->setDatestyle();
    d->setByteaOutput();

    setOpen(true);
    setOpenError(false);
    return true;
}

void QSMPSQLDriver::close()
{
    Q_D(QSMPSQLDriver);
    if (isOpen()) {

        d->seid.clear();
        if (d->sn) {
            disconnect(d->sn, SIGNAL(activated(int)), this, SLOT(_q_handleNotification(int)));
            delete d->sn;
            d->sn = 0;
        }

        if (d->connection)
            PQfinish(d->connection);
        d->connection = 0;
        setOpen(false);
        setOpenError(false);
    }
}

QSqlResult *QSMPSQLDriver::createResult() const
{
    return new QSMPSQLResult(this);
}

bool QSMPSQLDriver::beginTransaction()
{
    Q_D(QSMPSQLDriver);
    if (!isOpen()) {
        qWarning("QSMPSQLDriver::beginTransaction: Database not open");
        return false;
    }
    PGresult* res = d->exec("BEGIN");
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        setLastError(qMakeError(tr("Could not begin transaction"),
                                QSqlError::TransactionError, d, res));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

bool QSMPSQLDriver::commitTransaction()
{
    Q_D(QSMPSQLDriver);
    if (!isOpen()) {
        qWarning("QSMPSQLDriver::commitTransaction: Database not open");
        return false;
    }
    PGresult* res = d->exec("COMMIT");

    bool transaction_failed = false;

    // XXX
    // This hack is used to tell if the transaction has succeeded for the protocol versions of
    // PostgreSQL below. For 7.x and other protocol versions we are left in the dark.
    // This hack can dissapear once there is an API to query this sort of information.
    if (d->pro >= QSMPSQLDriver::Version8) {
        transaction_failed = qstrcmp(PQcmdStatus(res), "ROLLBACK") == 0;
    }

    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK || transaction_failed) {
        setLastError(qMakeError(tr("Could not commit transaction"),
                                QSqlError::TransactionError, d, res));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

bool QSMPSQLDriver::rollbackTransaction()
{
    Q_D(QSMPSQLDriver);
    if (!isOpen()) {
        qWarning("QSMPSQLDriver::rollbackTransaction: Database not open");
        return false;
    }
    PGresult* res = d->exec("ROLLBACK");
    if (!res || PQresultStatus(res) != PGRES_COMMAND_OK) {
        setLastError(qMakeError(tr("Could not rollback transaction"),
                                QSqlError::TransactionError, d, res));
        PQclear(res);
        return false;
    }
    PQclear(res);
    return true;
}

QStringList QSMPSQLDriver::tables(QSql::TableType type) const
{
    Q_D(const QSMPSQLDriver);
    QStringList tl;
    if (!isOpen())
        return tl;
    QSqlQuery t(createResult());
    t.setForwardOnly(true);

    if (type & QSql::Tables)
        const_cast<QSMPSQLDriverPrivate*>(d)->appendTables(tl, t, QLatin1Char('r'));
    if (type & QSql::Views)
        const_cast<QSMPSQLDriverPrivate*>(d)->appendTables(tl, t, QLatin1Char('v'));
    if (type & QSql::SystemTables) {
        t.exec(QLatin1String("select relname from pg_class where (relkind = 'r') "
                "and (relname like 'pg_%') "));
        while (t.next())
            tl.append(t.value(0).toString());
    }

    return tl;
}

static void qSplitTableName(QString &tablename, QString &schema)
{
    int dot = tablename.indexOf(QLatin1Char('.'));
    if (dot == -1)
        return;
    schema = tablename.left(dot);
    tablename = tablename.mid(dot + 1);
}

QSqlIndex QSMPSQLDriver::primaryIndex(const QString& tablename) const
{
    QSqlIndex idx(tablename);
    if (!isOpen())
        return idx;
    QSqlQuery i(createResult());

    QString tbl = tablename;
    QString schema;
    qSplitTableName(tbl, schema);

    if (isIdentifierEscaped(tbl, QSqlDriver::TableName))
        tbl = stripDelimiters(tbl, QSqlDriver::TableName);
    else
        tbl = std::move(tbl).toLower();

    if (isIdentifierEscaped(schema, QSqlDriver::TableName))
        schema = stripDelimiters(schema, QSqlDriver::TableName);
    else
        schema = std::move(schema).toLower();

    QString stmt = QLatin1String("SELECT pg_attribute.attname, pg_attribute.atttypid::int, "
                                 "pg_class.relname "
                                 "FROM pg_attribute, pg_class "
                                 "WHERE %1 pg_class.oid IN "
                                 "(SELECT indexrelid FROM pg_index WHERE indisprimary = true AND indrelid IN "
                                 "(SELECT oid FROM pg_class WHERE relname = '%2')) "
                                 "AND pg_attribute.attrelid = pg_class.oid "
                                 "AND pg_attribute.attisdropped = false "
                                 "ORDER BY pg_attribute.attnum");
    if (schema.isEmpty())
        stmt = stmt.arg(QLatin1String("pg_table_is_visible(pg_class.oid) AND"));
    else
        stmt = stmt.arg(QString::fromLatin1("pg_class.relnamespace = (select oid from "
                                        "pg_namespace where pg_namespace.nspname = '%1') AND").arg(schema));

    i.exec(stmt.arg(tbl));
    while (i.isActive() && i.next()) {
        auto type = qDecodePSQLType(i.value(1).toInt());
        QVariant::Type v_t = (type.isArray) ? QVariant::List : type.variant;

        QSqlField f(i.value(0).toString(), std::move(v_t), tablename);
        idx.append(f);
        idx.setName(i.value(2).toString());
    }
    return idx;
}

QSqlRecord QSMPSQLDriver::record(const QString& tablename) const
{
    QSqlRecord info;
    if (!isOpen())
        return info;

    QString tbl = tablename;
    QString schema;
    qSplitTableName(tbl, schema);

    if (isIdentifierEscaped(tbl, QSqlDriver::TableName))
        tbl = stripDelimiters(tbl, QSqlDriver::TableName);
    else
        tbl = std::move(tbl).toLower();

    if (isIdentifierEscaped(schema, QSqlDriver::TableName))
        schema = stripDelimiters(schema, QSqlDriver::TableName);
    else
        schema = std::move(schema).toLower();

    QString stmt = QLatin1String("select pg_attribute.attname, pg_attribute.atttypid::int, "
                                 "pg_attribute.attnotnull, pg_attribute.attlen, pg_attribute.atttypmod, "
                                 "pg_attrdef.adsrc "
                                 "from pg_class, pg_attribute "
                                 "left join pg_attrdef on (pg_attrdef.adrelid = "
                                 "pg_attribute.attrelid and pg_attrdef.adnum = pg_attribute.attnum) "
                                 "where %1 "
                                 "and pg_class.relname = '%2' "
                                 "and pg_attribute.attnum > 0 "
                                 "and pg_attribute.attrelid = pg_class.oid "
                                 "and pg_attribute.attisdropped = false "
                                 "order by pg_attribute.attnum");
    if (schema.isEmpty())
        stmt = stmt.arg(QLatin1String("pg_table_is_visible(pg_class.oid)"));
    else
        stmt = stmt.arg(QString::fromLatin1("pg_class.relnamespace = (select oid from "
                                            "pg_namespace where pg_namespace.nspname = '%1')").arg(schema));

    QSqlQuery query(createResult());
    query.exec(stmt.arg(tbl));
    while (query.next()) {
        int len = query.value(3).toInt();
        int precision = query.value(4).toInt();
        // swap length and precision if length == -1
        if (len == -1 && precision > -1) {
            len = precision - 4;
            precision = -1;
        }
        QString defVal = query.value(5).toString();
        if (!defVal.isEmpty() && defVal.at(0) == QLatin1Char('\''))
            defVal = defVal.mid(1, defVal.length() - 2);

        auto type = qDecodePSQLType(query.value(1).toInt());
        QVariant::Type v_t = (type.isArray) ? QVariant::List : type.variant;

        QSqlField f(query.value(0).toString(), std::move(v_t), tablename);
        f.setRequired(query.value(2).toBool());
        f.setLength(len);
        f.setPrecision(precision);
        f.setDefaultValue(defVal);
        f.setSqlType(query.value(1).toInt());
        info.append(f);
    }

    return info;
}

template <class FloatType>
inline void assignSpecialPsqlFloatValue(FloatType val, QString *target)
{
    if (qIsNaN(val))
        *target = QStringLiteral("'NaN'");
    else if (qIsInf(val))
        *target = (val < 0) ? QStringLiteral("'-Infinity'") : QStringLiteral("'Infinity'");
}

QString QSMPSQLDriver::formatValue(const QSqlField &field, bool trimStrings) const
{
    QString r;
    if (field.isNull()) {
        r = QLatin1String("NULL");
    } else {
        r = formatVariant(field.value(), true, trimStrings);
    }
    return r;
}

QString QSMPSQLDriver::formatVariant(const QVariant &value, bool isroot, bool trimStrings) const
{
    Q_D(const QSMPSQLDriver);
    QString r;
    if (value.isNull() || !value.isValid()) {
        r = QLatin1String("NULL");
    } else {
        switch (value.type()) {
        case QVariant::DateTime:
#ifndef QT_NO_DATESTRING
            if (value.toDateTime().isValid()) {
                // we force the value to be considered with a timezone information, and we force it to be UTC
                // this is safe since postgresql stores only the UTC value and not the timezone offset (only used
                // while parsing), so we have correct behavior in both case of with timezone and without tz
                r = QLatin1String("TIMESTAMP WITH TIME ZONE ") + QLatin1Char('\'') +
                        QLocale::c().toString(value.toDateTime().toUTC(), QLatin1String("yyyy-MM-ddThh:mm:ss.zzz")) +
                        QLatin1Char('Z') + QLatin1Char('\'');
            } else {
                r = QLatin1String("NULL");
            }
#else
            r = QLatin1String("NULL");
#endif // QT_NO_DATESTRING
            break;
        case QVariant::Time:
#ifndef QT_NO_DATESTRING
            if (value.toTime().isValid()) {
                r = QLatin1Char('\'') + value.toTime().toString(QLatin1String("hh:mm:ss.zzz")) + QLatin1Char('\'');
            } else
#endif
            {
                r = QLatin1String("NULL");
            }
            break;
        case QVariant::String: {
            QSqlField field(QString::fromUtf8(""), value.type());
            field.setValue(value);
            r = QSqlDriver::formatValue(field, trimStrings);
            if (d->hasBackslashEscape)
                r.replace(QLatin1String("\\"), QLatin1String("\\\\"));
        } break;
        case QVariant::Bool:
            if (value.toBool())
                r = QLatin1String("TRUE");
            else
                r = QLatin1String("FALSE");
            break;
        case QVariant::ByteArray: {
            QByteArray ba(value.toByteArray());
            size_t len;
#if defined PG_VERSION_NUM && PG_VERSION_NUM-0 >= 80200
            unsigned char *data = PQescapeByteaConn(d->connection, (const unsigned char*)ba.constData(), ba.size(), &len);
#else
            unsigned char *data = PQescapeBytea((const unsigned char*)ba.constData(), ba.size(), &len);
#endif
            r += QLatin1Char('\'');
            r += QLatin1String((const char*)data);
            r += QLatin1Char('\'');
            qPQfreemem(data);
            break;
        }
        case QVariant::Double:
            assignSpecialPsqlFloatValue(value.toDouble(), &r);
            if (r.isEmpty()) {
                QSqlField field(QString::fromUtf8(""), value.type());
                field.setValue(value);
                r = QSqlDriver::formatValue(field, trimStrings);
            }
            break;
        case QVariant::Uuid:
            r = QLatin1Char('\'') + value.toString() + QLatin1Char('\'');
            break;
        case QVariant::Date: {
            const QDate & date = refV<QDate>(value);

            r = QLatin1Char('\'') + date.toString(QString::fromUtf8("yyyy-MM-dd")) +  QLatin1Char('\'') + QString::fromUtf8("::date");
        } break;
        case QVariant::StringList:
        {
            QStringList str_lst = value.toStringList();

            if (str_lst.count() == 0)
                r = QString::fromUtf8("ARRAY[]::varchar[]");
            else {
                r.append(QString::fromUtf8("ARRAY["));
                while (str_lst.count() > 0) {
                    r.append(formatVariant(str_lst.takeFirst(), false, trimStrings));
                    if (str_lst.count() > 0)
                        r.append(QString::fromUtf8(", "));
                }
                r.append(QString::fromUtf8("]"));
            }
        } break;
        case QVariant::List:
        {
            QVariantList args = value.toList();

            int last_type = -1;

            if (args.count() > 0) {
                r.append(QString::fromUtf8("ARRAY["));
                while (args.count() > 0) {
                    QVariant t_val = args.takeFirst();

                    if (last_type < 0)
                        last_type = t_val.userType();
                    else if (last_type != t_val.userType()) {
                        qWarning("Diffefent types in bound list '%d' and '%d' in bound values", last_type, t_val.userType());
                    }

                    r.append(formatVariant(t_val, false, trimStrings));
                    if (args.count() > 0)
                        r.append(QString::fromUtf8(", "));
                }
                r.append(QString::fromUtf8("]"));
            } else {
                r = QString::fromUtf8("ARRAY[]");
            }
        } break;
        default:
            switch (value.userType()) {
            case QMetaType::Float: {
                assignSpecialPsqlFloatValue(value.toFloat(), &r);
                if (r.isEmpty()) {
                    QSqlField field(QString::fromUtf8(""), value.type());
                    field.setValue(value);
                    r = QSqlDriver::formatValue(field, trimStrings);
                }
                break;
            case QMetaType::QJsonDocument:
            case QMetaType::QJsonValue:
            case QMetaType::QJsonObject:
            case QMetaType::QJsonArray: {
                QJsonDocument jsondoc; {
                    switch (value.userType()) {
                    case QMetaType::QJsonDocument:
                        jsondoc = value.value<QJsonDocument>();
                        break;
                    case QMetaType::QJsonValue: {
                        const QJsonValue & val = refV<QJsonValue>(value);
                        if (val.isArray())
                            jsondoc = QJsonDocument(val.toArray());
                        else if (val.isObject())
                            jsondoc = QJsonDocument(val.toObject());
                        else {
                            r = formatVariant(val.toVariant(), isroot, trimStrings);
                            return r;
                        }
                        break;
                    }
                    case QMetaType::QJsonObject:
                        jsondoc = QJsonDocument(value.value<QJsonObject>());
                        break;
                    case QMetaType::QJsonArray:
                        jsondoc = QJsonDocument(value.value<QJsonArray>());
                        break;
                    }
                }
                r = QString::fromUtf8("'");
                {
                    QString data = QString::fromUtf8(jsondoc.toJson(QJsonDocument::Compact));
                    data.replace(QString::fromUtf8("'"),
                                 QString::fromUtf8("''"));
                    r += data;
                }
                r += QString::fromUtf8("'::json");
                return r;
            }
            default: {
                    QSqlField field(QString::fromUtf8(""), value.type());
                    field.setValue(value);
                    r = QSqlDriver::formatValue(field, trimStrings);
                }
            }
            }
            break;
        }
    }
    return r;
}

QString QSMPSQLDriver::escapeIdentifier(const QString &identifier, IdentifierType) const
{
    QString res = identifier;
    if(!identifier.isEmpty() && !identifier.startsWith(QLatin1Char('"')) && !identifier.endsWith(QLatin1Char('"')) ) {
        res.replace(QLatin1Char('"'), QLatin1String("\"\""));
        res.prepend(QLatin1Char('"')).append(QLatin1Char('"'));
        res.replace(QLatin1Char('.'), QLatin1String("\".\""));
    }
    return res;
}

bool QSMPSQLDriver::isOpen() const
{
    Q_D(const QSMPSQLDriver);
    return PQstatus(d->connection) == CONNECTION_OK;
}

QSMPSQLDriver::Protocol QSMPSQLDriver::protocol() const
{
    Q_D(const QSMPSQLDriver);
    return d->pro;
}

bool QSMPSQLDriver::subscribeToNotification(const QString &name)
{
    Q_D(QSMPSQLDriver);
    if (!isOpen()) {
        qWarning("QSMPSQLDriver::subscribeToNotificationImplementation: database not open.");
        return false;
    }

    if (d->seid.contains(name)) {
        qWarning("QSMPSQLDriver::subscribeToNotificationImplementation: already subscribing to '%s'.",
            qPrintable(name));
        return false;
    }

    int socket = PQsocket(d->connection);
    if (socket) {
        // Add the name to the list of subscriptions here so that QSQLDriverPrivate::exec knows
        // to check for notifications immediately after executing the LISTEN
        d->seid << name;
        QString query = QLatin1String("LISTEN ") + escapeIdentifier(name, QSqlDriver::TableName);
        PGresult *result = d->exec(query);
        if (PQresultStatus(result) != PGRES_COMMAND_OK) {
            d->seid.removeLast();
            setLastError(qMakeError(tr("Unable to subscribe"), QSqlError::StatementError, d, result));
            PQclear(result);
            return false;
        }
        PQclear(result);

        if (!d->sn) {
            d->sn = new QSocketNotifier(socket, QSocketNotifier::Read);
            connect(d->sn, SIGNAL(activated(int)), this, SLOT(_q_handleNotification(int)));
        }
    } else {
        qWarning("QSMPSQLDriver::subscribeToNotificationImplementation: PQsocket didn't return a valid socket to listen on");
        return false;
    }

    return true;
}

bool QSMPSQLDriver::unsubscribeFromNotification(const QString &name)
{
    Q_D(QSMPSQLDriver);
    if (!isOpen()) {
        qWarning("QSMPSQLDriver::unsubscribeFromNotificationImplementation: database not open.");
        return false;
    }

    if (!d->seid.contains(name)) {
        qWarning("QSMPSQLDriver::unsubscribeFromNotificationImplementation: not subscribed to '%s'.",
            qPrintable(name));
        return false;
    }

    QString query = QLatin1String("UNLISTEN ") + escapeIdentifier(name, QSqlDriver::TableName);
    PGresult *result = d->exec(query);
    if (PQresultStatus(result) != PGRES_COMMAND_OK) {
        setLastError(qMakeError(tr("Unable to unsubscribe"), QSqlError::StatementError, d, result));
        PQclear(result);
        return false;
    }
    PQclear(result);

    d->seid.removeAll(name);

    if (d->seid.isEmpty()) {
        disconnect(d->sn, SIGNAL(activated(int)), this, SLOT(_q_handleNotification(int)));
        delete d->sn;
        d->sn = 0;
    }

    return true;
}

QStringList QSMPSQLDriver::subscribedToNotifications() const
{
    Q_D(const QSMPSQLDriver);
    return d->seid;
}

void QSMPSQLDriver::_q_handleNotification(int)
{
    Q_D(QSMPSQLDriver);
    d->pendingNotifyCheck = false;
    PQconsumeInput(d->connection);

    PGnotify *notify = 0;
    while((notify = PQnotifies(d->connection)) != 0) {
        QString name(QLatin1String(notify->relname));
        if (d->seid.contains(name)) {
            QString payload;
#if defined PG_VERSION_NUM && PG_VERSION_NUM-0 >= 70400
            if (notify->extra)
                payload = d->isUtf8 ? QString::fromUtf8(notify->extra) : QString::fromLatin1(notify->extra);
#endif
            emit notification(name);
            QSqlDriver::NotificationSource source = (notify->be_pid == PQbackendPID(d->connection)) ? QSqlDriver::SelfSource : QSqlDriver::OtherSource;
            emit notification(name, source, payload);
        }
        else
            qWarning("QSMPSQLDriver: received notification for '%s' which isn't subscribed to.",
                    qPrintable(name));

        qPQfreemem(notify);
    }
}

QVariant QSMPSQLResult::processData(const QSMPSQLResult::DataType &info,
        const char *val,
        const int length)
{
    Q_D(const QSMPSQLResult);

    const char * begin = val;

    if (info.isArray) {

        if ((*begin != '{') ||
                (*(begin + length - 1) != '}')) {
            qWarning("QSMPSQLResult::data: Wrong array structure");
            return QVariant();
        }

        QVariantList res;

        int pos = 1;

        bool string_only = true;

        while (pos >= 0 && pos <= length - 1) {
            int next_pos = pos + findNextArrayPos(begin + pos, length - pos);
            DataType dtype = info;
            dtype.isArray = (*(begin + pos) == '{');

            const char * start = begin + pos;
            const int item_length = next_pos - pos;

            if (dtype.isArray) {
                res << processData(dtype, start, item_length);
            } else if (*start == '"') {
                QPair<char*, int> position = clearEscapesDoubleQouted(
                            start, item_length);
                res << processData(dtype, position.first, position.second);
                delete [] position.first;
            } /*else if (*start == '\'') {
                QPair<char*, int> position = clearEscapesSingleQouted(
                            start, item_length);
                res << processData(dtype, position.first, position.second);
                delete [] position.first;
            }*/ else {
                res << processData(dtype, start, item_length);
            }
            if (string_only && res.last().type() != QVariant::String)
                string_only = false;

            pos = next_pos + 1;
        }

        if (!string_only)
            return res;
        else {
            QStringList res_str;
            foreach(const QVariant & item, res)
                res_str << refV<QString>(item);
            return res_str;
        }
    }

    switch (info.variant) {
    case QVariant::Bool:
        return QVariant((bool)(begin[0] == 't'));
    case QVariant::String:
        return d->drv_d_func()->isUtf8 ? QString::fromUtf8(begin, length) : QString::fromLatin1(begin, length);
    case QVariant::LongLong:
        if (begin[0] == '-')
            return QString::fromLatin1(begin, length).toLongLong();
        else
            return QString::fromLatin1(begin, length).toULongLong();
    case QVariant::Int:
        return atoi(begin);
    case QVariant::Double: {
        if (info.ptype == QNUMERICOID) {
            if (numericalPrecisionPolicy() != QSql::HighPrecision) {
                QVariant retval;
                bool convert;
                double dbl=QString::fromLatin1(begin, length).toDouble(&convert);
                if (numericalPrecisionPolicy() == QSql::LowPrecisionInt64)
                    retval = (qlonglong)dbl;
                else if (numericalPrecisionPolicy() == QSql::LowPrecisionInt32)
                    retval = (int)dbl;
                else if (numericalPrecisionPolicy() == QSql::LowPrecisionDouble)
                    retval = dbl;
                if (!convert)
                    return QVariant();
                return retval;
            }
            return QString::fromLatin1(begin, length);
        }
        if (qstricmp(begin, "Infinity") == 0)
            return qInf();
        if (qstricmp(begin, "-Infinity") == 0)
            return -qInf();
        return QString::fromLatin1(begin, length).toDouble();
    }
    case QVariant::Date:
        if (begin[0] == '\0') {
            return QVariant(QDate());
        } else {
#ifndef QT_NO_DATESTRING
            return QVariant(QDate::fromString(QString::fromLatin1(begin, length), Qt::ISODate));
#else
            return QVariant(QString::fromLatin1(begin));
#endif
        }
    case QVariant::Time: {
        const QString str = QString::fromLatin1(begin, length);
#ifndef QT_NO_DATESTRING
        if (str.isEmpty())
            return QVariant(QTime());
        else
            return QVariant(QTime::fromString(str, Qt::ISODate));
#else
        return QVariant(str);
#endif
    }
    case QVariant::DateTime: {
        QString dtval = QString::fromLatin1(begin, length);
#ifndef QT_NO_DATESTRING
        if (dtval.length() < 10) {
            return QVariant(QDateTime());
       } else {
            QChar sign = dtval[dtval.size() - 3];
            if (sign == QLatin1Char('-') || sign == QLatin1Char('+')) dtval += QLatin1String(":00");
            return QVariant(QDateTime::fromString(dtval, Qt::ISODate).toLocalTime());
        }
#else
        return QVariant(dtval);
#endif
    }
    case QVariant::ByteArray: {
        size_t len;
        unsigned char *data = PQunescapeBytea((const unsigned char*)begin, &len);
        QByteArray ba(reinterpret_cast<const char *>(data), int(len));
        qPQfreemem(data);
        return QVariant(ba);
    }
    case QVariant::UserType:

        switch (info.metatype) {
        case QMetaType::QJsonDocument:{
            QByteArray ba(begin, length);
            return QVariant::fromValue(QJsonDocument::fromJson(ba));
        }
        default:
            qWarning("QSMPSQLResult::data: unknown data type");
            break;
        }

        break;
    case QVariant::Invalid:
        qWarning("QSMPSQLResult::data: unknown data type");
        break;
    default:
        qWarning("QSMPSQLResult::data: unknown data type");
        break;
    }
    return QVariant();
}

int QSMPSQLResult::findNextArrayPos(const char *begin, const int length)
{
    if (length == 0) return -2;

//    auto iter_single_quote = [](const char * begin, const int length) -> int
//    {
//        int pos = 1;

//        while (pos < length) {
//            const char * current = begin + pos;
//            ++pos;
//            switch (*current) {
//            case '\'':
//                if (*(current) == '\'')
//                    ++pos;
//                else
//                    return pos;
//            case '\\':
//                ++pos;
//            }
//        }

//        return pos;
//    };

    auto iter_double_quote = [](const char * begin, const int length) -> int
    {
        int pos = 1;

        while (pos < length) {
            const char * current = begin + pos;
            ++pos;
            switch (*current) {
            case '"':
                return pos;
            case '\\':
                ++pos;
            }
        }

        return pos;
    };

    auto iter_no_quote = [](const char * begin, const int length) -> int
    {
        int pos = 0;
        while (pos < length &&
               *(begin + pos) != ',' &&
               *(begin + pos) != ';' &&
               *(begin + pos) != '}')
            ++pos;

        return pos;
    };

//    if (*begin == '\'')
//        return iter_single_quote(begin, length);
    if (*begin == '"')
        return iter_double_quote(begin, length);

    return iter_no_quote(begin, length);
}

QPair<char*, int> QSMPSQLResult::clearEscapesSingleQouted(const char *begin, const int length)
{
    int esc_count = 0;
    for (int i = 1; i < length - 1; ++i) {
        const char * iter = begin + i;
        if (*iter == '\'' && *iter == '\\') {
            ++esc_count;
            ++i;
        }
    }

    const int new_length = length - esc_count - 2 + 1;
    char * res = new char[new_length];
    res[new_length] = '\0';
    int ind = 0;
    for (int i = 1; i < length - 1; ++i) {
        const char * iter = begin + i;
        if (*iter == '\'' || *iter == '\\')
            ++i;
        res[ind++] = *(begin + i);
    }

    return QPair<char*, int>(res, new_length - 1);
}

QPair<char*, int> QSMPSQLResult::clearEscapesDoubleQouted(const char *begin, const int length)
{
    int esc_count = 0;
    for (int i = 1; i < length - 1; ++i) {
        const char * iter = begin + i;
        if (*iter == '\\') {
            ++esc_count;
            ++i;
        }
    }

    const int new_length = length - esc_count - 2 + 1;
    char * res = new char[new_length];
    res[new_length - 1] = '\0';
    int ind = 0;
    for (int i = 1; i < length - 1; ++i) {
        const char * iter = begin + i;
        if (*iter == '\\')
            ++i;
        res[ind++] = *(begin + i);
    }

    return QPair<char*, int>(res, new_length - 1);
}

QT_END_NAMESPACE
