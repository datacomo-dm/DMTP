/****************************************************************************
** Meta object code from reading C++ file 'CMonitor.h'
**
** Created: Tue Sep 2 11:05:55 2008
**      by: The Qt Meta Object Compiler version 59 (Qt 4.3.2)
**
** WARNING! All changes made in this file will be lost!
*****************************************************************************/

#include "CMonitor.h"
#if !defined(Q_MOC_OUTPUT_REVISION)
#error "The header file 'CMonitor.h' doesn't include <QObject>."
#elif Q_MOC_OUTPUT_REVISION != 59
#error "This file was generated using the moc from 4.3.2. It"
#error "cannot be used with the include files from this version of Qt."
#error "(The moc has changed too much.)"
#endif

static const uint qt_meta_data_CMonitor[] = {

 // content:
       1,       // revision
       0,       // classname
       0,    0, // classinfo
       0,    0, // methods
       0,    0, // properties
       0,    0, // enums/sets

       0        // eod
};

static const char qt_meta_stringdata_CMonitor[] = {
    "CMonitor\0"
};

const QMetaObject CMonitor::staticMetaObject = {
    { &QTabWidget::staticMetaObject, qt_meta_stringdata_CMonitor,
      qt_meta_data_CMonitor, 0 }
};

const QMetaObject *CMonitor::metaObject() const
{
    return &staticMetaObject;
}

void *CMonitor::qt_metacast(const char *_clname)
{
    if (!_clname) return 0;
    if (!strcmp(_clname, qt_meta_stringdata_CMonitor))
	return static_cast<void*>(const_cast< CMonitor*>(this));
    return QTabWidget::qt_metacast(_clname);
}

int CMonitor::qt_metacall(QMetaObject::Call _c, int _id, void **_a)
{
    _id = QTabWidget::qt_metacall(_c, _id, _a);
    if (_id < 0)
        return _id;
    return _id;
}