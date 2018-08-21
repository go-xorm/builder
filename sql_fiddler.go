// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"io/ioutil"
	"os"

	"github.com/go-xorm/sqlfiddle"
)

type fiddler struct {
	sessionCode string
	dbType      int
	f           *sqlfiddle.Fiddle
}

func readPreparationSQLFromFile(path string) (string, error) {
	file, err := os.Open(path)
	defer file.Close()
	if err != nil {
		return "", err
	}

	data, err := ioutil.ReadAll(file)
	if err != nil {
		return "", err
	}

	return string(data), nil
}

func newFiddler(fiddleServerAddr, dbDialect, preparationSQL string) (*fiddler, error) {
	var dbType int
	switch dbDialect {
	case MYSQL:
		dbType = sqlfiddle.Mysql5_6
	case MSSQL:
		dbType = sqlfiddle.MSSQL2017
	case POSTGRES:
		dbType = sqlfiddle.PostgreSQL96
	case ORACLE:
		dbType = sqlfiddle.Oracle11gR2
	case SQLITE:
		dbType = sqlfiddle.SQLite_WebSQL
	default:
		return nil, ErrNotSupportDialectType
	}

	f := sqlfiddle.NewFiddle(fiddleServerAddr)
	response, err := f.CreateSchema(dbType, preparationSQL)
	if err != nil {
		return nil, err
	}

	return &fiddler{sessionCode: response.Code, f: f, dbType: dbType}, nil
}

func (f *fiddler) executableCheck(obj interface{}) error {
	var sql string
	var err error
	switch obj.(type) {
	case *Builder:
		sql, err = obj.(*Builder).ToBindedSQL()
		if err != nil {
			return err
		}
	case string:
		sql = obj.(string)
	}

	_, err = f.f.RunSQL(f.dbType, f.sessionCode, sql)
	if err != nil {
		return err
	}

	return nil
}
