// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestReadPreparationSQLFromFile(t *testing.T) {
	sqlFromFile, err := readPreparationSQLFromFile("testdata/mysql_fiddle_data.sql")
	assert.NoError(t, err)
	fmt.Println(sqlFromFile)
}

func TestNewFiddler(t *testing.T) {
	sqlFromFile, err := readPreparationSQLFromFile("testdata/mysql_fiddle_data.sql")
	assert.NoError(t, err)
	f, err := newFiddler("", MYSQL, sqlFromFile)
	assert.NoError(t, err)
	assert.NotEmpty(t, f.sessionCode)
}

func TestExecutableCheck(t *testing.T) {
	sqlFromFile, err := readPreparationSQLFromFile("testdata/mysql_fiddle_data.sql")
	assert.NoError(t, err)
	f, err := newFiddler("", MYSQL, sqlFromFile)
	assert.NoError(t, err)
	assert.NotEmpty(t, f.sessionCode)

	assert.NoError(t, f.executableCheck("SELECT * FROM table1"))

	err = f.executableCheck("SELECT * FROM table3")
	assert.Error(t, err)
	fmt.Println(err)
}
