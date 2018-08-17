// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"fmt"
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestBuilder_Union(t *testing.T) {
	sql, args, err := Select("*").From("t1").Where(Eq{"status": "1"}).
		Union("all", Select("*").From("t2").Where(Eq{"status": "2"})).
		Union("distinct", Select("*").From("t2").Where(Eq{"status": "3"})).
		Union("", Select("*").From("t2").Where(Eq{"status": "3"})).
		ToSQL()
	assert.NoError(t, err)
	assert.EqualValues(t, []interface{}{"1", "2", "3", "3"}, args)
	fmt.Println(sql, args)

	// will raise error
	sql, args, err = Delete(Eq{"a": 1}).From("t1").
		Union("all", Select("*").From("t2").Where(Eq{"status": "2"})).ToSQL()
	assert.Error(t, err)
	fmt.Println(err)

	// will be overwrote by SELECT op
	sql, args, err = Select("*").From("t1").Where(Eq{"status": "1"}).
		Union("all", Select("*").From("t2").Where(Eq{"status": "2"})).
		Select("*").From("t2").Where(Eq{"status": "3"}).ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	// will be overwrote by DELETE op
	sql, args, err = Select("*").From("t1").Where(Eq{"status": "1"}).
		Union("all", Select("*").From("t2").Where(Eq{"status": "2"})).
		Delete(Eq{"status": "1"}).From("t2").ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)

	// will be overwrote by INSERT op
	sql, args, err = Select("*").From("t1").Where(Eq{"status": "1"}).
		Union("all", Select("*").From("t2").Where(Eq{"status": "2"})).
		Insert(Eq{"status": "1"}).From("t2").ToSQL()
	assert.NoError(t, err)
	fmt.Println(sql, args)
}
