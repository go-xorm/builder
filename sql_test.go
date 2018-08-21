// Copyright 2018 The Xorm Authors. All rights reserved.
// Use of this source code is governed by a BSD-style
// license that can be found in the LICENSE file.

package builder

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

const placeholderConverterSQL = "SELECT a, b FROM table_a WHERE b_id=(SELECT id FROM table_b WHERE b=?) AND id=? AND c=? AND d=? AND e=? AND f=?"
const placeholderConvertedSQL = "SELECT a, b FROM table_a WHERE b_id=(SELECT id FROM table_b WHERE b=$1) AND id=$2 AND c=$3 AND d=$4 AND e=$5 AND f=$6"
const placeholderBoundSQL = "SELECT a, b FROM table_a WHERE b_id=(SELECT id FROM table_b WHERE b=1) AND id=2.1 AND c='3' AND d=4 AND e='5' AND f=true"

func TestPlaceholderConverter(t *testing.T) {
	newSQL, err := ConvertPlaceholder(placeholderConverterSQL, "$")
	assert.NoError(t, err)
	assert.EqualValues(t, placeholderConvertedSQL, newSQL)
}

func BenchmarkPlaceholderConverter(b *testing.B) {
	for i := 0; i < b.N; i++ {
		ConvertPlaceholder(placeholderConverterSQL, "$")
	}
}

func TestBoundSQLConverter(t *testing.T) {
	newSQL, err := ConvertToBoundSQL(placeholderConverterSQL, []interface{}{1, 2.1, "3", 4, "5", true})
	assert.NoError(t, err)
	assert.EqualValues(t, placeholderBoundSQL, newSQL)

	newSQL, err = ConvertToBoundSQL(placeholderConverterSQL, []interface{}{1, 2.1, "3", 4, "5"})
	assert.Error(t, err)
	assert.EqualValues(t, ErrNeedMoreArguments, err)

	newSQL, err = ToBoundSQL(Select("id").From("table").Where(In("a", 1, 2)))
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT id FROM table WHERE a IN (1,2)", newSQL)

	newSQL, err = ToBoundSQL(1)
	assert.Error(t, err)
	assert.EqualValues(t, ErrNotSupportType, err)
}

func TestSQL(t *testing.T) {
	newSQL, args, err := ToSQL(In("a", 1, 2))
	assert.NoError(t, err)
	assert.EqualValues(t, "a IN (?,?)", newSQL)
	assert.EqualValues(t, []interface{}{1, 2}, args)

	newSQL, args, err = ToSQL(Select("id").From("table").Where(In("a", 1, 2)))
	assert.NoError(t, err)
	assert.EqualValues(t, "SELECT id FROM table WHERE a IN (?,?)", newSQL)
	assert.EqualValues(t, []interface{}{1, 2}, args)

	newSQL, args, err = ToSQL(1)
	assert.Error(t, err)
	assert.EqualValues(t, ErrNotSupportType, err)
}
