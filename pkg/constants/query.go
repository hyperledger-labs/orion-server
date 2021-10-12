// Copyright IBM Corp. All Rights Reserved.
// SPDX-License-Identifier: Apache-2.0
package constants

const (
	// Combination operators
	QueryOpAnd = "$and"
	QueryOpOr  = "$or"

	// Logical operators
	QueryOpEqual              = "$eq"
	QueryOpNotEqual           = "$neq"
	QueryOpGreaterThan        = "$gt"
	QueryOpLesserThan         = "$lt"
	QueryOpGreaterThanOrEqual = "$gte"
	QueryOpLesserThanOrEqual  = "$lte"

	// Top-level fields allowed in the query
	QueryFieldSelector = "selector"
)
