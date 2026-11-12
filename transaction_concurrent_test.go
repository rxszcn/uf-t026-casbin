// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

package casbin

import (
	"context"
	"testing"
)

// TestConcurrentTransactionCommitMergesModels reproduces the lost-update bug:
// two transactions started from the same model snapshot commit different rules
// one after another. The rule committed by the first transaction must still be
// present after the second transaction commits, because the second transaction
// must merge its buffered operations into the live model instead of replacing
// it with its own stale snapshot.
func TestConcurrentTransactionCommitMergesModels(t *testing.T) {
	e := newTestTransactionalEnforcer(t)
	ctx := context.Background()

	tx1, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	if _, err = tx1.AddPolicy("alice", "data1", "read"); err != nil {
		t.Fatalf("Failed to add policy in transaction 1: %v", err)
	}

	// tx2 starts from the same snapshot, before tx1 commits.
	tx2, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}
	if _, err = tx2.AddPolicy("bob", "data2", "write"); err != nil {
		t.Fatalf("Failed to add policy in transaction 2: %v", err)
	}

	if err = tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}
	if err = tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	if !hasPolicy(t, e, "alice", "data1", "read") {
		t.Error("Policy added by the first committed transaction was lost after the second transaction committed")
	}
	if !hasPolicy(t, e, "bob", "data2", "write") {
		t.Error("Policy added by the second committed transaction is missing")
	}
}

// TestConcurrentTransactionCommitKeepsNormalAPIChanges ensures that changes made
// through the regular (non-transactional) management API while a transaction is
// in flight are not wiped out when that transaction commits with a stale
// snapshot.
func TestConcurrentTransactionCommitKeepsNormalAPIChanges(t *testing.T) {
	e := newTestTransactionalEnforcer(t)
	ctx := context.Background()

	tx, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction: %v", err)
	}
	if _, err = tx.AddPolicy("bob", "data2", "write"); err != nil {
		t.Fatalf("Failed to add policy in transaction: %v", err)
	}

	// A concurrent change goes through the ordinary management API.
	if _, err = e.AddPolicy("alice", "data1", "read"); err != nil {
		t.Fatalf("Failed to add policy through the normal API: %v", err)
	}

	if err = tx.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction: %v", err)
	}

	if !hasPolicy(t, e, "alice", "data1", "read") {
		t.Error("Policy added through the normal API was lost when the transaction committed")
	}
	if !hasPolicy(t, e, "bob", "data2", "write") {
		t.Error("Policy added by the transaction is missing")
	}
}

// TestConcurrentTransactionCommitMergesRemoveAndUpdate covers remove/update
// operations: a rule removed by an earlier committed transaction must stay
// removed, and an update made by a later transaction must be visible while the
// earlier transaction's independent change survives.
func TestConcurrentTransactionCommitMergesRemoveAndUpdate(t *testing.T) {
	e := newTestTransactionalEnforcer(t)
	ctx := context.Background()

	// Seed the model with three rules.
	if err := e.WithTransaction(ctx, func(tx *Transaction) error {
		_, err := tx.AddPolicies([][]string{
			{"alice", "data1", "read"},
			{"bob", "data2", "write"},
			{"carol", "data3", "read"},
		})
		return err
	}); err != nil {
		t.Fatalf("Failed to seed policies: %v", err)
	}

	tx1, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	if _, err = tx1.RemovePolicy("alice", "data1", "read"); err != nil {
		t.Fatalf("Failed to buffer removal in transaction 1: %v", err)
	}

	tx2, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}
	if _, err = tx2.UpdatePolicy(
		[]string{"bob", "data2", "write"},
		[]string{"bob", "data2", "read"},
	); err != nil {
		t.Fatalf("Failed to buffer update in transaction 2: %v", err)
	}

	if err = tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}
	if err = tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	if hasPolicy(t, e, "alice", "data1", "read") {
		t.Error("Rule removed by the first transaction should stay removed after the second commit")
	}
	if hasPolicy(t, e, "bob", "data2", "write") {
		t.Error("The old rule of the update performed by the second transaction should be gone")
	}
	if !hasPolicy(t, e, "bob", "data2", "read") {
		t.Error("The new rule of the update performed by the second transaction should be present")
	}
	if !hasPolicy(t, e, "carol", "data3", "read") {
		t.Error("An untouched rule should survive both commits")
	}
}

// TestConcurrentTransactionCommitMergesGroupingPolicies makes sure grouping
// policy changes from concurrently committed transactions are merged as well,
// and that role links end up reflecting both sets of rules.
func TestConcurrentTransactionCommitMergesGroupingPolicies(t *testing.T) {
	e := newTestTransactionalEnforcer(t)
	ctx := context.Background()

	if err := e.WithTransaction(ctx, func(tx *Transaction) error {
		_, err := tx.AddPolicy("admin", "data1", "read")
		return err
	}); err != nil {
		t.Fatalf("Failed to seed admin policy: %v", err)
	}

	tx1, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	if _, err = tx1.AddGroupingPolicy("alice", "admin"); err != nil {
		t.Fatalf("Failed to buffer grouping policy in transaction 1: %v", err)
	}

	tx2, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}
	if _, err = tx2.AddGroupingPolicy("bob", "admin"); err != nil {
		t.Fatalf("Failed to buffer grouping policy in transaction 2: %v", err)
	}

	if err = tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}
	if err = tx2.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 2: %v", err)
	}

	if !hasGroupingPolicy(t, e, "alice", "admin") {
		t.Error("Grouping policy added by the first transaction was lost after the second commit")
	}
	if !hasGroupingPolicy(t, e, "bob", "admin") {
		t.Error("Grouping policy added by the second transaction is missing")
	}

	for _, subject := range []string{"alice", "bob"} {
		allowed, enforceErr := e.Enforce(subject, "data1", "read")
		if enforceErr != nil || !allowed {
			t.Errorf("%s should inherit the admin permission after both commits, got allowed=%v err=%v", subject, allowed, enforceErr)
		}
	}
}

// TestConcurrentTransactionRollbackKeepsCommittedModel verifies that rolling a
// transaction back never disturbs changes already committed by another
// transaction.
func TestConcurrentTransactionRollbackKeepsCommittedModel(t *testing.T) {
	e := newTestTransactionalEnforcer(t)
	ctx := context.Background()

	tx1, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 1: %v", err)
	}
	if _, err = tx1.AddPolicy("alice", "data1", "read"); err != nil {
		t.Fatalf("Failed to add policy in transaction 1: %v", err)
	}

	tx2, err := e.BeginTransaction(ctx)
	if err != nil {
		t.Fatalf("Failed to begin transaction 2: %v", err)
	}
	if _, err = tx2.AddPolicy("bob", "data2", "write"); err != nil {
		t.Fatalf("Failed to add policy in transaction 2: %v", err)
	}

	if err = tx1.Commit(); err != nil {
		t.Fatalf("Failed to commit transaction 1: %v", err)
	}
	if err = tx2.Rollback(); err != nil {
		t.Fatalf("Failed to roll back transaction 2: %v", err)
	}

	if !hasPolicy(t, e, "alice", "data1", "read") {
		t.Error("The committed transaction's policy must survive the rollback of the other transaction")
	}
	if hasPolicy(t, e, "bob", "data2", "write") {
		t.Error("The rolled back transaction's policy should never reach the model")
	}
}
