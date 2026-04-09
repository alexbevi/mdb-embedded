/*
 * test_ffi.c — Smoke test for the smongo C ABI.
 *
 * Exercises: open, collection, insert_one, find_one, find (cursor),
 * count, update_one, delete_one, create_index, aggregate,
 * find_one (no match → SMONGO_ERROR_NOT_FOUND),
 * session transactions (begin/commit/rollback, multi-collection,
 * session_insert_one, session_find, session_find_one, session_delete_one),
 * TTL indexes (create_index_with_ttl, reap_ttl), close.
 *
 * BSON documents are built manually using the wire format:
 *   int32 (total size LE) | e_list | 0x00
 *   e_list = element*
 *   element = type(1 byte) | cstring key | value
 *
 * Compile (macOS, from repo root):
 *   cargo build -p smongo-c
 *   clang -o test_ffi rust/smongo-c/tests/test_ffi.c \
 *         -L rust/target/debug -lsmongo_c \
 *         -Irust/smongo-c
 *   ./test_ffi
 */

#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>
#include <assert.h>
#include "smongo.h"

/* ------------------------------------------------------------------ */
/* Minimal BSON builder (enough for this test)                        */
/* ------------------------------------------------------------------ */

typedef struct {
    uint8_t *buf;
    size_t   len;
    size_t   cap;
} BsonBuf;

static void bb_init(BsonBuf *b) {
    b->cap = 256;
    b->buf = (uint8_t *)malloc(b->cap);
    b->len = 4; /* reserve space for the total-size prefix */
}

static void bb_ensure(BsonBuf *b, size_t extra) {
    while (b->len + extra > b->cap) {
        b->cap *= 2;
        b->buf = (uint8_t *)realloc(b->buf, b->cap);
    }
}

static void bb_raw(BsonBuf *b, const void *src, size_t n) {
    bb_ensure(b, n);
    memcpy(b->buf + b->len, src, n);
    b->len += n;
}

static void bb_byte(BsonBuf *b, uint8_t v) { bb_raw(b, &v, 1); }

static void bb_i32(BsonBuf *b, int32_t v) { bb_raw(b, &v, 4); }

static void bb_cstring(BsonBuf *b, const char *s) {
    size_t n = strlen(s) + 1;
    bb_raw(b, s, n);
}

/* Append a UTF-8 string element. */
static void bb_string(BsonBuf *b, const char *key, const char *val) {
    bb_byte(b, 0x02); /* type string */
    bb_cstring(b, key);
    int32_t slen = (int32_t)strlen(val) + 1;
    bb_i32(b, slen);
    bb_raw(b, val, (size_t)slen);
}

/* Append an int32 element. */
static void bb_int32(BsonBuf *b, const char *key, int32_t val) {
    bb_byte(b, 0x10); /* type int32 */
    bb_cstring(b, key);
    bb_i32(b, val);
}

/* Append a sub-document element. */
static void bb_subdoc(BsonBuf *b, const char *key, const uint8_t *doc, size_t doc_len) {
    bb_byte(b, 0x03); /* type document */
    bb_cstring(b, key);
    bb_raw(b, doc, doc_len);
}

/* Finalise: write total size and trailing NUL. */
static void bb_finish(BsonBuf *b) {
    bb_byte(b, 0x00); /* terminator */
    int32_t total = (int32_t)b->len;
    memcpy(b->buf, &total, 4);
}

static void bb_free(BsonBuf *b) { free(b->buf); b->buf = NULL; b->len = 0; }

/* Build an empty BSON document {} (5 bytes). */
static void build_empty_doc(BsonBuf *b) {
    bb_init(b);
    bb_finish(b);
}

/* ------------------------------------------------------------------ */
/* Test helpers                                                       */
/* ------------------------------------------------------------------ */

#define CHECK(rc, msg) do { \
    if ((rc) != SMONGO_OK) { \
        const char *err = smongo_last_error(); \
        fprintf(stderr, "FAIL: %s (rc=%d): %s\n", msg, rc, err ? err : "(null)"); \
        exit(1); \
    } \
} while (0)

static const char *TEST_DIR = "/tmp/smongo_c_test";

static void cleanup_test_dir(void) {
    char cmd[256];
    snprintf(cmd, sizeof(cmd), "rm -rf %s", TEST_DIR);
    (void)system(cmd);
}

/* ------------------------------------------------------------------ */
/* Main test                                                          */
/* ------------------------------------------------------------------ */

int main(void) {
    int32_t rc;
    cleanup_test_dir();

    printf("=== smongo C ABI test ===\n\n");

    /* 1. Open database */
    printf("1. Opening database at %s ...\n", TEST_DIR);
    SmongoDb *db = NULL;
    rc = smongo_open(TEST_DIR, &db);
    CHECK(rc, "smongo_open");
    printf("   OK — database opened.\n\n");

    /* 2. Get collection */
    printf("2. Getting collection 'users' ...\n");
    SmongoCollection *col = NULL;
    rc = smongo_collection(db, "users", &col);
    CHECK(rc, "smongo_collection");
    printf("   OK — collection handle obtained.\n\n");

    /* 3. Insert a document: {"name": "Alice", "age": 30} */
    printf("3. Inserting document {name: 'Alice', age: 30} ...\n");
    BsonBuf doc1;
    bb_init(&doc1);
    bb_string(&doc1, "name", "Alice");
    bb_int32(&doc1, "age", 30);
    bb_finish(&doc1);

    uint8_t *insert_result = NULL;
    size_t insert_result_len = 0;
    rc = smongo_insert_one(col, doc1.buf, doc1.len, &insert_result, &insert_result_len);
    CHECK(rc, "smongo_insert_one");
    printf("   OK — inserted (%zu bytes result).\n", insert_result_len);
    smongo_free(insert_result, insert_result_len);
    bb_free(&doc1);

    /* 4. Insert another: {"name": "Bob", "age": 25} */
    printf("4. Inserting document {name: 'Bob', age: 25} ...\n");
    BsonBuf doc2;
    bb_init(&doc2);
    bb_string(&doc2, "name", "Bob");
    bb_int32(&doc2, "age", 25);
    bb_finish(&doc2);

    rc = smongo_insert_one(col, doc2.buf, doc2.len, &insert_result, &insert_result_len);
    CHECK(rc, "smongo_insert_one (Bob)");
    printf("   OK — inserted.\n\n");
    smongo_free(insert_result, insert_result_len);
    bb_free(&doc2);

    /* 5. Count all documents */
    printf("5. Counting all documents ...\n");
    int64_t count = 0;
    rc = smongo_count(col, NULL, 0, &count);
    CHECK(rc, "smongo_count");
    printf("   OK — count = %lld\n\n", (long long)count);
    assert(count == 2);

    /* 6. Find one: {"name": "Alice"} */
    printf("6. Find one where name='Alice' ...\n");
    BsonBuf filter1;
    bb_init(&filter1);
    bb_string(&filter1, "name", "Alice");
    bb_finish(&filter1);

    uint8_t *found = NULL;
    size_t found_len = 0;
    rc = smongo_find_one(col, filter1.buf, filter1.len, &found, &found_len);
    CHECK(rc, "smongo_find_one");
    if (found != NULL) {
        printf("   OK — found document (%zu bytes).\n\n", found_len);
        smongo_free(found, found_len);
    } else {
        fprintf(stderr, "FAIL: expected to find Alice, got NULL\n");
        exit(1);
    }
    bb_free(&filter1);

    /* 7. Find all with cursor (empty filter = all docs) */
    printf("7. Finding all documents via cursor ...\n");
    BsonBuf empty;
    build_empty_doc(&empty);

    SmongoCursor *cursor = NULL;
    rc = smongo_find(col, empty.buf, empty.len, &cursor);
    CHECK(rc, "smongo_find");

    int cursor_count = 0;
    const uint8_t *cursor_doc = NULL;
    size_t cursor_doc_len = 0;
    while (smongo_cursor_next(cursor, &cursor_doc, &cursor_doc_len) == SMONGO_OK) {
        cursor_count++;
        printf("   Document %d: %zu bytes\n", cursor_count, cursor_doc_len);
    }
    smongo_cursor_free(cursor);
    printf("   OK — iterated %d documents.\n\n", cursor_count);
    assert(cursor_count == 2);
    bb_free(&empty);

    /* 8. Update: set Alice's age to 31 */
    printf("8. Updating Alice's age to 31 ...\n");
    BsonBuf uf;
    bb_init(&uf);
    bb_string(&uf, "name", "Alice");
    bb_finish(&uf);

    /* Build update doc: {"$set": {"age": 31}} */
    BsonBuf age_doc;
    bb_init(&age_doc);
    bb_int32(&age_doc, "age", 31);
    bb_finish(&age_doc);

    BsonBuf update;
    bb_init(&update);
    bb_subdoc(&update, "$set", age_doc.buf, age_doc.len);
    bb_finish(&update);

    uint8_t *update_result = NULL;
    size_t update_result_len = 0;
    rc = smongo_update_one(col, uf.buf, uf.len, update.buf, update.len,
                           &update_result, &update_result_len);
    CHECK(rc, "smongo_update_one");
    printf("   OK — update result (%zu bytes).\n\n", update_result_len);
    smongo_free(update_result, update_result_len);
    bb_free(&uf);
    bb_free(&age_doc);
    bb_free(&update);

    /* 9. Delete Bob */
    printf("9. Deleting Bob ...\n");
    BsonBuf df;
    bb_init(&df);
    bb_string(&df, "name", "Bob");
    bb_finish(&df);

    uint8_t *delete_result = NULL;
    size_t delete_result_len = 0;
    rc = smongo_delete_one(col, df.buf, df.len, &delete_result, &delete_result_len);
    CHECK(rc, "smongo_delete_one");
    printf("   OK — delete result (%zu bytes).\n\n", delete_result_len);
    smongo_free(delete_result, delete_result_len);
    bb_free(&df);

    /* 10. Verify count is now 1 */
    printf("10. Verifying count is 1 ...\n");
    count = 0;
    rc = smongo_count(col, NULL, 0, &count);
    CHECK(rc, "smongo_count after delete");
    printf("    OK — count = %lld\n\n", (long long)count);
    assert(count == 1);

    /* 11. Create index on {"age": 1} */
    printf("11. Creating index on {age: 1} ...\n");
    BsonBuf idx_keys;
    bb_init(&idx_keys);
    bb_int32(&idx_keys, "age", 1);
    bb_finish(&idx_keys);

    uint8_t *idx_result = NULL;
    size_t idx_result_len = 0;
    rc = smongo_create_index(col, idx_keys.buf, idx_keys.len,
                             NULL, 0, &idx_result, &idx_result_len);
    CHECK(rc, "smongo_create_index");
    printf("    OK — index created (%zu bytes result).\n\n", idx_result_len);
    smongo_free(idx_result, idx_result_len);
    bb_free(&idx_keys);

    /* 12. Find one with no match → SMONGO_ERROR_NOT_FOUND */
    printf("12. Find one where name='Nobody' (expect NOT_FOUND) ...\n");
    BsonBuf filter_nobody;
    bb_init(&filter_nobody);
    bb_string(&filter_nobody, "name", "Nobody");
    bb_finish(&filter_nobody);

    uint8_t *nobody = NULL;
    size_t nobody_len = 0;
    rc = smongo_find_one(col, filter_nobody.buf, filter_nobody.len,
                         &nobody, &nobody_len);
    assert(rc == SMONGO_ERROR_NOT_FOUND);
    assert(nobody == NULL);
    assert(nobody_len == 0);
    printf("    OK — got SMONGO_ERROR_NOT_FOUND as expected.\n\n");
    bb_free(&filter_nobody);

    /* 13. Insert more docs for aggregation: Bob(25), Charlie(35) */
    printf("13. Inserting Bob(25) and Charlie(35) for aggregation ...\n");
    BsonBuf doc_bob;
    bb_init(&doc_bob);
    bb_string(&doc_bob, "name", "Bob");
    bb_int32(&doc_bob, "age", 25);
    bb_finish(&doc_bob);

    rc = smongo_insert_one(col, doc_bob.buf, doc_bob.len,
                           &insert_result, &insert_result_len);
    CHECK(rc, "smongo_insert_one (Bob)");
    smongo_free(insert_result, insert_result_len);
    bb_free(&doc_bob);

    BsonBuf doc_charlie;
    bb_init(&doc_charlie);
    bb_string(&doc_charlie, "name", "Charlie");
    bb_int32(&doc_charlie, "age", 35);
    bb_finish(&doc_charlie);

    rc = smongo_insert_one(col, doc_charlie.buf, doc_charlie.len,
                           &insert_result, &insert_result_len);
    CHECK(rc, "smongo_insert_one (Charlie)");
    smongo_free(insert_result, insert_result_len);
    bb_free(&doc_charlie);
    printf("    OK — 3 documents total.\n\n");

    /* 14. Aggregation: $match {age: {$gte: 30}} then $sort {age: 1}
     *
     * Pipeline is a BSON "array document" with keys "0", "1", ...
     * Stage 0: {"$match": {"age": {"$gte": 30}}}
     * Stage 1: {"$sort":  {"age": 1}}
     *
     * Expected results: Alice(31), Charlie(35) in ascending age order.
     */
    printf("14. Aggregation: $match age>=30, $sort age asc ...\n");

    /* Build inner: {"$gte": 30} */
    BsonBuf gte_doc;
    bb_init(&gte_doc);
    bb_int32(&gte_doc, "$gte", 30);
    bb_finish(&gte_doc);

    /* Build: {"age": {"$gte": 30}} */
    BsonBuf match_filter;
    bb_init(&match_filter);
    bb_subdoc(&match_filter, "age", gte_doc.buf, gte_doc.len);
    bb_finish(&match_filter);

    /* Build stage 0: {"$match": {"age": {"$gte": 30}}} */
    BsonBuf stage0;
    bb_init(&stage0);
    bb_subdoc(&stage0, "$match", match_filter.buf, match_filter.len);
    bb_finish(&stage0);

    /* Build: {"age": 1} */
    BsonBuf sort_spec;
    bb_init(&sort_spec);
    bb_int32(&sort_spec, "age", 1);
    bb_finish(&sort_spec);

    /* Build stage 1: {"$sort": {"age": 1}} */
    BsonBuf stage1;
    bb_init(&stage1);
    bb_subdoc(&stage1, "$sort", sort_spec.buf, sort_spec.len);
    bb_finish(&stage1);

    /* Build pipeline array doc: {"0": stage0, "1": stage1} */
    BsonBuf pipeline;
    bb_init(&pipeline);
    bb_subdoc(&pipeline, "0", stage0.buf, stage0.len);
    bb_subdoc(&pipeline, "1", stage1.buf, stage1.len);
    bb_finish(&pipeline);

    SmongoCursor *agg_cursor = NULL;
    rc = smongo_aggregate(col, pipeline.buf, pipeline.len, &agg_cursor);
    CHECK(rc, "smongo_aggregate");

    int agg_count = 0;
    const uint8_t *agg_doc = NULL;
    size_t agg_doc_len = 0;
    while (smongo_cursor_next(agg_cursor, &agg_doc, &agg_doc_len) == SMONGO_OK) {
        agg_count++;
        printf("    Aggregation result %d: %zu bytes\n", agg_count, agg_doc_len);
    }
    smongo_cursor_free(agg_cursor);
    printf("    OK — aggregation returned %d documents (expected 2).\n\n", agg_count);
    assert(agg_count == 2);

    bb_free(&gte_doc);
    bb_free(&match_filter);
    bb_free(&stage0);
    bb_free(&sort_spec);
    bb_free(&stage1);
    bb_free(&pipeline);

    /* ================================================================ */
    /* Session-based transactions                                       */
    /* ================================================================ */

    /* 15. Start a session */
    printf("15. Starting session ...\n");
    SmongoSession *session = NULL;
    rc = smongo_start_session(db, &session);
    CHECK(rc, "smongo_start_session");
    printf("    OK — session created.\n\n");

    /* 16. Multi-collection transaction — commit */
    printf("16. Session transaction commit across two collections ...\n");
    rc = smongo_session_begin_transaction(session);
    CHECK(rc, "smongo_session_begin_transaction");

    BsonBuf txn_doc_a;
    bb_init(&txn_doc_a);
    bb_string(&txn_doc_a, "account", "Alice");
    bb_int32(&txn_doc_a, "balance", 500);
    bb_finish(&txn_doc_a);

    uint8_t *txn_result = NULL;
    size_t txn_result_len = 0;
    rc = smongo_session_insert_one(session, "txn_accounts", txn_doc_a.buf, txn_doc_a.len,
                                   &txn_result, &txn_result_len);
    CHECK(rc, "smongo_session_insert_one (txn_accounts)");
    smongo_free(txn_result, txn_result_len);
    bb_free(&txn_doc_a);

    BsonBuf txn_doc_b;
    bb_init(&txn_doc_b);
    bb_string(&txn_doc_b, "action", "create_account");
    bb_string(&txn_doc_b, "who", "Alice");
    bb_finish(&txn_doc_b);

    rc = smongo_session_insert_one(session, "txn_ledger", txn_doc_b.buf, txn_doc_b.len,
                                   &txn_result, &txn_result_len);
    CHECK(rc, "smongo_session_insert_one (txn_ledger)");
    smongo_free(txn_result, txn_result_len);
    bb_free(&txn_doc_b);

    rc = smongo_session_commit_transaction(session);
    CHECK(rc, "smongo_session_commit_transaction");

    /* Verify both collections have the committed data */
    SmongoCollection *txn_col_a = NULL;
    rc = smongo_collection(db, "txn_accounts", &txn_col_a);
    CHECK(rc, "smongo_collection (txn_accounts)");
    int64_t txn_count = 0;
    rc = smongo_count(txn_col_a, NULL, 0, &txn_count);
    CHECK(rc, "smongo_count txn_accounts");
    assert(txn_count == 1);
    smongo_collection_free(txn_col_a);

    SmongoCollection *txn_col_b = NULL;
    rc = smongo_collection(db, "txn_ledger", &txn_col_b);
    CHECK(rc, "smongo_collection (txn_ledger)");
    txn_count = 0;
    rc = smongo_count(txn_col_b, NULL, 0, &txn_count);
    CHECK(rc, "smongo_count txn_ledger");
    assert(txn_count == 1);
    smongo_collection_free(txn_col_b);
    printf("    OK — committed data visible in both collections.\n\n");

    /* 17. Multi-collection transaction — rollback */
    printf("17. Session transaction rollback ...\n");
    rc = smongo_session_begin_transaction(session);
    CHECK(rc, "smongo_session_begin_transaction (rollback)");

    BsonBuf txn_doc_rb;
    bb_init(&txn_doc_rb);
    bb_string(&txn_doc_rb, "should_vanish", "yes");
    bb_finish(&txn_doc_rb);

    rc = smongo_session_insert_one(session, "txn_rollback", txn_doc_rb.buf, txn_doc_rb.len,
                                   &txn_result, &txn_result_len);
    CHECK(rc, "smongo_session_insert_one (txn_rollback)");
    smongo_free(txn_result, txn_result_len);
    bb_free(&txn_doc_rb);

    rc = smongo_session_rollback_transaction(session);
    CHECK(rc, "smongo_session_rollback_transaction");

    SmongoCollection *txn_col_rb = NULL;
    rc = smongo_collection(db, "txn_rollback", &txn_col_rb);
    CHECK(rc, "smongo_collection (txn_rollback)");
    txn_count = 0;
    rc = smongo_count(txn_col_rb, NULL, 0, &txn_count);
    CHECK(rc, "smongo_count txn_rollback");
    assert(txn_count == 0);
    smongo_collection_free(txn_col_rb);
    printf("    OK — rolled-back data is not visible.\n\n");

    /* 18. Session find / find_one / delete_one */
    printf("18. Session find, find_one, delete_one ...\n");
    rc = smongo_session_begin_transaction(session);
    CHECK(rc, "smongo_session_begin_transaction (find test)");

    BsonBuf sess_doc;
    bb_init(&sess_doc);
    bb_string(&sess_doc, "item", "widget");
    bb_int32(&sess_doc, "qty", 42);
    bb_finish(&sess_doc);

    rc = smongo_session_insert_one(session, "sess_crud", sess_doc.buf, sess_doc.len,
                                   &txn_result, &txn_result_len);
    CHECK(rc, "smongo_session_insert_one (sess_crud)");
    smongo_free(txn_result, txn_result_len);
    bb_free(&sess_doc);

    /* session find — should yield 1 document */
    BsonBuf sess_empty;
    build_empty_doc(&sess_empty);

    SmongoCursor *sess_cursor = NULL;
    rc = smongo_session_find(session, "sess_crud", sess_empty.buf, sess_empty.len, &sess_cursor);
    CHECK(rc, "smongo_session_find");

    int sess_cursor_count = 0;
    const uint8_t *sess_cur_doc = NULL;
    size_t sess_cur_doc_len = 0;
    while (smongo_cursor_next(sess_cursor, &sess_cur_doc, &sess_cur_doc_len) == SMONGO_OK) {
        sess_cursor_count++;
    }
    smongo_cursor_free(sess_cursor);
    assert(sess_cursor_count == 1);
    printf("    session find: %d documents (expected 1).\n", sess_cursor_count);

    /* session find_one */
    BsonBuf sess_filter;
    bb_init(&sess_filter);
    bb_string(&sess_filter, "item", "widget");
    bb_finish(&sess_filter);

    uint8_t *sess_found = NULL;
    size_t sess_found_len = 0;
    rc = smongo_session_find_one(session, "sess_crud", sess_filter.buf, sess_filter.len,
                                 &sess_found, &sess_found_len);
    CHECK(rc, "smongo_session_find_one");
    assert(sess_found != NULL && sess_found_len > 0);
    smongo_free(sess_found, sess_found_len);
    printf("    session find_one: found (%zu bytes).\n", sess_found_len);

    /* session delete_one */
    uint8_t *sess_del_result = NULL;
    size_t sess_del_result_len = 0;
    rc = smongo_session_delete_one(session, "sess_crud", sess_filter.buf, sess_filter.len,
                                   &sess_del_result, &sess_del_result_len);
    CHECK(rc, "smongo_session_delete_one");
    smongo_free(sess_del_result, sess_del_result_len);
    bb_free(&sess_filter);
    bb_free(&sess_empty);

    rc = smongo_session_commit_transaction(session);
    CHECK(rc, "smongo_session_commit_transaction (find test)");

    /* Verify the delete took effect */
    SmongoCollection *sess_col = NULL;
    rc = smongo_collection(db, "sess_crud", &sess_col);
    CHECK(rc, "smongo_collection (sess_crud)");
    txn_count = 0;
    rc = smongo_count(sess_col, NULL, 0, &txn_count);
    CHECK(rc, "smongo_count sess_crud");
    assert(txn_count == 0);
    smongo_collection_free(sess_col);
    printf("    OK — session find/find_one/delete_one all work.\n\n");

    /* ================================================================ */
    /* TTL index + reap                                                 */
    /* ================================================================ */

    /* 19. Create a TTL index on a fresh collection */
    printf("19. Creating TTL index (expire_after_seconds=3600) ...\n");
    SmongoCollection *ttl_col = NULL;
    rc = smongo_collection(db, "ttl_test", &ttl_col);
    CHECK(rc, "smongo_collection (ttl_test)");

    BsonBuf ttl_keys;
    bb_init(&ttl_keys);
    bb_int32(&ttl_keys, "createdAt", 1);
    bb_finish(&ttl_keys);

    uint8_t *ttl_result = NULL;
    size_t ttl_result_len = 0;
    rc = smongo_create_index_with_ttl(ttl_col, ttl_keys.buf, ttl_keys.len,
                                      0, 3600, &ttl_result, &ttl_result_len);
    CHECK(rc, "smongo_create_index_with_ttl");
    printf("    OK — TTL index created (%zu bytes result).\n", ttl_result_len);
    smongo_free(ttl_result, ttl_result_len);
    bb_free(&ttl_keys);
    smongo_collection_free(ttl_col);

    /* 20. Reap TTL — nothing expired (empty collection) */
    printf("20. Reaping TTL (expect 0 expired) ...\n");
    int64_t reap_count = 0;
    rc = smongo_reap_ttl(db, &reap_count);
    CHECK(rc, "smongo_reap_ttl");
    printf("    OK — reaped %lld documents (expected 0).\n\n", (long long)reap_count);
    assert(reap_count == 0);

    /* Free session */
    smongo_session_free(session);

    /* Cleanup */
    printf("21. Cleaning up ...\n");
    smongo_collection_free(col);
    smongo_close(db);
    cleanup_test_dir();
    printf("    OK — database closed and test directory removed.\n\n");

    printf("=== ALL TESTS PASSED ===\n");
    return 0;
}
