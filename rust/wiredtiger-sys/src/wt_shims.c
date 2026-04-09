/*
 * C shim functions for WiredTiger variadic vtable calls.
 *
 * On ARM64 (Apple Silicon), variadic functions use a different calling
 * convention from non-variadic ones.  Rust cannot directly call variadic
 * function pointers, so we provide thin C wrappers that do the correct
 * variadic dispatch.
 */

#include <stddef.h>  /* size_t */
#include <stdint.h>  /* uint32_t */

/* Minimal forward declarations -- we never dereference these, just pass
 * opaque pointers through. */
typedef struct __wt_cursor WT_CURSOR;

typedef struct {
    const void *data;
    size_t      size;
    void       *mem;
    size_t      memsize;
    uint32_t    flags;
} WT_ITEM;

/* Variadic function pointer types matching the WT vtable signatures. */
typedef void (*wt_set_key_fn)(WT_CURSOR *, ...);
typedef void (*wt_set_value_fn)(WT_CURSOR *, ...);
typedef int  (*wt_get_key_fn)(WT_CURSOR *, ...);
typedef int  (*wt_get_value_fn)(WT_CURSOR *, ...);

/* ---- set_key wrappers ---- */

void wt_shim_set_key_str(void *fn, WT_CURSOR *cursor, const char *key) {
    ((wt_set_key_fn)fn)(cursor, key);
}

void wt_shim_set_key_raw(void *fn, WT_CURSOR *cursor, WT_ITEM *item) {
    ((wt_set_key_fn)fn)(cursor, item);
}

/* ---- set_value wrappers ---- */

void wt_shim_set_value_raw(void *fn, WT_CURSOR *cursor, WT_ITEM *item) {
    ((wt_set_value_fn)fn)(cursor, item);
}

void wt_shim_set_value_str(void *fn, WT_CURSOR *cursor, const char *value) {
    ((wt_set_value_fn)fn)(cursor, value);
}

/* ---- get_key wrappers ---- */

int wt_shim_get_key_str(void *fn, WT_CURSOR *cursor, const char **keyp) {
    return ((wt_get_key_fn)fn)(cursor, keyp);
}

int wt_shim_get_key_raw(void *fn, WT_CURSOR *cursor, WT_ITEM *keyp) {
    return ((wt_get_key_fn)fn)(cursor, keyp);
}

/* ---- get_value wrappers ---- */

int wt_shim_get_value_raw(void *fn, WT_CURSOR *cursor, WT_ITEM *valuep) {
    return ((wt_get_value_fn)fn)(cursor, valuep);
}

int wt_shim_get_value_str(void *fn, WT_CURSOR *cursor, const char **valp) {
    return ((wt_get_value_fn)fn)(cursor, valp);
}

/* Statistics cursor value format "SSq": description, name, int64 value */
int wt_shim_get_value_ssq(void *fn, WT_CURSOR *cursor,
                          const char **descp, const char **namep, int64_t *valp) {
    return ((wt_get_value_fn)fn)(cursor, descp, namep, valp);
}
