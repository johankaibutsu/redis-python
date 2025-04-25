import pytest
import time
import os
from redis_store import PyRedisStore

@pytest.fixture
def store():
    """Provides a clean PyRedisStore instance for each test."""
    dump_file = "pyredis_dump.pkl"
    if os.path.exists(dump_file):
        try:
            os.remove(dump_file)
        except OSError as e:
            print(f"Warning: could not remove dump file {dump_file}: {e}")
    return PyRedisStore()

# --- Basic String Command Tests ---

def test_basic_set_get_del(store):
    """Test basic SET, GET, and DEL operations."""
    assert store.command_set("mykey", "value1") == "OK"
    assert store.command_get("mykey") == "value1"
    assert store.command_del("mykey") == 1
    assert store.command_get("mykey") is None
    assert store.command_del("mykey") == 0

def test_get_non_existent(store):
    """Test GET on a key that was never set."""
    assert store.command_get("nonexistent") is None

def test_del_multiple_keys(store):
    """Test DEL with multiple keys, some existing, some not."""
    assert store.command_set("key1", "val1") == "OK"
    assert store.command_set("key2", "val2") == "OK"
    assert store.command_del("key1", "key_missing", "key2") == 2
    assert store.command_get("key1") is None
    assert store.command_get("key2") is None

def test_set_overwrite(store):
    """Test that SET overwrites existing keys."""
    assert store.command_set("mykey", "value1") == "OK"
    assert store.command_get("mykey") == "value1"
    assert store.command_set("mykey", "value2") == "OK"
    assert store.command_get("mykey") == "value2"

# --- Expiration Tests ---

def test_basic_expiry(store):
    """Test SET with EX and GET after expiration."""
    assert store.command_set("tempkey", "tempval", expire_ms="150") == "OK"
    assert store.command_get("tempkey") == "tempval"
    time.sleep(0.2)
    assert store.command_get("tempkey") is None

def test_ttl_command(store):
    """Test the TTL command."""
    assert store.command_set("key_no_expire", "val") == "OK"
    assert store.command_set("key_with_expire", "val", expire_ms="2000") == "OK"

    assert store.command_ttl("key_no_expire") == -1
    assert store.command_ttl("nonexistent_key") == -2

    ttl_initial = store.command_ttl("key_with_expire")
    assert ttl_initial > 0 and ttl_initial <= 2

    time.sleep(2.2)
    assert store.command_ttl("key_with_expire") == -2
    assert store.command_get("key_with_expire") is None

def test_expire_command(store):
    """Test the EXPIRE command."""
    assert store.command_set("mykey", "myvalue") == "OK"
    assert store.command_ttl("mykey") == -1

    assert store.command_expire("mykey", "1") == 1
    ttl_after_expire = store.command_ttl("mykey")
    assert ttl_after_expire >= 0 and ttl_after_expire <= 1

    assert store.command_expire("nonexistent_key", "10") == 0

    time.sleep(1.2)
    assert store.command_ttl("mykey") == -2
    assert store.command_get("mykey") is None

    assert store.command_set("key_rem_exp", "data", expire_ms="5000") == "OK"
    assert store.command_ttl("key_rem_exp") > 0
    assert store.command_expire("key_rem_exp", "0") == 1
    assert store.command_ttl("key_rem_exp") == -1

# --- List Command Tests ---

def test_list_lpush_lrange(store):
    assert store.command_lpush("mylist", "a") == 1
    assert store.command_lpush("mylist", "b", "c") == 3
    assert store.command_lrange("mylist", "0", "0") == ["b"]
    assert store.command_lrange("mylist", "0", "1") == ["b", "c"]
    assert store.command_lrange("mylist", "0", "-1") == ["b", "c", "a"]
    assert store.command_lrange("mylist", "-2", "-1") == ["c", "a"]
    assert store.command_lrange("mylist", "5", "10") == []
    assert store.command_lrange("nonexistent_list", "0", "-1") == []

def test_list_rpush(store):
    """Test RPUSH."""
    assert store.command_rpush("mylist", "a") == 1
    assert store.command_rpush("mylist", "b", "c") == 3
    assert store.command_lrange("mylist", "0", "-1") == ["a", "b", "c"]

def test_list_expiry(store):
    """Test expiration on list keys."""
    assert store.command_lpush("templist", "val1") == 1
    assert store.command_expire("templist", "1") == 1
    assert store.command_ttl("templist") >= 0
    time.sleep(1.2)
    assert store.command_lrange("templist", "0", "-1") == []
    assert store.command_ttl("templist") == -2

# --- Hash Command Tests ---

def test_hash_hset_hget(store):
    """Test HSET and HGET basics."""
    assert store.command_hset("myhash", "field1", "value1") == 1
    assert store.command_hset("myhash", "field2", "value2") == 1
    assert store.command_hget("myhash", "field1") == "value1"
    assert store.command_hget("myhash", "field2") == "value2"
    assert store.command_hget("myhash", "missing_field") is None
    assert store.command_hget("missing_hash", "field1") is None

def test_hash_hset_overwrite(store):
    """Test HSET overwriting an existing field."""
    assert store.command_hset("myhash", "field1", "value1") == 1
    assert store.command_hset("myhash", "field1", "value_new") == 0
    assert store.command_hget("myhash", "field1") == "value_new"

def test_hash_hdel(store):
    """Test HDEL command."""
    assert store.command_hset("myhash", "f1", "v1") == 1
    assert store.command_hset("myhash", "f2", "v2") == 1
    assert store.command_hset("myhash", "f3", "v3") == 1

    assert store.command_hdel("myhash", "f1", "f_missing") == 1
    assert store.command_hget("myhash", "f1") is None
    assert store.command_hget("myhash", "f2") == "v2"

    assert store.command_hdel("myhash", "f2", "f3") == 2
    assert store.command_hget("myhash", "f2") is None
    assert store.command_hget("myhash", "f3") is None
    assert store.command_hdel("myhash", "f_missing") == 0
    assert store.command_hdel("missing_hash", "f1") == 0

def test_hash_expiry(store):
    """Test expiration on hash keys."""
    assert store.command_hset("temphash", "f1", "v1") == 1
    assert store.command_expire("temphash", "1") == 1
    assert store.command_ttl("temphash") >= 0
    time.sleep(1.2)
    assert store.command_hget("temphash", "f1") is None
    assert store.command_ttl("temphash") == -2

# --- Type Error Tests ---
WRONGTYPE_ERROR_PREFIX = "ERROR: WRONGTYPE Operation against a key holding the wrong kind of value"

def test_wrong_type_operations(store):
    """Test operations on keys holding the wrong data type."""
    assert store.command_set("mystring", "hello") == "OK"
    assert store.command_lpush("mylist", "a") == 1
    assert store.command_hset("myhash", "f1", "v1") == 1

    get_list_result = store.command_get("mylist")
    assert get_list_result == WRONGTYPE_ERROR_PREFIX
    get_hash_result = store.command_get("myhash")
    assert get_hash_result == WRONGTYPE_ERROR_PREFIX

    lpush_str_result = store.command_lpush("mystring", "b")
    assert lpush_str_result == WRONGTYPE_ERROR_PREFIX
    lrange_hash_result = store.command_lrange("myhash", "0", "-1")
    assert lrange_hash_result == WRONGTYPE_ERROR_PREFIX

    hset_str_result = store.command_hset("mystring", "f1", "v1")
    assert hset_str_result == WRONGTYPE_ERROR_PREFIX
    hget_list_result = store.command_hget("mylist", "f1")
    assert hget_list_result == WRONGTYPE_ERROR_PREFIX
